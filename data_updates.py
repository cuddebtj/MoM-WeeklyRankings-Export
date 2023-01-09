import pandas as pd
import yaml

from assests.assests import PRIVATE, CHANGES
from Mom_WeeklyRankings_Export.db_upload import DatabaseCursor
from Mom_WeeklyRankings_Export.utils import data_upload


def data_pipeline():

    matchups_q = """
    SELECT *
    FROM raw.matchups
    """
    matchups_df = DatabaseCursor(PRIVATE).copy_from_psql(matchups_q)

    weekly_team_pts_q = """
    SELECT *
    FROM raw.weekly_team_pts
    """
    points_df = DatabaseCursor(PRIVATE).copy_from_psql(weekly_team_pts_q)

    team_keys = []
    weeks = []

    new_matchups_q = """
    SELECT *
    FROM raw.matchups
    WHERE NOT ((team_a_team_key IN ({team_as}) 
            OR team_b_team_key IN ({team_bs}))
        AND week IN ({weeks}))
    """

    new_wkly_pts_q = """
    SELECT *
    FROM raw.weekly_team_pts
    WHERE NOT (team_key IN ({teams})
        AND week IN ({weeks}))
    """

    with open(CHANGES) as file:
        data_changes = yaml.load(file, Loader=yaml.SafeLoader)

    for wk in data_changes["weeks"]:
        week = wk["week"]
        weeks.append(str(week))
        for chg in wk["changes"]:
            team_key = chg["team_key"]
            team_keys.append(team_key)
            points = chg["points"]
            matchups_mask = (matchups_df["week"] == week) & (
                matchups_df["team_a_team_key"] == team_key
            )
            matchups_mask_opp = (matchups_df["week"] == week) & (
                matchups_df["team_b_team_key"] == team_key
            )
            points_mask = (points_df["week"] == week) & (
                points_df["team_key"] == team_key
            )
            matchups_df.loc[matchups_mask, "team_a_points"] = points
            matchups_df.loc[matchups_mask_opp, "team_b_points"] = points
            points_df.loc[points_mask, "final_points"] = points

    teams_string = "'" + "', '".join(team_keys) + "'"
    weeks_string = ", ".join(weeks)
    new_matchups_q = new_matchups_q.format(
        team_as=teams_string, team_bs=teams_string, weeks=weeks_string
    )
    new_wkly_pts_q = new_wkly_pts_q.format(teams=teams_string, weeks=weeks_string)

    new_matchups = matchups_df[
        (matchups_df["week"].astype(str).isin(weeks))
        & (
            (matchups_df["team_a_team_key"].isin(team_keys))
            | (matchups_df["team_b_team_key"].isin(team_keys))
        )
    ]
    new_wkly_pts = points_df[
        (points_df["week"].astype(str).isin(weeks))
        & (points_df["team_key"].isin(team_keys))
    ]

    data_upload(new_matchups, "raw.matchups", PRIVATE, new_matchups_q)
    data_upload(new_wkly_pts, "raw.weekly_team_pts", PRIVATE, new_wkly_pts_q)


if __name__ == "__main__":
    data_pipeline()
