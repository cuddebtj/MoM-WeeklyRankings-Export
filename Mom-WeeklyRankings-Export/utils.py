import pandas as pd
import numpy as np
import yaml
from pathlib import Path
import calendar

from db_upload import DatabaseCursor
from tournament import (
    Tournament,
    curr_playoff_picture,
    competition_rounds,
    postseason_teams,
    playoff_weeks_calc,
)
from cust_logging import log_print, log_print_tourney

PATH = list(Path().cwd().glob("**/private.yaml"))
if PATH == []:
    PATH = list(Path().cwd().parent.glob("**/private.yaml"))[0]
else:
    PATH = PATH[0]

TEAMS_FILE = list(Path().cwd().glob("**/teams.yaml"))
if TEAMS_FILE == []:
    TEAMS_FILE = list(Path().cwd().parent.glob("**/teams.yaml"))[0]
else:
    TEAMS_FILE = TEAMS_FILE[0]


def get_laborday(date):
    """
    Calculates when Labor day is of the given year
    """
    date = np.datetime64(date, "D")
    year = date.astype("datetime64[Y]").astype(int) + 1970
    month = 9
    if date < np.datetime64(f"{year}-09-01"):
        year -= 1
    mycal = calendar.Calendar(0)
    cal = mycal.monthdatescalendar(year, month)
    if cal[0][0].month == month:
        return cal[0][0], cal[0][0].year
    else:
        return cal[1][0], cal[1][0].year


def nfl_weeks_pull():
    """
    Function to call assests files for Yahoo API Query
    """
    try:
        db_cursor = DatabaseCursor(PATH)
        nfl_weeks = db_cursor.copy_from_psql("SELECT DISTINCT * FROM prod.nfl_weeks")
        nfl_weeks["week_end"] = nfl_weeks["week_end"].astype("datetime64[D]")
        nfl_weeks["week_start"] = nfl_weeks["week_start"].astype("datetime64[D]")
        return nfl_weeks

    except Exception as e:
        log_print(error=e, module_="utils.py", func="nfl_weeks_pull")


def game_keys_pull():
    """
    Function to call game_keys
    """
    try:
        db_cursor = DatabaseCursor(PATH)
        game_keys = db_cursor.copy_from_psql("SELECT DISTINCT * FROM prod.game_keys")

        if game_keys.empty:
            with open(TEAMS_FILE, "r") as file:
                c_teams = yaml.load(file, Loader=yaml.SafeLoader)
            game_keys = pd.DataFrame.from_dict(c_teams["teams"])
            game_keys = game_keys[["game_id", "league_id", "year"]]

        log_print(
            module_="utils.py",
            func="game_keys_pull",
            path=str(TEAMS_FILE),
        )
        return game_keys

    except Exception as e:
        log_print(
            error=e, module_="utils.py", func="game_keys_pull", path=str(TEAMS_FILE)
        )


def data_upload(df: pd.DataFrame, table_name, path, query):

    try:
        psql = DatabaseCursor(path).copy_from_psql(query)
        df = pd.concat([psql, df])
        df.drop_duplicates(inplace=True)
        DatabaseCursor(path).copy_to_psql(df=df, table=table_name)

    except Exception as e:
        log_print(
            error=e,
            module_="utils.py",
            func="data_upload",
            table_name=table_name,
            query=query,
            path=path,
        )


def reg_season(game_id):
    """
    Fucntion to calculate regular season rankings, scores, wins/losses, and matchups
    """
    try:
        matchups_query = f"SELECT DISTINCT game_id, \
week, \
week_start, \
week_end, \
is_playoffs, \
is_consolation, \
team_a_team_key, \
team_a_points, \
team_a_projected_points, \
team_b_team_key, \
team_b_points, \
team_b_projected_points, \
winner_team_key \
FROM raw.matchups \
WHERE game_id = {str(game_id)}"
        teams_query = f"SELECT DISTINCT * FROM raw.teams WHERE game_id = {str(game_id)}"
        settings_query = f"SELECT DISTINCT playoff_start_week, game_id FROM prod.settings WHERE game_id = {str(game_id)}"
        matchups = DatabaseCursor(PATH).copy_from_psql(matchups_query).drop_duplicates()

        teams = DatabaseCursor(PATH).copy_from_psql(teams_query).drop_duplicates()

        settings = DatabaseCursor(PATH).copy_from_psql(settings_query).drop_duplicates()

        matchups_a = matchups.copy()
        matchups_b = matchups.copy()

        matchups_b_cols = list(matchups_b.columns)

        rename_columns = {}
        for col in matchups_b_cols:
            if "team_a" in col:
                rename_columns[col] = f"team_b{col[6:]}"
            elif "team_b" in col:
                rename_columns[col] = f"team_a{col[6:]}"

        matchups_b.rename(columns=rename_columns, inplace=True)

        matchups = pd.concat([matchups_a, matchups_b])

        matchups.sort_values(["week_start", "team_a_team_key"], inplace=True)

        matchups.reset_index(drop=True, inplace=True)

        # logic to help create playoff brackets
        playoff_start_week = settings["playoff_start_week"][
            settings["game_id"] == game_id
        ].values[0]

        reg_season = matchups[
            (matchups["game_id"] == game_id) & (matchups["week"] < playoff_start_week)
        ]

        reg_season["Wk W/L"] = np.where(
            reg_season["team_a_points"] > reg_season["team_b_points"], "W", "L"
        )
        reg_season["Wk Pts Rk"] = (
            reg_season.groupby(["week", "game_id"])["team_a_points"]
            .rank("first", ascending=False)
            .astype(int)
        )
        reg_season["Wk Pro. Pts Rk"] = (
            reg_season.groupby(["week", "game_id"])["team_a_projected_points"]
            .rank("first", ascending=False)
            .astype(int)
        )
        reg_season["Wk Pts W/L"] = np.where(reg_season["Wk Pts Rk"] <= 5, 1, 0).astype(
            int
        )

        reg_season["Opp Wk Pts Rk"] = (
            reg_season.groupby(["week", "game_id"])["team_b_points"]
            .rank("first", ascending=False)
            .astype(int)
        )
        reg_season["Opp Wk Pro. Pts Rk"] = (
            reg_season.groupby(["week", "game_id"])["team_b_projected_points"]
            .rank("first", ascending=False)
            .astype(int)
        )

        reg_season = reg_season.merge(
            teams, how="left", left_on="team_a_team_key", right_on="team_key"
        )
        reg_season["team_a_name"] = reg_season["name"].fillna(
            reg_season["team_a_team_key"]
        )
        reg_season["team_a_nickname"] = reg_season["nickname"].fillna(
            reg_season["team_a_team_key"]
        )
        reg_season["game_id_a"] = reg_season["game_id_x"].fillna(
            reg_season["game_id_y"]
        )
        reg_season.drop(["name", "nickname"], axis=1, inplace=True)

        reg_season = reg_season.merge(
            teams, how="left", left_on="team_b_team_key", right_on="team_key"
        )
        reg_season["team_b_name"] = reg_season["name"].fillna(
            reg_season["team_b_team_key"]
        )
        reg_season["team_b_nickname"] = reg_season["nickname"].fillna(
            reg_season["team_b_team_key"]
        )
        reg_season.drop(["name", "nickname"], axis=1, inplace=True)

        reg_season["game_id"] = reg_season["game_id_a"]

        reg_season = reg_season.rename(
            columns={
                "team_a_team_key": "team_key",
                "team_a_name": "Team",
                "team_a_nickname": "Manager",
                "team_a_points": "Wk Pts",
                "team_a_projected_points": "Wk Pro. Pts",
                "team_b_team_key": "opp_team_key",
                "team_b_name": "Opp Team",
                "team_b_nickname": "Opp Manager",
                "team_b_points": "Opp Wk Pts",
                "team_b_projected_points": "Opp Wk Pro. Pts",
                "week": "Week",
            }
        )

        reg_season["Ttl Pts"] = (
            reg_season.groupby(["team_key"])["Wk Pts"]
            .cumsum()
            .astype(float)
            .round(decimals=2)
        )
        reg_season["Ttl Pts Rk"] = (
            reg_season.groupby(["Week"])["Ttl Pts"]
            .rank(method="min", ascending=False)
            .astype(int)
        )
        reg_season["Avg Pts"] = round(reg_season["Ttl Pts"] / reg_season["Week"], 2)
        reg_season["Avg Pts Rk"] = (
            reg_season.groupby(["Week"])["Avg Pts"]
            .rank(method="min", ascending=False)
            .astype(int)
        )

        reg_season["Ttl Pro. Pts"] = (
            reg_season.groupby(["team_key"])["Wk Pro. Pts"]
            .cumsum()
            .astype(float)
            .round(decimals=2)
        )
        reg_season["Ttl Pro. Pts Rk"] = (
            reg_season.groupby(["Week"])["Ttl Pro. Pts"]
            .rank(method="min", ascending=False)
            .astype(int)
        )

        reg_season["Ttl Opp Pts"] = (
            reg_season.groupby(["team_key"])["Opp Wk Pts"]
            .cumsum()
            .astype(float)
            .round(decimals=2)
        )
        reg_season["Ttl Opp Pts Rk"] = (
            reg_season.groupby(["Week"])["Ttl Opp Pts"]
            .rank(method="max", ascending=True)
            .astype(int)
        )
        reg_season["Avg Opp Pts"] = round(
            reg_season["Ttl Opp Pts"] / reg_season["Week"], 2
        )
        reg_season["Avg Opp Pts Rk"] = (
            reg_season.groupby(["Week"])["Avg Opp Pts"]
            .rank(method="min", ascending=False)
            .astype(int)
        )

        reg_season["Ttl Opp Pro. Pts"] = (
            reg_season.groupby(["team_key"])["Opp Wk Pro. Pts"]
            .cumsum()
            .astype(float)
            .round(decimals=2)
        )
        reg_season["Ttl Opp Pro. Pts Rk"] = (
            reg_season.groupby(["Week"])["Ttl Opp Pro. Pts"]
            .rank(method="max", ascending=True)
            .astype(int)
        )

        reg_season["Win Ttl"] = (
            reg_season["Wk W/L"]
            .eq("W")
            .groupby(reg_season["team_key"])
            .cumsum()
            .astype(int)
        )
        reg_season["Loss Ttl"] = (
            reg_season["Wk W/L"]
            .eq("L")
            .groupby(reg_season["team_key"])
            .cumsum()
            .astype(int)
        )
        reg_season["W/L Rk"] = (
            reg_season.groupby(["Week"])["Win Ttl"]
            .rank(method="min", ascending=False)
            .astype(int)
        )

        reg_season["Ttl Pts Win"] = (
            reg_season.groupby(["team_key"])["Wk Pts W/L"].cumsum().astype(int)
        )
        reg_season["Ttl Pts Win Rk"] = (
            reg_season.groupby(["Week"])["Ttl Pts Win"]
            .rank(method="min", ascending=False)
            .astype(int)
        )
        reg_season["2pt Ttl"] = reg_season["Ttl Pts Win"] + reg_season["Win Ttl"]
        reg_season["2pt Ttl Rk"] = (
            reg_season.groupby(["Week"])["2pt Ttl"]
            .rank(method="min", ascending=False)
            .astype(int)
        )

        if game_id >= 390:
            reg_season["rk_tuple"] = reg_season[["2pt Ttl Rk", "Ttl Pts Rk"]].apply(
                tuple, axis=1
            )
        else:
            reg_season["rk_tuple"] = reg_season[["W/L Rk", "Ttl Pts Rk"]].apply(
                tuple, axis=1
            )

        reg_season["Cur. Wk Rk"] = (
            reg_season.groupby(["Week"])["rk_tuple"]
            .rank(method="min", ascending=True)
            .astype(int)
        )
        reg_season["Prev. Wk Rk"] = (
            reg_season.sort_values(["Week"])
            .groupby(["team_key"])["Cur. Wk Rk"]
            .shift(fill_value=0)
            .astype(int)
        )

        reg_season.sort_values(
            ["Week", "Cur. Wk Rk"], ascending=[True, True], inplace=True
        )

        reg_season_final = reg_season[
            [
                "game_id",
                "Week",
                "team_key",
                "Cur. Wk Rk",
                "Prev. Wk Rk",
                "Manager",
                "Team",
                "2pt Ttl",
                "2pt Ttl Rk",
                "Ttl Pts Win",
                "Ttl Pts Win Rk",
                "Win Ttl",
                "Loss Ttl",
                "W/L Rk",
                "Wk W/L",
                "Wk Pts W/L",
                "Wk Pts",
                "Wk Pts Rk",
                "Wk Pro. Pts",
                "Wk Pro. Pts Rk",
                "Avg Pts",
                "Avg Pts Rk",
                "Avg Opp Pts",
                "Avg Opp Pts Rk",
                "Ttl Pts",
                "Ttl Pts Rk",
                "Ttl Opp Pts",
                "Ttl Opp Pts Rk",
                "Ttl Pro. Pts",
                "Ttl Pro. Pts Rk",
                "Ttl Opp Pro. Pts",
                "Ttl Opp Pro. Pts Rk",
                "opp_team_key",
                "Opp Manager",
                "Opp Team",
                "Opp Wk Pts",
                "Opp Wk Pts Rk",
                "Opp Wk Pro. Pts",
                "Opp Wk Pro. Pts Rk",
                "rk_tuple",
            ]
        ]

        if game_id >= 390:
            max_week = reg_season_final["Week"].max()
            m_reg_df = reg_season_final[["game_id", "team_key", "Team", "Cur. Wk Rk"]][
                reg_season_final["Week"] == max_week
            ]
            teams = teams.merge(
                m_reg_df,
                how="left",
                left_on=["game_id", "team_key"],
                right_on=["game_id", "team_key"],
            )
            teams["name"] = teams["Team"]
            teams["team_standings.rank"] = teams["Cur. Wk Rk"]
            teams["team_standings.playoff_seed"] = teams["Cur. Wk Rk"]
            teams = teams[
                [
                    "game_id",
                    "league_id",
                    "team_id",
                    "team_key",
                    "manager_id",
                    "clinched_playoffs",
                    "draft_grade",
                    "faab_balance",
                    "name",
                    "nickname",
                    "number_of_moves",
                    "number_of_trades",
                    "team_standings.playoff_seed",
                    "team_standings.rank",
                    "team_standings.outcome_totals.wins",
                    "team_standings.outcome_totals.losses",
                    "team_standings.outcome_totals.ties",
                    "team_standings.outcome_totals.percentage",
                    "team_standings.points_for",
                    "team_standings.points_against",
                ]
            ]

            query_1 = f"SELECT * FROM raw.teams WHERE game_id != {str(game_id)}"

            data_upload(teams, "raw.teams", PATH, query_1)

        query_2 = (
            f"SELECT * FROM prod.reg_season_results WHERE game_id != {str(game_id)}"
        )
        data_upload(reg_season_final, "prod.reg_season_results", PATH, query_2)

        return reg_season_final

    except Exception as e:
        log_print(
            error=e,
            module_="utils.py",
            func="reg_season",
            table="prod.reg_season_results",
            game_id=game_id,
        )


def post_season(game_id):
    """
    Function to calculate post_season winners/losers, create final rank for the season
    """
    try:
        settings_query = f"SELECT set.game_id, \
set.playoff_start_week, \
met.end_week, \
set.max_teams, \
set.num_playoff_teams, \
set.num_playoff_consolation_teams \
FROM prod.settings set \
JOIN prod.metadata met \
on met.game_id = set.game_id \
and met.league_id = set.league_id \
WHERE set.game_id = {str(game_id)}"
        settings = DatabaseCursor(PATH).copy_from_psql(settings_query).drop_duplicates()

        one_playoff_season_query = f'SELECT pts.game_id, \
pts.week as "Week", \
pts.team_key, \
pts.final_points as "Wk Pts", \
pts.projected_points as "Wk Pro. Pts", \
tm.name as "Team", \
tm.nickname as "Manager", \
tm."team_standings.playoff_seed" as "Playoff Seed" \
FROM raw.weekly_team_pts pts \
JOIN raw.teams tm \
on tm.team_key = pts.team_key \
WHERE pts.game_id = {str(game_id)}'
        one_playoff_season = (
            DatabaseCursor(PATH)
            .copy_from_psql(one_playoff_season_query)
            .drop_duplicates()
        )

        max_teams = settings["max_teams"][settings["game_id"] == game_id].values[0]
        num_playoff_teams = settings["num_playoff_teams"][
            settings["game_id"] == game_id
        ].values[0]
        num_conso_teams = settings["num_playoff_consolation_teams"][
            settings["game_id"] == game_id
        ].values[0]
        num_toliet_teams = (
            (
                max_teams - num_playoff_teams
                if num_playoff_teams >= (max_teams / 2)
                else num_playoff_teams
            )
            if num_conso_teams != 0
            else None
        )
        playoff_start_week = settings["playoff_start_week"][
            settings["game_id"] == game_id
        ].values[0]
        playoff_end_week = settings["end_week"][settings["game_id"] == game_id].values[
            0
        ]
        current_week = one_playoff_season["Week"].max()

        one_playoff_season = one_playoff_season[
            (one_playoff_season["Week"] >= playoff_start_week)
            | (one_playoff_season["Week"] == one_playoff_season["Week"].max())
        ]

        one_playoff_season["Finish"] = np.nan
        one_playoff_season["Bracket"] = np.nan
        one_playoff_season["Opp Wk Pts"] = np.nan
        one_playoff_season["Opp Wk Pro. Pts"] = np.nan
        one_playoff_season["Wk W/L"] = np.nan

        one_playoff_season.sort_values(
            ["Week", "Playoff Seed"], ascending=[True, True], inplace=True
        )

        playoff_teams, conso_teams, toilet_teams = postseason_teams(
            one_playoff_season,
            game_id,
            max_teams,
            num_playoff_teams,
            num_conso_teams,
            num_toliet_teams,
        )

        playoff_bracket = Tournament(playoff_teams)
        curr_playoff_picture(
            playoff_bracket, one_playoff_season, current_week, "Playoff"
        )

        if conso_teams:
            conso_bracket = Tournament(conso_teams)
            if current_week < playoff_start_week:
                curr_playoff_picture(
                    conso_bracket, one_playoff_season, current_week, "Consolation"
                )
        else:
            conso_bracket = None

        if toilet_teams:
            toilet_bracket = Tournament(toilet_teams)
            if current_week < playoff_start_week:
                curr_playoff_picture(
                    toilet_bracket, one_playoff_season, current_week, "Toilet"
                )
        else:
            toilet_bracket = None

        playoff_weeks, conso_weeks, toilet_weeks = playoff_weeks_calc(
            current_week,
            playoff_start_week,
            playoff_end_week,
            playoff_teams,
            conso_teams,
            toilet_teams,
        )

        playoff_end_week_mask = one_playoff_season["Week"] == playoff_end_week

        if playoff_weeks:
            for week in playoff_weeks:
                competition_rounds(playoff_bracket, "Playoff", week, one_playoff_season)
                curr_playoff_picture(
                    playoff_bracket, one_playoff_season, week + 1, "Playoff"
                )

        if conso_weeks:
            for week in conso_weeks:
                competition_rounds(
                    conso_bracket, "Consolation", week, one_playoff_season
                )
                curr_playoff_picture(
                    conso_bracket, one_playoff_season, current_week + 1, "Consolation"
                )

        if toilet_weeks:
            for week in toilet_weeks:
                competition_rounds(toilet_bracket, "Toilet", week, one_playoff_season)
                curr_playoff_picture(
                    toilet_bracket, one_playoff_season, current_week + 1, "Toilet"
                )

        if current_week == playoff_end_week:
            log_print_tourney(bracket="Playoffs", final=playoff_bracket.get_final())
            for rk, tm in playoff_bracket.get_final().items():
                finish_mask = (
                    one_playoff_season["team_key"] == tm
                ) & playoff_end_week_mask
                one_playoff_season.loc[finish_mask, "Finish"] = int(rk)
                one_playoff_season.loc[finish_mask, "Bracket"] = "Playoff Final"

            log_print_tourney(bracket="Consolation", final=conso_bracket.get_final())
            for rk, tm in conso_bracket.get_final().items():
                finish_mask = (
                    one_playoff_season["team_key"] == tm
                ) & playoff_end_week_mask
                one_playoff_season.loc[finish_mask, "Finish"] = int(
                    int(rk) + len(playoff_teams)
                )
                one_playoff_season.loc[finish_mask, "Bracket"] = "Consolation Final"

            log_print_tourney(bracket="Toliet", final=toilet_bracket.get_final())
            for rk, tm in toilet_bracket.get_final().items():
                finish_mask = (
                    one_playoff_season["team_key"] == tm
                ) & playoff_end_week_mask
                one_playoff_season.loc[finish_mask, "Finish"] = int(
                    int(rk)
                    + len(playoff_teams)
                    + (len(conso_teams) if conso_teams else 0)
                )
                one_playoff_season.loc[finish_mask, "Bracket"] = "Toliet Final"

            one_playoff_season.loc[
                playoff_end_week_mask, "Finish"
            ] = one_playoff_season.loc[playoff_end_week_mask, "Finish"].fillna(
                one_playoff_season["Playoff Seed"]
            )
            one_playoff_season.loc[
                playoff_end_week_mask, "Bracket"
            ] = one_playoff_season.loc[playoff_end_week_mask, "Bracket"].fillna(
                "Reg Season Finish"
            )

        one_playoff_season["Finish"] = (
            one_playoff_season["Finish"]
            .fillna(one_playoff_season["Playoff Seed"])
            .astype(int)
        )
        one_playoff_season["Bracket"] = one_playoff_season["Bracket"].fillna(
            "Reg Season Finish"
        )
        one_playoff_season["Bracket"] = one_playoff_season["Bracket"].fillna(
            "Reg Season Finish"
        )

        one_playoff_season["Opp Wk Pts"] = one_playoff_season["Opp Wk Pts"].fillna(0)
        one_playoff_season["Opp Wk Pro. Pts"] = one_playoff_season[
            "Opp Wk Pro. Pts"
        ].fillna(0)
        one_playoff_season["opp_team_key"] = one_playoff_season["opp_team_key"].fillna(
            "Bye"
        )
        one_playoff_season["Opp Team"] = one_playoff_season["Opp Team"].fillna("Bye")
        one_playoff_season["Opp Manager"] = one_playoff_season["Opp Manager"].fillna(
            "Bye"
        )
        one_playoff_season["Wk W/L"] = one_playoff_season["Wk W/L"].fillna("Bye")
        one_playoff_season["Ttl Pts"] = one_playoff_season.groupby(["team_key"])[
            "Wk Pts"
        ].cumsum()
        one_playoff_season["Ttl Pro. Pts"] = one_playoff_season.groupby(["team_key"])[
            "Wk Pro. Pts"
        ].cumsum()
        one_playoff_season["Opp Ttl Pts"] = one_playoff_season.groupby(["team_key"])[
            "Opp Wk Pts"
        ].cumsum()
        one_playoff_season["Opp Ttl Pro. Pts"] = one_playoff_season.groupby(
            ["team_key"]
        )["Opp Wk Pro. Pts"].cumsum()

        one_playoff_season = one_playoff_season[
            [
                "game_id",
                "Week",
                "Bracket",
                "Finish",
                "Playoff Seed",
                "team_key",
                "Team",
                "Manager",
                "Wk W/L",
                "Wk Pts",
                "Wk Pro. Pts",
                "Ttl Pts",
                "Ttl Pro. Pts",
                "opp_team_key",
                "Opp Team",
                "Opp Manager",
                "Opp Wk Pts",
                "Opp Wk Pro. Pts",
                "Opp Ttl Pts",
                "Opp Ttl Pro. Pts",
            ]
        ]

        one_playoff_season.sort_values(["Week", "Finish"], inplace=True)

        query = f"SELECT * FROM prod.playoff_board WHERE game_id != {str(game_id)}"

        data_upload(one_playoff_season, "prod.playoff_board", PATH, query)

        return one_playoff_season

    except Exception as e:
        log_print(
            error=e,
            module_="utils.py",
            func="post_season",
            table="prod.playoff_board",
            game_id=game_id,
        )
