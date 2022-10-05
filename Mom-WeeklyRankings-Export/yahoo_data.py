import pandas as pd
import numpy as np
import logging
import time
import yaml
from pathlib import Path
from yfpy import YahooFantasySportsQuery
from yfpy.utils import complex_json_handler, unpack_data
from yfpy import get_logger

from db_upload import DatabaseCursor
from utils import data_upload
from cust_logging import log_print

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

LOG_PATH = list(Path().cwd().glob("**/logg.txt"))
if LOG_PATH == []:
    LOG_PATH = list(Path().cwd().parent.glob("**/logg.txt"))[0]
else:
    LOG_PATH = LOG_PATH[0]


class league_season_data(object):

    LOGGET = get_logger(__name__)
    LOG_OUTPUT = False
    logging.getLogger("yfpy.query").setLevel(level=logging.INFO)

    def __init__(
        self,
        auth_dir=None,
        league_id=None,
        game_id=None,
        game_code="nfl",
        offline=False,
        all_output_as_json=False,
        consumer_key=None,
        consumer_secret=None,
        browser_callback=True,
    ):
        self._auth_dir = auth_dir
        self._consumer_key = str(consumer_key)
        self._consumer_secret = str(consumer_secret)
        self._browser_callback = browser_callback

        self.league_id = str(league_id)
        self.game_id = str(game_id)
        self.game_code = str(game_code)

        self.offline = offline
        self.all_output_as_json = all_output_as_json

        self.yahoo_query = YahooFantasySportsQuery(
            auth_dir=self._auth_dir,
            league_id=self.league_id,
            game_id=self.game_id,
            game_code=self.game_code,
            offline=self.offline,
            all_output_as_json=self.all_output_as_json,
            consumer_key=self._consumer_key,
            consumer_secret=self._consumer_secret,
            browser_callback=self._browser_callback,
        )

    def metadata(self):
        """
        Pull League Metadata
        """
        try:
            try:
                response = complex_json_handler(self.yahoo_query.get_league_metadata())
            except Exception as e:
                if "Invalid week" in str(e):
                    return
                elif "token_expired" in str(e):
                    self.yahoo_query._authenticate()
                elif "Network is unreachable" in str(e):
                    log_print(
                        error=e,
                        module_="yahoo_query.py",
                        func="metadata",
                        game_id=self.game_id,
                        sleep="15 min before retrying",
                    )
                    time.sleep(900)
                else:
                    log_print(
                        error=e,
                        module_="yahoo_query.py",
                        func="metadata",
                        game_id=self.game_id,
                        sleep="1 hour before retrying",
                    )
                    time.sleep(3600)
                    try:
                        self.yahoo_query._authenticate()
                    except Exception as e:
                        log_print(
                            error=e,
                            module_="yahoo_query.py",
                            func="metadata",
                            game_id=self.game_id,
                            sleep="30 min before 2nd retry",
                        )
                        time.sleep(1800)
                        self.yahoo_query._authenticate()

                response = complex_json_handler(self.yahoo_query.get_league_metadata())

            league_metadata = pd.json_normalize(response)
            league_metadata["game_id"] = self.game_id
            league_metadata.drop_duplicates(ignore_index=True, inplace=True)
            league_metadata = league_metadata[
                [
                    "game_id",
                    "league_id",
                    "name",
                    "num_teams",
                    "season",
                    "start_date",
                    "start_week",
                    "end_date",
                    "end_week",
                ]
            ]

            league_metadata["game_id"] = league_metadata["game_id"].astype(int)
            league_metadata["league_id"] = league_metadata["league_id"].astype(int)
            league_metadata["name"] = league_metadata["name"].astype(str)
            league_metadata["num_teams"] = league_metadata["num_teams"].astype(int)
            league_metadata["season"] = league_metadata["season"].astype(int)
            league_metadata["start_date"] = league_metadata["start_date"].astype(
                "datetime64[D]"
            )
            league_metadata["start_week"] = league_metadata["start_week"].astype(int)
            league_metadata["end_date"] = league_metadata["end_date"].astype(
                "datetime64[D]"
            )
            league_metadata["end_week"] = league_metadata["end_week"].astype(int)

            query = f"SELECT DISTINCT game_id, \
league_id, \
name, \
num_teams, \
season, \
start_date, \
start_week, \
end_date, \
end_week \
FROM prod.metadata \
WHERE game_id != {str(self.game_id)} \
ORDER BY game_id, \
league_id"

            data_upload(
                df=league_metadata,
                table_name="prod.metadata",
                query=query,
                path=PATH,
            )

            return league_metadata

        except Exception as e:
            log_print(
                error=e, module_="yahoo_query.py", func="metadata", game_id=self.game_id
            )

    def settings(self):
        """
        Get Roster Positions, Stat Categories, and League Settigns
        """
        try:
            try:
                response = complex_json_handler(self.yahoo_query.get_league_settings())
            except Exception as e:
                if "Invalid week" in str(e):
                    return
                elif "token_expired" in str(e):
                    self.yahoo_query._authenticate()
                elif "Network is unreachable" in str(e):
                    log_print(
                        error=e,
                        module_="yahoo_query.py",
                        func="set_roster_pos_stat_cat",
                        game_id=self.game_id,
                        sleep="15 min before retrying",
                    )
                    time.sleep(900)
                else:
                    log_print(
                        error=e,
                        module_="yahoo_query.py",
                        func="set_roster_pos_stat_cat",
                        game_id=self.game_id,
                        sleep="1 hour before retrying",
                    )
                    time.sleep(3600)
                    try:
                        self.yahoo_query._authenticate()
                    except Exception as e:
                        log_print(
                            error=e,
                            module_="yahoo_query.py",
                            func="set_roster_pos_stat_cat",
                            game_id=self.game_id,
                            sleep="30 min before 2nd retry",
                        )
                        time.sleep(1800)
                        self.yahoo_query._authenticate()

                response = complex_json_handler(self.yahoo_query.get_league_settings())

            league_settings = pd.json_normalize(response)
            league_settings.drop(
                ["roster_positions", "stat_categories.stats", "stat_modifiers.stats"],
                axis=1,
                inplace=True,
            )

            league_settings["game_id"] = self.game_id
            league_settings["league_id"] = self.league_id
            league_settings["has_playoff_consolation_games"].fillna(0, inplace=True)
            league_settings["has_multiweek_championship"].fillna(0, inplace=True)
            league_settings.drop_duplicates(ignore_index=True, inplace=True)
            league_settings = league_settings[
                [
                    "game_id",
                    "league_id",
                    "has_multiweek_championship",
                    "max_teams",
                    "num_playoff_teams",
                    "has_playoff_consolation_games",
                    "num_playoff_consolation_teams",
                    "playoff_start_week",
                    "trade_end_date",
                ]
            ]

            league_settings["game_id"] = league_settings["game_id"].astype(int)
            league_settings["league_id"] = league_settings["league_id"].astype(int)
            league_settings["has_multiweek_championship"] = league_settings[
                "has_multiweek_championship"
            ].astype(int)
            league_settings["max_teams"] = league_settings["max_teams"].astype(int)
            league_settings["num_playoff_teams"] = league_settings[
                "num_playoff_teams"
            ].astype(int)
            league_settings["has_playoff_consolation_games"] = league_settings[
                "has_playoff_consolation_games"
            ].astype(int)
            league_settings["num_playoff_consolation_teams"] = league_settings[
                "num_playoff_consolation_teams"
            ].astype(int)
            league_settings["playoff_start_week"] = league_settings[
                "playoff_start_week"
            ].astype(int)
            league_settings["trade_end_date"] = league_settings[
                "trade_end_date"
            ].astype("datetime64[D]")

            query_1 = f"SELECT DISTINCT game_id, \
league_id, \
has_multiweek_championship, \
max_teams, \
num_playoff_teams, \
has_playoff_consolation_games, \
num_playoff_consolation_teams, \
playoff_start_week, \
trade_end_date \
FROM prod.settings \
WHERE game_id != {str(self.game_id)} \
ORDER BY game_id, \
league_id"

            data_upload(
                df=league_settings,
                table_name="prod.settings",
                query=query_1,
                path=PATH,
            )

            return league_settings

        except Exception as e:
            log_print(
                error=e,
                module_="yahoo_query.py",
                func="settings",
                game_id=self.game_id,
            )

    def matchups(self, nfl_week=None):
        """
        stuff here
        """
        try:
            if nfl_week == None:
                print(
                    "\n----ERROR yahoo_query.py: matchups_by_week\n----Please include nfl_week in class creation\n"
                )
            else:
                m = []
                team_a = pd.DataFrame()
                team_b = pd.DataFrame()

                try:
                    response = self.yahoo_query.get_league_matchups_by_week(nfl_week)
                except Exception as e:
                    if "Invalid week" in str(e):
                        return
                    elif "scoreboard" in str(e):
                        return
                    elif "token_expired" in str(e):
                        self.yahoo_query._authenticate()
                    elif "Network is unreachable" in str(e):
                        log_print(
                            error=e,
                            module_="yahoo_query.py",
                            func="matchups",
                            game_id=self.game_id,
                            nfl_week=nfl_week,
                            sleep="15 min before retrying",
                        )
                        time.sleep(900)
                    else:
                        log_print(
                            error=e,
                            module_="yahoo_query.py",
                            func="matchups",
                            game_id=self.game_id,
                            nfl_week=nfl_week,
                            sleep="1 hour before retrying",
                        )
                        time.sleep(3600)
                        try:
                            self.yahoo_query._authenticate()
                        except Exception as e:
                            log_print(
                                error=e,
                                module_="yahoo_query.py",
                                func="matchups",
                                game_id=self.game_id,
                                nfl_week=nfl_week,
                                sleep="30 min before 2nd retry",
                            )
                            time.sleep(1800)
                            self.yahoo_query._authenticate()

                    response = self.yahoo_query.get_league_matchups_by_week(nfl_week)

                for data in response:
                    m.append(complex_json_handler(data["matchup"]))

                matchups = pd.DataFrame()
                for r in m:
                    matchup = pd.json_normalize(r)
                    if "is_tied" not in matchup.columns:
                        matchup["is_tied"] = 0
                    if "matchup_recap_title" not in matchup.columns:
                        matchup["matchup_recap_title"] = 0
                    if "matchup_recap_url" not in matchup.columns:
                        matchup["matchup_recap_url"] = 0
                    if "winner_team_key" not in matchup.columns:
                        matchup["winner_team_key"] = 0

                    matchup = matchup[
                        [
                            "is_consolation",
                            "is_matchup_recap_available",
                            "is_playoffs",
                            "is_tied",
                            "matchup_recap_title",
                            "matchup_recap_url",
                            "status",
                            "week",
                            "week_end",
                            "week_start",
                            "winner_team_key",
                        ]
                    ]
                    try:
                        team_a = pd.json_normalize(
                            complex_json_handler(
                                r["matchup_grades"][0]["matchup_grade"]
                            )
                        )
                        team_a["points"] = complex_json_handler(r["teams"][0]["team"])[
                            "team_points"
                        ]["total"]
                        team_a["projected_points"] = complex_json_handler(
                            r["teams"][0]["team"]
                        )["team_projected_points"]["total"]

                    except:
                        team_a = pd.json_normalize(
                            complex_json_handler(r["teams"][0]["team"])
                        )
                        team_a = team_a[
                            [
                                "team_key",
                                "team_points.total",
                                "team_projected_points.total",
                            ]
                        ]
                        team_a["grade"] = ""
                        team_a.rename(
                            columns={
                                "team_points.total": "points",
                                "team_projected_points.total": "projected_points",
                            },
                            inplace=True,
                        )

                    team_a = team_a.add_prefix("team_a_")

                    try:
                        team_b = pd.json_normalize(
                            complex_json_handler(
                                r["matchup_grades"][1]["matchup_grade"]
                            )
                        )
                        team_b["points"] = complex_json_handler(r["teams"][1]["team"])[
                            "team_points"
                        ]["total"]
                        team_b["projected_points"] = complex_json_handler(
                            r["teams"][1]["team"]
                        )["team_projected_points"]["total"]

                    except:
                        team_b = pd.json_normalize(
                            complex_json_handler(r["teams"][1]["team"])
                        )
                        team_b = team_b[
                            [
                                "team_key",
                                "team_points.total",
                                "team_projected_points.total",
                            ]
                        ]
                        team_b["grade"] = ""
                        team_b.rename(
                            columns={
                                "team_points.total": "points",
                                "team_projected_points.total": "projected_points",
                            },
                            inplace=True,
                        )

                    team_b = team_b.add_prefix("team_b_")

                    matchup = pd.concat([matchup, team_a, team_b], axis=1)

                    matchups = pd.concat([matchups, matchup])

                try:
                    matchups.drop(["teams", "matchup_grades"], axis=1, inplace=True)

                except:
                    pass

                matchups["game_id"] = self.game_id
                matchups["league_id"] = self.league_id
                matchups["is_playoffs"].fillna(0, inplace=True)
                matchups["is_consolation"].fillna(0, inplace=True)
                matchups["is_tied"].fillna(0, inplace=True)

            matchups = matchups[
                [
                    "game_id",
                    "is_consolation",
                    "is_playoffs",
                    "is_tied",
                    "league_id",
                    "team_a_grade",
                    "team_a_points",
                    "team_a_projected_points",
                    "team_a_team_key",
                    "team_b_grade",
                    "team_b_points",
                    "team_b_projected_points",
                    "team_b_team_key",
                    "week",
                    "week_start",
                    "week_end",
                    "winner_team_key",
                ]
            ]

            matchups["game_id"] = matchups["game_id"].astype(int)
            matchups["league_id"] = matchups["league_id"].astype(int)
            matchups["week"] = matchups["week"].astype(int)
            matchups["week_start"] = matchups["week_start"].astype("datetime64[D]")
            matchups["week_end"] = matchups["week_end"].astype("datetime64[D]")
            matchups["is_playoffs"] = matchups["is_playoffs"].astype(int)
            matchups["is_consolation"] = matchups["is_consolation"].astype(int)
            matchups["is_tied"] = matchups["is_tied"].astype(int)
            matchups["team_a_team_key"] = matchups["team_a_team_key"].astype(str)
            matchups["team_a_points"] = (
                matchups["team_a_points"].astype(float).round(decimals=2)
            )
            matchups["team_a_projected_points"] = (
                matchups["team_a_projected_points"].astype(float).round(decimals=2)
            )
            matchups["team_b_team_key"] = matchups["team_b_team_key"].astype(str)
            matchups["team_b_points"] = (
                matchups["team_b_points"].astype(float).round(decimals=2)
            )
            matchups["team_b_projected_points"] = (
                matchups["team_b_projected_points"].astype(float).round(decimals=2)
            )
            matchups["winner_team_key"] = matchups["winner_team_key"].astype(str)
            matchups["team_a_grade"] = matchups["team_a_grade"].astype(str)
            matchups["team_b_grade"] = matchups["team_b_grade"].astype(str)

            query = f"SELECT DISTINCT game_id, \
league_id, \
week, \
week_start, \
week_end, \
is_playoffs, \
is_consolation, \
is_tied, \
team_a_team_key, \
team_a_points, \
team_a_projected_points, \
team_b_team_key, \
team_b_points, \
team_b_projected_points, \
winner_team_key, \
team_a_grade, \
team_b_grade \
FROM raw.matchups \
WHERE (concat(game_id, week) <> concat({str(self.game_id)}, {str(nfl_week)})) \
ORDER BY game_id, \
league_id, \
week, \
team_a_team_key"

            data_upload(
                df=matchups,
                table_name="raw.matchups",
                query=query,
                path=PATH,
            )

            return matchups

        except Exception as e:
            log_print(
                error=e,
                module_="yahoo_query.py",
                func="matchups",
                game_id=self.game_id,
                nfl_week=nfl_week,
            )

    def teams(self):
        """
        stuff here
        """
        try:
            try:
                response = self.yahoo_query.get_league_standings()
            except Exception as e:
                if "Invalid week" in str(e):
                    return
                elif "token_expired" in str(e):
                    self.yahoo_query._authenticate()
                elif "Network is unreachable" in str(e):
                    log_print(
                        error=e,
                        module_="yahoo_query.py",
                        func="teams",
                        game_id=self.game_id,
                        sleep="15 min before retrying",
                    )
                    time.sleep(900)
                else:
                    log_print(
                        error=e,
                        module_="yahoo_query.py",
                        func="teams",
                        game_id=self.game_id,
                        sleep="1 hour before retrying",
                    )
                    time.sleep(3600)
                    try:
                        self.yahoo_query._authenticate()
                    except Exception as e:
                        log_print(
                            error=e,
                            module_="yahoo_query.py",
                            func="teams",
                            game_id=self.game_id,
                            sleep="30 min before 2nd retry",
                        )
                        time.sleep(1800)
                        self.yahoo_query._authenticate()
                response = self.yahoo_query.get_league_standings()

            teams = complex_json_handler(response)
            teams_standings = pd.DataFrame()
            for t in teams["teams"]:
                row = pd.json_normalize(complex_json_handler(t["team"]))
                if "managers.manager" not in row.columns:
                    manager = pd.json_normalize(
                        complex_json_handler(row["managers"][0][0]["manager"])
                    )
                else:
                    manager = pd.json_normalize(
                        complex_json_handler(row["managers.manager"][0])
                    )
                row = pd.concat([row, manager], axis=1)
                teams_standings = pd.concat([teams_standings, row])

            teams_standings["name"] = teams_standings["name"].str.decode("utf-8")

            if "draft_grade" not in teams_standings.columns:
                teams_standings["draft_grade"] = "Z"

            if "faab_balance" not in teams_standings.columns:
                teams_standings["faab_balance"] = 0

            if "clinched_playoffs" not in teams_standings.columns:
                teams_standings["clinched_playoffs"] = 0

            teams_standings["game_id"] = self.game_id
            teams_standings["league_id"] = self.league_id

            with open(TEAMS_FILE, "r") as file:
                c_teams = yaml.load(file, Loader=yaml.SafeLoader)

            correct_teams = pd.DataFrame.from_dict(c_teams["teams"])

            correct_teams = correct_teams.melt(
                id_vars=["season", "game_id", "league_id"],
                var_name="team_id",
                value_name="nickname",
            )
            correct_teams["team_key"] = (
                correct_teams["game_id"].astype(str)
                + ".l."
                + correct_teams["league_id"].astype(str)
                + ".t."
                + correct_teams["team_id"].astype(str)
            )
            correct_teams.dropna(subset=["nickname"], inplace=True)

            teams_standings = teams_standings.merge(
                correct_teams,
                how="left",
                left_on="team_key",
                right_on="team_key",
                suffixes=("_drop", ""),
            )

            teams_standings["nickname"] = teams_standings["nickname"].fillna(
                teams_standings["nickname_drop"]
            )

            teams_standings["nickname"] = np.where(
                teams_standings["nickname"] == "--hidden--",
                teams_standings["team_key"],
                teams_standings["nickname"],
            )

            teams_standings.dropna(
                subset=["game_id", "league_id", "manager_id", "team_key"], inplace=True
            )

            teams_standings = teams_standings[
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

            teams_standings.fillna(0, inplace=True)

            teams_standings["game_id"] = teams_standings["game_id"].astype(int)
            teams_standings["league_id"] = teams_standings["league_id"].astype(int)
            teams_standings["team_id"] = teams_standings["team_id"].astype(int)
            teams_standings["team_key"] = teams_standings["team_key"].astype(str)
            teams_standings["manager_id"] = teams_standings["manager_id"].astype(int)
            teams_standings["clinched_playoffs"] = teams_standings[
                "clinched_playoffs"
            ].astype(int)
            teams_standings["draft_grade"] = teams_standings["draft_grade"].astype(str)
            teams_standings["faab_balance"] = teams_standings["faab_balance"].astype(
                int
            )
            teams_standings["name"] = teams_standings["name"].astype(str)
            teams_standings["number_of_moves"] = teams_standings[
                "number_of_moves"
            ].astype(int)
            teams_standings["number_of_trades"] = teams_standings[
                "number_of_trades"
            ].astype(int)
            teams_standings["team_standings.playoff_seed"] = teams_standings[
                "team_standings.playoff_seed"
            ].astype(int)
            teams_standings["team_standings.rank"] = teams_standings[
                "team_standings.rank"
            ].astype(int)
            teams_standings["team_standings.outcome_totals.wins"] = teams_standings[
                "team_standings.outcome_totals.wins"
            ].astype(int)
            teams_standings["team_standings.outcome_totals.losses"] = teams_standings[
                "team_standings.outcome_totals.losses"
            ].astype(int)
            teams_standings["team_standings.outcome_totals.ties"] = teams_standings[
                "team_standings.outcome_totals.ties"
            ].astype(int)
            teams_standings["team_standings.outcome_totals.percentage"] = (
                teams_standings["team_standings.outcome_totals.percentage"]
                .astype(float)
                .round(decimals=4)
            )
            teams_standings["team_standings.points_for"] = (
                teams_standings["team_standings.points_for"]
                .astype(float)
                .round(decimals=2)
            )
            teams_standings["team_standings.points_against"] = (
                teams_standings["team_standings.points_against"]
                .astype(float)
                .round(decimals=2)
            )

            query = f'SELECT DISTINCT game_id, \
league_id, \
team_id, \
team_key, \
manager_id, \
clinched_playoffs, \
draft_grade, \
faab_balance, \
name, \
nickname, \
number_of_moves, \
number_of_trades, \
"team_standings.playoff_seed", \
"team_standings.rank", \
"team_standings.outcome_totals.wins", \
"team_standings.outcome_totals.losses", \
"team_standings.outcome_totals.ties", \
"team_standings.outcome_totals.percentage", \
"team_standings.points_for", \
"team_standings.points_against" \
FROM raw.teams \
WHERE game_id != {str(self.game_id)} \
ORDER BY game_id, \
league_id, \
team_id'

            data_upload(
                df=teams_standings,
                table_name="raw.teams",
                query=query,
                path=PATH,
            )

            return teams_standings

        except Exception as e:
            log_print(
                error=e,
                module_="yahoo_query.py",
                func="teams",
                game_id=self.game_id,
            )

    def weekly_points(self, nfl_week=None):
        """
        stuff here
        """
        try:
            sql_query = f"SELECT DISTINCT max_teams FROM prod.settings WHERE game_id = {str(self.game_id)}"
            teams = DatabaseCursor(PATH).copy_from_psql(sql_query)
            teams = teams["max_teams"].values[0]

            team_points_weekly = pd.DataFrame()
            for team in range(1, teams + 1):
                try:
                    response = self.yahoo_query.get_team_stats_by_week(
                        str(team), nfl_week
                    )

                except Exception as e:
                    if "Invalid week" in str(e):
                        return
                    elif "token_expired" in str(e):
                        self.yahoo_query._authenticate()
                    elif "Network is unreachable" in str(e):
                        log_print(
                            error=e,
                            module_="yahoo_query.py",
                            func="weekly_points",
                            game_id=self.game_id,
                            nfl_week=nfl_week,
                            sleep="15 min before retrying",
                        )
                        time.sleep(900)
                    else:
                        log_print(
                            error=e,
                            module_="yahoo_query.py",
                            func="weekly_points",
                            game_id=self.game_id,
                            nfl_week=nfl_week,
                            sleep="1 hour before retrying",
                        )
                        time.sleep(3600)
                        try:
                            self.yahoo_query._authenticate()
                        except Exception as e:
                            log_print(
                                error=e,
                                module_="yahoo_query.py",
                                func="weekly_points",
                                game_id=self.game_id,
                                nfl_week=nfl_week,
                                sleep="30 min before 2nd retry",
                            )
                            time.sleep(1800)
                            self.yahoo_query._authenticate()
                    try:
                        response = complex_json_handler(
                            self.yahoo_query.get_team_stats_by_week(str(team), nfl_week)
                        )
                    except:
                        response = self.yahoo_query.get_team_stats_by_week(
                            str(team), nfl_week
                        )

                time.sleep(1)

                team_pts = pd.DataFrame()
                try:
                    ttl_pts = pd.json_normalize(
                        complex_json_handler(response["team_points"])
                    )
                except:
                    ttl_pts = pd.json_normalize(response["team_points"])
                ttl_pts = ttl_pts[["total", "week"]]
                ttl_pts.rename(columns={"total": "final_points"}, inplace=True)

                try:
                    pro_pts = pd.json_normalize(
                        complex_json_handler(response["team_projected_points"])
                    )
                except:
                    pro_pts = pd.json_normalize(response["team_projected_points"])

                pro_pts = pro_pts[["total"]]
                pro_pts.rename(columns={"total": "projected_points"}, inplace=True)
                team_pts = pd.concat([ttl_pts, pro_pts], axis=1)
                team_pts["team_id"] = team

                team_points_weekly = pd.concat([team_points_weekly, team_pts])

            team_points_weekly["game_id"] = self.game_id
            team_points_weekly["league_id"] = self.league_id
            team_points_weekly["team_key"] = (
                team_points_weekly["game_id"].astype(str)
                + ".l."
                + team_points_weekly["league_id"].astype(str)
                + ".t."
                + team_points_weekly["team_id"].astype(str)
            )

            team_points_weekly = team_points_weekly[
                [
                    "game_id",
                    "league_id",
                    "team_id",
                    "team_key",
                    "week",
                    "final_points",
                    "projected_points",
                ]
            ]

            team_points_weekly["game_id"] = team_points_weekly["game_id"].astype(int)
            team_points_weekly["league_id"] = team_points_weekly["league_id"].astype(
                int
            )
            team_points_weekly["team_id"] = team_points_weekly["team_id"].astype(int)
            team_points_weekly["team_key"] = team_points_weekly["team_key"].astype(str)
            team_points_weekly["week"] = team_points_weekly["week"].astype(int)
            team_points_weekly["final_points"] = (
                team_points_weekly["final_points"].astype(float).round(decimals=2)
            )
            team_points_weekly["projected_points"] = (
                team_points_weekly["projected_points"].astype(float).round(decimals=2)
            )

            query = f"SELECT DISTINCT game_id, \
league_id, \
team_id, \
team_key, \
week, \
final_points, \
projected_points \
FROM raw.weekly_team_pts \
WHERE (concat(game_id, week) <> concat({str(self.game_id)}, {str(nfl_week)})) \
ORDER BY game_id, \
league_id, \
week, \
team_id"

            data_upload(
                df=team_points_weekly,
                table_name="raw.weekly_team_pts",
                query=query,
                path=PATH,
            )

            return team_points_weekly

        except Exception as e:
            log_print(
                error=e,
                module_="yahoo_query.py",
                func="weekly_points",
                game_id=self.game_id,
                nfl_week=nfl_week,
            )

    def all_game_keys(self):
        """
        stuff here
        """
        try:
            response = unpack_data(self.yahoo_query.get_all_yahoo_fantasy_game_keys())
            try:
                with open(TEAMS_FILE, "r") as file:
                    c_teams = yaml.load(file, Loader=yaml.SafeLoader)

                keys = pd.DataFrame.from_dict(c_teams["teams"])
                keys = keys[["game_id", "league_id", "season"]]

                league_keys = pd.DataFrame.from_dict(keys)

            except:
                league_keys = pd.DataFrame(
                    {"game_id": np.nan, "season": np.nan}, index=0
                )

            game_keys = pd.DataFrame()
            for r in response:
                row = pd.DataFrame(complex_json_handler(r["game"]), index=[0])
                game_keys = pd.concat([game_keys, row])

            game_keys.reset_index(drop=True, inplace=True)
            game_keys = game_keys[game_keys["season"] >= 2012]
            game_keys = game_keys.merge(
                league_keys,
                how="left",
                left_on=["game_id", "season"],
                right_on=["game_id", "season"],
            )
            game_keys = game_keys[
                ["game_id", "league_id", "season", "is_game_over", "is_offseason"]
            ]
            game_keys.drop_duplicates(ignore_index=True, inplace=True)

            query = "SELECT DISTINCT * FROM prod.game_keys"

            data_upload(
                df=game_keys,
                table_name="prod.game_keys",
                path=PATH,
                query=query,
            )

            return game_keys

        except Exception as e:
            log_print(
                error=e,
                module_="yahoo_query.py",
                func="all_game_keys",
                game_id=self.game_id,
            )

    def all_nfl_weeks(self):
        """
        stuff here
        """
        try:
            game_keys = DatabaseCursor(PATH).copy_from_psql(
                "SELECT DISTINCT game_id FROM prod.game_keys"
            )
            game_id = list(game_keys["game_id"])
            weeks = pd.DataFrame()
            for g in game_id:
                response = self.yahoo_query.get_game_weeks_by_game_id(str(g))
                for r in response:
                    row = pd.json_normalize(complex_json_handler(r["game_week"]))
                    row["game_id"] = g
                    weeks = pd.concat([weeks, row])

            weeks.rename(
                columns={
                    "display_name": "week",
                    "start": "week_start",
                    "end": "week_end",
                },
                inplace=True,
            )
            weeks = weeks[["week", "week_start", "week_end", "game_id"]]
            weeks = weeks.iloc[:, 1:]
            weeks["week_start"] = weeks["week_start"].astype("datetime64[D]")
            weeks["week_end"] = weeks["week_end"].astype("datetime64[D]")
            weeks.drop_duplicates(ignore_index=True, inplace=True)

            query = "SELECT DISTINCT * FROM prod.nfl_weeks"

            data_upload(
                df=weeks,
                table_name="prod.nfl_weeks",
                path=PATH,
                query=query,
            )

            return weeks

        except Exception as e:
            log_print(
                error=e,
                module_="yahoo_query.py",
                func="all_nfl_weeks",
                game_id=self.game_id,
            )
