import numpy as np
from pandas import DataFrame
import yaml

from Mom_WeeklyRankings_Export.utils import (
    get_laborday,
    nfl_weeks_pull,
    game_keys_pull,
    reg_season,
    post_season,
)
from Mom_WeeklyRankings_Export.yahoo_data import league_season_data
from assests.assests import PRIVATE, TEAMS


def data_pipeline():

    with open(PRIVATE) as file:
        credentials = yaml.load(file, Loader=yaml.SafeLoader)

    CONSUMER_KEY = credentials["YFPY_CONSUMER_KEY"]
    CONSUMER_SECRET = credentials["YFPY_CONSUMER_SECRET"]
    DATE = np.datetime64("2023-01-03", "D")

    NFL_WEEKS = nfl_weeks_pull(PRIVATE)
    START_OF_SEASON, SEASON = get_laborday(DATE)
    GAME_KEYS = game_keys_pull(PRIVATE, TEAMS)

    LEAGUE_ID = GAME_KEYS[GAME_KEYS["season"] == SEASON]["league_id"].values[0]
    GAME_ID = GAME_KEYS[GAME_KEYS["season"] == SEASON]["game_id"].values[0]
    NFL_WEEKS = NFL_WEEKS[NFL_WEEKS["game_id"] == GAME_ID]

    try:
        NFL_WEEK = NFL_WEEKS[["week", "week_start", "week_end"]][
            (NFL_WEEKS["week_end"] >= DATE) & (NFL_WEEKS["week_start"] <= DATE)
        ]
        CURRENT_WEEK = NFL_WEEK["week"].values[0]
        PREVIOUS_WEEK = CURRENT_WEEK - 1

    except:
        NFL_WEEK = DataFrame()

    league = league_season_data(
        auth_dir=PRIVATE.parents[1],
        league_id=LEAGUE_ID,
        game_id=GAME_ID,
        game_code="nfl",
        offline=False,
        all_output_as_json_str=False,
        consumer_key=CONSUMER_KEY,
        consumer_secret=CONSUMER_SECRET,
        browser_callback=True,
    )

    # league.all_game_keys()
    # league.all_nfl_weeks()
    league.metadata()
    league.settings()
    league.teams()

    # league.matchups(nfl_week=PREVIOUS_WEEK)
    # league.weekly_points(nfl_week=PREVIOUS_WEEK)

    # league.matchups(nfl_week=CURRENT_WEEK)
    # league.weekly_points(nfl_week=CURRENT_WEEK)

    reg_season(GAME_ID, PRIVATE)
    post_season(GAME_ID, PRIVATE)


if __name__ == "__main__":
    data_pipeline()
