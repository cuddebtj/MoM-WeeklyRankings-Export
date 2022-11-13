import numpy as np
from pandas import DataFrame
import yaml
from pathlib import Path
from time import time

from utils import get_laborday, nfl_weeks_pull, game_keys_pull, reg_season, post_season
from cust_logging import log_print
from yahoo_data import league_season_data
from assests import PRIVATE


def data_pipeline():
    date = np.datetime64("today", "D")

    try:
        with open(PRIVATE) as file:
            credentials = yaml.load(file, Loader=yaml.SafeLoader)

        CONSUMER_KEY = credentials["YFPY_CONSUMER_KEY"]
        CONSUMER_SECRET = credentials["YFPY_CONSUMER_SECRET"]
        DATE = np.datetime64(date, "D")

        NFL_WEEKS = nfl_weeks_pull()
        START_OF_SEASON, SEASON = get_laborday(DATE)
        GAME_KEYS = game_keys_pull()

        LEAGUE_ID = GAME_KEYS[GAME_KEYS["season"] == SEASON]["league_id"].values[0]
        GAME_ID = GAME_KEYS[GAME_KEYS["season"] == SEASON]["game_id"].values[0]
        NFL_WEEKS = NFL_WEEKS[NFL_WEEKS["game_id"] == GAME_ID]

        MAX_WEEK = NFL_WEEKS["week"].max() - 1
        END_OF_SEASON = NFL_WEEKS["week_end"][NFL_WEEKS["week"] == MAX_WEEK].values[0]

        try:
            NFL_WEEK = NFL_WEEKS[["week", "week_start", "week_end"]][
                (NFL_WEEKS["week_end"] >= DATE) & (NFL_WEEKS["week_start"] <= DATE)
            ]
            CURRENT_WEEK = NFL_WEEK["week"].values[0]

        except:
            NFL_WEEK = DataFrame()

    except Exception as e:
        log_print(
            error=e,
            module_="app.py",
            today=PRIVATE,
            credentials="Credential File",
            at_line="16",
        )

    league = league_season_data(
        auth_dir=PRIVATE.parent,
        league_id=LEAGUE_ID,
        game_id=GAME_ID,
        game_code="nfl",
        offline=False,
        all_output_as_json=False,
        consumer_key=CONSUMER_KEY,
        consumer_secret=CONSUMER_SECRET,
        browser_callback=True,
    )

    start = time()
    # league.all_game_keys()
    # league.all_nfl_weeks()
    # league.metadata()
    # league.settings()
    # league.matchups(nfl_week=CURRENT_WEEK)
    # league.weekly_points(nfl_week=CURRENT_WEEK)
    league.teams()
    reg_season(GAME_ID)
    post_season(GAME_ID)
    end = time()
    log_print(
        success="Individual Run",
        module_="sep_run_indy.py",
        date=DATE,
        start_of_season=START_OF_SEASON,
        end_of_season=END_OF_SEASON,
        time_to_complete=(end - start) / 60,
    )

if __name__ == '__main__':
    data_pipeline()