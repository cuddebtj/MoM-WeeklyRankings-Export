from pathlib import Path
import time

TOURNEY_PATH = list(Path().cwd().glob("**/tournament_results.txt"))
if TOURNEY_PATH == []:
    TOURNEY_PATH = list(Path().cwd().parent.glob("**/tournament_results.txt"))[0]
else:
    TOURNEY_PATH = TOURNEY_PATH[0]

LOG_PATH = list(Path().cwd().glob("**/logg.txt"))
if LOG_PATH == []:
    LOG_PATH = list(Path().cwd().parent.glob("**/logg.txt"))[0]
else:
    LOG_PATH = LOG_PATH[0]


def log_print(error=None, success=None, **kwargs):
    """
    some info here
    """
    items = ""
    with open(LOG_PATH, "a") as file:
        if error:
            if kwargs:
                for k, v in kwargs.items():
                    items += f"\t\t{list(kwargs.keys()).index(k)})\t {k}: {v}\n"
            error_str = f"********************************************************************************\nERROR:\n\t{error}\n{items}\tTimestamp: {time.ctime(time.time())}\n"
            file.write(error_str)

        else:
            if kwargs:
                for k, v in kwargs.items():
                    items += f"\t\t{list(kwargs.keys()).index(k)})\t {k}: {v}\n"
            success_str = f"--------------------------------------------------------------------------------\nSuccessful:\n\t{success}\n{items}\tTimestamp: {time.ctime(time.time())}\n"
            file.write(success_str)


def log_print_tourney(bracket=None, round_=None, final=None, **kwargs):
    """
    some text here
    """
    items = ""
    with open(TOURNEY_PATH, "a") as file:
        if bracket:
            for key, val in final.items():
                items += f"\t{key}: -> {val}\n"
            if kwargs:
                for k, v in kwargs.items():
                    items += f"\t\t{list(kwargs.keys()).index(k)})\t {k}: {v}\n"
            final_str = (
                f"***************Results for {bracket}*******************\n{items}"
            )
            file.write(final_str)

        else:
            if kwargs:
                items += f"\t{kwargs['right_comp']}\t\t\tvs.\t\t\t{kwargs['left_comp']}\n\t---------------------------------------------------\n\t{kwargs['right_score']}\t\t\t\t\t\t\t\t{kwargs['left_score']}\n"
            round_str = f"-------------------------------------------------------\n{round_}:\n{items}"
            file.write(round_str)