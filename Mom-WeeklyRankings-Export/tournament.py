import math
import pandas as pd

from cust_logging import log_print_tourney


def postseason_teams(
    season_df, game_id, max_teams, num_playoff_teams, num_conso_teams, num_toliet_teams
):
    """
    Calculate the playoff teams, consolation teams, and toilet teams
    """
    season_df.sort_values(
        ["Week", "Playoff Seed"], ascending=[True, True], inplace=True
    )
    playoff_teams = list(
        season_df["team_key"][season_df["Playoff Seed"] <= num_playoff_teams].unique()
    )
    if num_toliet_teams:
        if game_id == 314:
            conso_teams = list(
                season_df["team_key"][
                    season_df["Playoff Seed"] <= (max_teams - num_playoff_teams)
                ].unique()
            )
            conso_teams = [tm for tm in conso_teams if tm not in playoff_teams]
            toilet_teams = None

        elif game_id >= 406:
            conso_teams = list(
                season_df["team_key"][
                    season_df["Playoff Seed"] > num_playoff_teams
                ].unique()
            )
            toilet_teams = list(
                season_df["team_key"][
                    season_df["Playoff Seed"] > (max_teams - num_conso_teams)
                ].unique()
            )
            conso_teams = [tm for tm in conso_teams if tm not in toilet_teams]

        else:
            conso_teams = None
            toilet_teams = list(
                season_df["team_key"][
                    season_df["Playoff Seed"] > (max_teams - num_toliet_teams)
                ].unique()
            )
    else:
        toilet_teams = None
        conso_teams = None

    return playoff_teams, conso_teams, toilet_teams


def curr_playoff_picture(bracket, season_df, week, type):
    """
    Calculate current week playoffs if season ended on the current week.
    """
    for match in bracket.get_active_matches():
        right_comp = match.get_participants()[0].get_competitor()
        left_comp = match.get_participants()[1].get_competitor()

        right_comp_mask = (season_df["team_key"] == right_comp) & (
            season_df["Week"] == week
        )
        left_comp_mask = (season_df["team_key"] == left_comp) & (
            season_df["Week"] == week
        )

        right_team = season_df["Team"][right_comp_mask].values[0]
        right_manager = season_df["Manager"][right_comp_mask].values[0]

        left_team = season_df["Team"][left_comp_mask].values[0]
        left_manager = season_df["Manager"][left_comp_mask].values[0]

        season_df.loc[right_comp_mask, "opp_team_key"] = left_comp
        season_df.loc[right_comp_mask, "Opp Team"] = left_team
        season_df.loc[right_comp_mask, "Opp Manager"] = left_manager

        season_df.loc[left_comp_mask, "opp_team_key"] = right_comp
        season_df.loc[left_comp_mask, "Opp Team"] = right_team
        season_df.loc[left_comp_mask, "Opp Manager"] = right_manager

        season_df.loc[right_comp_mask, "Bracket"] = type
        season_df.loc[left_comp_mask, "Bracket"] = type


def playoff_weeks_calc(
    current_week, playoff_start, playoff_end, playoff_teams, conso_teams, toilet_teams
):
    """
    Calculate the weeks for the playoffs for the for loop
    """
    if (current_week < playoff_start) | (
        current_week > playoff_start & current_week > playoff_end
    ):
        playoff_weeks, conso_weeks, toilet_weeks = None, None, None

    elif current_week == playoff_start:
        playoff_weeks = [playoff_start]

        if len(conso_teams) < len(playoff_teams):
            conso_weeks = None
        else:
            conso_weeks = [playoff_start]

        if len(toilet_teams) < len(playoff_teams):
            toilet_weeks = None
        else:
            toilet_weeks = [playoff_start]

    elif current_week > playoff_start & current_week < playoff_end:
        playoff_weeks = range(playoff_start, current_week + 1)

        if len(conso_teams) < len(playoff_teams):
            conso_weeks = range(playoff_start + 1, current_week + 1)
        else:
            conso_weeks = range(playoff_start, current_week + 1)

        if len(toilet_teams) < len(playoff_teams):
            toilet_weeks = range(playoff_start + 1, current_week + 1)
        else:
            toilet_weeks = range(playoff_start, current_week + 1)

    elif current_week == playoff_end:
        playoff_weeks = range(playoff_start, playoff_end + 1)

        if len(conso_teams) < len(playoff_teams):
            conso_weeks = range(playoff_start + 1, playoff_end + 1)
        else:
            conso_weeks = range(playoff_start, playoff_end + 1)

        if len(toilet_teams) < len(playoff_teams):
            toilet_weeks = range(playoff_start + 1, playoff_end + 1)
        else:
            toilet_weeks = range(playoff_start, playoff_end + 1)

    return playoff_weeks, conso_weeks, toilet_weeks


def competition_rounds(bracket, type, week, season_df):
    """
    Calculate the rounds for the playoffs
    """
    for match in bracket.get_active_matches():
        right_comp = match.get_participants()[0].get_competitor()
        left_comp = match.get_participants()[1].get_competitor()

        right_comp_mask = (season_df["team_key"] == right_comp) & (
            season_df["Week"] == week
        )
        left_comp_mask = (season_df["team_key"] == left_comp) & (
            season_df["Week"] == week
        )

        right_score = season_df["Wk Pts"][right_comp_mask].values[0]
        right_pro_score = season_df["Wk Pro. Pts"][right_comp_mask].values[0]
        right_team = season_df["Team"][right_comp_mask].values[0]
        right_manager = season_df["Manager"][right_comp_mask].values[0]

        left_score = season_df["Wk Pts"][left_comp_mask].values[0]
        left_pro_score = season_df["Wk Pro. Pts"][left_comp_mask].values[0]
        left_team = season_df["Team"][left_comp_mask].values[0]
        left_manager = season_df["Manager"][left_comp_mask].values[0]

        season_df.loc[right_comp_mask, "opp_team_key"] = left_comp
        season_df.loc[right_comp_mask, "Opp Team"] = left_team
        season_df.loc[right_comp_mask, "Opp Manager"] = left_manager
        season_df.loc[right_comp_mask, "Opp Wk Pts"] = left_score
        season_df.loc[right_comp_mask, "Opp Wk Pro. Pts"] = left_pro_score

        season_df.loc[left_comp_mask, "opp_team_key"] = right_comp
        season_df.loc[left_comp_mask, "Opp Team"] = right_team
        season_df.loc[left_comp_mask, "Opp Manager"] = right_manager
        season_df.loc[left_comp_mask, "Opp Wk Pts"] = right_score
        season_df.loc[left_comp_mask, "Opp Wk Pro. Pts"] = right_pro_score

        season_df.loc[right_comp_mask, "Bracket"] = f"{type} R {week}"
        season_df.loc[left_comp_mask, "Bracket"] = f"{type} R {week}"

        if right_score > left_score:
            match.set_winner(right_comp)
            season_df.loc[right_comp_mask, "Wk W/L"] = "W"
            season_df.loc[left_comp_mask, "Wk W/L"] = "L"

        elif right_score < left_score:
            match.set_winner(left_comp)
            season_df.loc[right_comp_mask, "Wk W/L"] = "L"
            season_df.loc[left_comp_mask, "Wk W/L"] = "W"

        log_print_tourney(
            round_=f"{type} Week {week}",
            right_comp=right_comp,
            right_score=right_score,
            left_comp=left_comp,
            left_score=left_score,
        )


class Participant:
    """
    The Participant class represents a participant in a specific match.
    It can be used as a placeholder until the participant is decided.
    """

    def __init__(self, competitor=None):
        self.competitor = competitor

    def get_competitor(self):
        """
        Return the competitor that was set,
        or None if it hasn't been decided yet
        """
        return self.competitor

    def set_competitor(self, competitor):
        """
        Set competitor after you've decided who it will be,
        after a previous match is completed.
        """
        self.competitor = competitor


class Match:
    """
    A match represents a single match in a tournament, between 2 participants.
    It adds empty participants as placeholders for the winner and loser,
    so they can be accessed as individual object pointers.
    """

    def __init__(self, left_participant, right_participant):
        self.__left_participant = left_participant
        self.__right_participant = right_participant
        self.__winner = Participant()
        self.__loser = Participant()

    def set_winner(self, competitor):
        """
        When the match is over, set the winner competitor here and the loser will be set too.
        """
        if competitor == self.__left_participant.get_competitor():
            self.__winner.set_competitor(competitor)
            self.__loser.set_competitor(self.__right_participant.get_competitor())
        elif competitor == self.__right_participant.get_competitor():
            self.__winner.set_competitor(competitor)
            self.__loser.set_competitor(self.__left_participant.get_competitor())
        else:
            raise Exception("Invalid competitor")

    def get_winner_participant(self):
        """
        If the winner is set, get it here. Otherwise this return None.
        """
        return self.__winner

    def get_loser_participant(self):
        """
        If the winner is set, you can get the loser here. Otherwise this return None.
        """
        return self.__loser

    def get_participants(self):
        """
        Get the left and right participants in a list.
        """
        return [self.__left_participant, self.__right_participant]

    def is_ready_to_start(self):
        """
        This returns True if both of the participants coming in have their competitors "resolved".
        This means that the match that the participant is coming from is finished.
        It also ensure that the winner hasn't been set yet.
        """
        is_left_resolved = self.__left_participant.get_competitor() is not None
        is_right_resolved = self.__right_participant.get_competitor() is not None
        is_winner_resolved = self.__winner.get_competitor() is not None
        return is_left_resolved and is_right_resolved and not is_winner_resolved


class Tournament:
    """
    This is a single-elimination tournament where each match is between 2 competitors.
    It takes in a list of competitors, which can be strings or any type of Python object,
    but they should be unique. They should be ordered by a seed, with the first entry being the most
    skilled and the last being the least. They can also be randomized before creating the instance.
    Optional options dict fields:
    """

    def __init__(self, competitors_list, options={}):
        assert len(competitors_list) > 1
        self.__matches = []
        next_higher_power_of_two = int(
            math.pow(2, math.ceil(math.log2(len(competitors_list))))
        )
        winners_number_of_byes = next_higher_power_of_two - len(competitors_list)
        incoming_participants = list(map(Participant, competitors_list))
        incoming_participants.extend([None] * winners_number_of_byes)
        num_of_rounds = int(math.ceil(math.log2(len(incoming_participants))))

        round_ = 1
        while round_ <= num_of_rounds:
            if round_ == 1:
                half_length = int(len(incoming_participants) / 2)
                first = incoming_participants[0:half_length]
                last = incoming_participants[half_length:]
                last.reverse()
                round_1_winners = []
                round_1_losers = []
                for participant_pair in zip(first, last):
                    if participant_pair[1] is None:
                        round_1_winners.append(participant_pair[0])
                    elif participant_pair[0] is None:
                        round_1_winners.append(participant_pair[1])
                    else:
                        match = Match(participant_pair[0], participant_pair[1])
                        round_1_winners.append(match.get_winner_participant())
                        round_1_losers.append(match.get_loser_participant())
                        self.__matches.append(match)
                next_round = [round_1_winners, round_1_losers]
                round_ += 1

            elif round_ > 1:
                while True:
                    if len(next_round[0]) > 1:
                        bracket = next_round[0]
                        half_length = int(len(bracket) / 2)
                        first = bracket[0:half_length]
                        last = bracket[half_length:]
                        last.reverse()
                        next_round_winners = []
                        next_round_losers = []
                        for participant_pair in zip(first, last):
                            if participant_pair[1] is None:
                                next_round_winners.append(participant_pair[0])
                            elif participant_pair[0] is None:
                                next_round_winners.append(participant_pair[1])
                            else:
                                match = Match(participant_pair[0], participant_pair[1])
                                next_round_winners.append(
                                    match.get_winner_participant()
                                )
                                next_round_losers.append(match.get_loser_participant())
                                self.__matches.append(match)

                        if winners_number_of_byes > 0 and len(next_round[0]) > len(
                            next_round[1]
                        ):
                            next_round.insert(0, next_round_losers)
                            next_round.insert(0, next_round_winners)
                            winners_number_of_byes = 0
                        else:
                            next_round.append(next_round_winners)
                            next_round.append(next_round_losers)

                        try:
                            next_round.remove(bracket)
                        except:
                            continue

                        next_round = [ele for ele in next_round if ele != []]
                        if len(next_round) == len(competitors_list):
                            break
                    round_ += 1

        self.__final = next_round

    def __iter__(self):
        return iter(self.__matches)

    def get_active_matches(self):
        """
        Returns a list of all matches that are ready to be played.
        """
        return [match for match in self.get_matches() if match.is_ready_to_start()]

    def get_matches(self):
        """
        Returns a list of all matches for the tournament.
        """
        return self.__matches

    def get_active_matches_for_competitor(self, competitor):
        """
        Given the string or object of the competitor that was supplied
        when creating the tournament instance,
        returns a list of Matches that they are currently playing in.
        """
        matches = []
        for match in self.get_active_matches():
            competitors = [
                participant.get_competitor() for participant in match.get_participants()
            ]
            if competitor in competitors:
                matches.append(match)
        return matches

    def get_final(self):
        """
        Returns None if the winner has not been decided yet,
        and returns a list containing the single victor otherwise.
        """
        if len(self.get_active_matches()) > 0:
            return None

        final_dict = {}
        for team in range(len(self.__final)):
            final_dict[f"{team+1}"] = self.__final[team][0].get_competitor()

        return final_dict

    def add_win(self, match, competitor):
        """
        Set the victor of a match, given the competitor string/object and match.
        """
        match.set_winner(competitor)
