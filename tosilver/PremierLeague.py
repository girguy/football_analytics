import pandas as pd
import numpy as np
from Team import Team

# Meaning of these abreviations are in data_abreviation.txt
COLUMNS_TO_KEEP = [
    'Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'HST', 'AST', 'HC', 'AC']

FEATURES_NAME = [
    'date', 'season', 'gameNumber', 'homeTeam', 'awayTeam', 'FTHG', 'FTAG',
    'HST', 'AST', 'HC', 'AC', 'HPKST', 'HPKGS', 'HPKGC', 'HPKC', 'HGD', 'HS',
    'HWS', 'HF', 'APKST', 'APKGS', 'APKGC', 'APKC', 'AGD', 'AS', 'AWS',
    'AF', 'PKSTD', 'PKGD', 'PKCD', 'GDD', 'SD', 'WSD', 'FD', 'avgGoalSH',
    'avgGoalSA', 'avgGoalSHH', 'avgGoalCHH', 'avgGoalSAA', 'avgGoalCAA',
    'HR', '>1.5', '>2.5'
    ]


class PremierLeague:
    """
    This class is designed for processing and analyzing Premier League
    football data.

    Attributes:
        _id_game (int): Identifier for the game, incremented with each
            game processed.
        _teams (dict): Dictionary to store team data, keyed by team names.
        _epl_games (DataFrame): DataFrame holding the raw EPL games data.
        _teams_dataset (DataFrame): DataFrame for processed data related
            to teams.
        _dataset (DataFrame): DataFrame for processed and aggregated
            game statistics.
        _k (int): Number of past games to consider for statistical analysis.
        _season (str): The football season year.
        _goal_scored_h (list): List to track goals scored at home across
            all games.
        _goal_scored_a (list): List to track goals scored away across
            all games.
        _avg_goal_scored_home (list): List to track the average home
            goals scored.
        _avg_goal_scored_away (list): List to track the average away
            goals scored.
        _max_nb_games (int): Maximum number of games in the dataset.

    Methods:
        create_dataset(df): Processes the raw EPL games DataFrame and creates
            the main dataset.
        read_games_file(): Reads and processes each game, compiling the main
            dataset.
        create_teams(teams): Initializes Team objects for each unique team in
            the dataset.
        update_teams(game, home_team, away_team, id_game): Updates team
            statistics after each game.
        get_average_goals(home_team, away_team): Calculates average goals
            stats for home and away teams.
        result_15_25(home_goals, away_goals): Determines game results and
            goals (>1.5, >2.5).
        update_season_stats(home_scored_goals, away_scored_goals): Updates
            season stats with new game data.
        create_teams_dataset(year): Creates a dataset of team-specific
            data for the given year.
    """

    def __init__(self, k, season, id_first_game):
        """
        Initializes the PremierLeague class with specified parameters.

        Args:
            k (int): The number of past games to consider for statistical
            analysis.
            season (str): The football season year.
            id_first_game (int): The starting identifier for games.
        """
        self._id_game = id_first_game
        self._teams = {}
        self._epl_games = None
        self._teams_dataset = None
        self._dataset = None
        self._k = k
        self._season = season
        self._goal_scored_h = []
        self._goal_scored_a = []
        self._avg_goal_scored_home = []
        self._avg_goal_scored_away = []
        self._max_nb_games = 0

    def create_dataset(self, df):
        """
        Processes the raw EPL games DataFrame and prepares it for analysis.

        Args:
            df (DataFrame): Raw DataFrame containing EPL games data.
        """
        self._epl_games = df
        self._epl_games = self._epl_games[COLUMNS_TO_KEEP]
        self._max_nb_games = self._epl_games.shape[0]
        self._dataset = self.read_games_file()
        self._dataset = pd.DataFrame(self._dataset, columns=FEATURES_NAME)

    def read_games_file(self):
        """
        Processes each game in the EPL games DataFrame to compile the main
        dataset.

        Returns:
            list: A list of tuples, each representing processed data for a
            single game.
        """
        self._teams = self.create_teams(self._epl_games['HomeTeam'])

        dataset = []

        if self._season in ['2005', '2012', '2013', '2015']:
            nb_games = self._epl_games.shape[0] - 1
        else:
            nb_games = self._epl_games.shape[0]

        for i in range(nb_games):
            game = self._epl_games.iloc[i]
            date_game = game['Date']
            self._id_game += 1
            home_team = game['HomeTeam']  # name of the home team
            away_team = game['AwayTeam']  # name of the away team

            # If both teams has played at least k+1 game then this game
            # can be put into the dataset so I need to increment the number
            # of games played in each team in order to keep a counter put
            if (self._teams[home_team]._games_played >= self._k) and \
               (self._teams[away_team]._games_played >= self._k):

                game_statistics = game['FTHG'], game['FTAG'], game['HST'],  \
                    game['AST'], game['HC'], game['AC']

                home_statistics = self._teams[home_team]\
                    .compute_statistics(self._k)
                away_statistics = self._teams[away_team]\
                    .compute_statistics(self._k)
                differential_statistics = Team.compute_differential_statistics(
                    home_statistics, away_statistics
                    )

                home_streak = self._teams[home_team].compute_streak(self._k)
                away_streak = self._teams[away_team].compute_streak(self._k)
                differential_streak = Team.compute_differential_streak(
                    home_streak, away_streak
                    )

                home_weighted_streak = self._teams[home_team] \
                    .compute_weighted_streak(self._k)
                away_weighted_streak = self._teams[away_team] \
                    .compute_weighted_streak(self._k)
                differential_weighted_streak = Team \
                    .compute_differential_weighted_streak(
                        home_weighted_streak, away_weighted_streak
                        )

                home_form = self._teams[home_team].get_form()
                away_form = self._teams[away_team].get_form()
                differential_form = Team.compute_differential_form(
                    home_form, away_form
                    )

                home_features = home_statistics + home_streak \
                    + home_weighted_streak + home_form

                away_features = away_statistics + away_streak \
                    + away_weighted_streak + away_form

                differential_features = \
                    differential_statistics + differential_streak + \
                    differential_weighted_streak + differential_form

                avg_goals_features = self.get_average_goals(
                    self._teams[home_team], self._teams[away_team]
                    )

                # Home win (3), home draw (1), home lose (0)
                # > 1.5 goals = True
                # > 2.5 goals = True
                game_goals_results = self.result_15_25(
                    game['FTHG'], game['FTAG']
                    )

                total_features = (date_game,) + (self._season, ) + \
                    (self._id_game,) + (home_team, away_team) + \
                    game_statistics + home_features + \
                    away_features + differential_features + \
                    avg_goals_features + game_goals_results

                dataset.append(total_features)

            self.update_season_stats(game['FTHG'], game['FTAG'])
            self._teams[home_team], self._teams[away_team] = self.update_teams(
                game, self._teams[home_team], self._teams[away_team],
                self._id_game
                )

        return dataset

    def create_teams(self, teams):
        """
        Creates and initializes Team objects for each unique team in the
        dataset.

        Args:
            teams (list): A list of team names.

        Returns:
            dict: A dictionary of Team objects, keyed by team names.
        """
        teams = list(dict.fromkeys(teams.tolist()))
        teams = sorted([team for team in teams if team == team])
        dict_teams = {}
        for team in teams:
            dict_teams[team] = Team(team, 1)
        return dict_teams

    def update_teams(self, game, home_team, away_team, id_game):
        """
        Updates the statistics for home and away teams after a game.

        Args:
            game (DataFrame): A DataFrame row containing the game's data.
            home_team (Team): The home team's Team object.
            away_team (Team): The away team's Team object.
            id_game (int): Identifier for the game.

        Returns:
            tuple: Updated Team objects for the home and away teams.
        """
        home_team.update(
            game['FTHG'], game['FTAG'], game['HST'], game['HC'],
            away_team._form[-1], 'H', id_game, game['Date']
            )

        # -2 and not -1 otherwise it will update the awayTeam form value with
        # the updated homeTeam form value !
        away_team.update(
            game['FTAG'], game['FTHG'], game['AST'], game['AC'],
            home_team._form[-2], 'A', id_game, game['Date']
            )

        return home_team, away_team

    def get_average_goals(self, home_team, away_team):
        """
        Calculates average goals for home and away teams.

        Args:
            home_team (Team): The home team's Team object.
            away_team (Team): The away team's Team object.

        Returns:
            tuple: A tuple containing average goals statistics.
        """
        # avg of goals scored at home in the league
        avg_goal_sh = self._avg_goal_scored_home[-1]
        # avg of goals scored at away in the league
        avg_goal_sa = self._avg_goal_scored_away[-1]
        # avg of goals scored at home by home team
        avg_goal_shh = np.mean(home_team._goal_scored_h)
        # avg of goals conceded at home by home team
        avg_goal_chh = np.mean(home_team._goal_conceded_h)
        # avg of goals scored away by away team
        avg_goal_saa = np.mean(away_team._goal_scored_a)
        # avg of goals conceded away by away team
        avg_goal_caa = np.mean(away_team._goal_conceded_a)

        return avg_goal_sh, avg_goal_sa, avg_goal_shh, avg_goal_chh, \
            avg_goal_saa, avg_goal_caa

    def result_15_25(self, home_goals, away_goals):
        """
        Determines the match result and whether the game had over
        1.5 or 2.5 goals.

        Args:
            home_goals (int): Number of goals scored by the home team.
            away_goals (int): Number of goals scored by the away team.

        Returns:
            tuple: A tuple containing the match result and boolean values for
            >1.5 and >2.5 goals.
        """
        if home_goals >= away_goals:
            if home_goals == away_goals:
                res = 1
            else:
                res = 3
        else:
            res = 0

        total_goals = home_goals+away_goals
        if total_goals > 1.5:
            goal_15 = True
        else:
            goal_15 = False
        if total_goals > 2.5:
            goal_25 = True
        else:
            goal_25 = False

        return res, goal_15, goal_25

    def update_season_stats(self, home_scored_goals, away_scored_goals):
        """
        Updates the season statistics for goals scored by home and away teams.

        Args:
            home_scored_goals (int): Goals scored by the home team.
            away_scored_goals (int): Goals scored by the away team.
        """
        self._goal_scored_h.append(home_scored_goals)
        self._goal_scored_a.append(away_scored_goals)
        self._avg_goal_scored_home.append(np.mean(self._goal_scored_h))
        self._avg_goal_scored_away.append(np.mean(self._goal_scored_a))

    def create_teams_dataset(self, year):
        """
        Creates a dataset for each team with their respective data for the
        given year.

        Args:
            year (str): The year for which the team data is to be created.

        This method ag data is retrieved, and a new column 'team' is added
        to indicate the team name. The data is then concatenated into a
        single DataFrame, sorted by the game number.
        """
        frames = []
        for team in self._teams.keys():
            team_data = self._teams[team].get_team_data(year)
            team_col = [team for i in range(team_data.shape[0])]
            team_data['team'] = team_col
            frames.append(team_data)

        self._teams_dataset = pd.concat(frames).sort_values(by='gameNumber',
                                                            ascending=True)
