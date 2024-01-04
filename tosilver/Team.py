import pandas as pd

FEATURES_NAME = [
    'date', 'season', 'gameNumber', 'matchDay', 'RES', 'PTS', 'GS',
    'GSCUM', 'GC', 'GCCUM', 'GD', 'ST', 'C', 'WP', 'FORM'
    ]


class Team:
    """
    Represents a football team with various statistics and historical data.

    Attributes:
        _id_game (list): List of game IDs associated with the team.
        _name (str): Name of the team.
        _games_played (int): Number of games played.
        _matchDay (list): List of match days.
        _date (list): List of dates when games were played.
        _res (list): Game results (3 for win, 1 for draw, 0 for loss).
        _points (list): Cumulative points earned.
        _goal_scored_h (list): Goals scored at home.
        _goal_scored_a (list): Goals scored away.
        _goal_scored (list): Total goals scored.
        _goal_scoredCum (list): Cumulative goals scored.
        _goal_conceded_h (list): Goals conceded at home.
        _goal_conceded_a (list): Goals conceded away.
        _goal_conceded (list): Total goals conceded.
        _goal_conceded_cum (list): Cumulative goals conceded.
        _goal_difference (list): Goal difference.
        _shot_target (list): Number of shots on target.
        _corner (list): Number of corners.
        _where_played (list): List indicating where the game was played
            ('H' for home, 'A' for away).
        _form (list): Team's form, calculated using a formula based on game
            results and opponent's form.
        _gamma (float): Factor used in form calculation.
        _streak (list): Team's performance streak.
        _weighted_streak (list): Weighted streak based on recent performances.

    Methods:
        update: Updates team statistics based on a single game's data.
        compute_statistics: Computes average statistics over the
            last 'k' games.
        compute_streak: Calculates the team's recent performance streak.
        compute_weighted_streak: Calculates a weighted streak based on recent
            performances.
        compute_differential_statistics: Computes differential statistics
            between home and away teams.
        compute_differential_streak: Computes the differential streak
            between two teams.
        compute_differential_weighted_streak: Computes the differential
            weighted streak between two teams.
        get_form: Returns the current form of the team.
        compute_differential_form: Computes the differential form between
            two teams.
        add_res: Adds a game result and updates the team's form and points.
        add_goal_scored: Adds goals scored data for a game.
        add_goal_conceded: Adds goals conceded data for a game.
        add_goal_difference: Updates the team's goal difference.
        add_shot_target: Adds the number of shots on target for a game.
        add_corner: Adds the number of corners for a game.
        add_where_played: Records where the game was played (home or away).
        update_form: Updates the team's form based on the game result and
            opponent's form.
        update_calendar: Updates the team's calendar with the game date and ID.
        update_cumulative_list: Updates a cumulative list
            (like total goals scored) with new data.
        team_state: Prints the current state of the team including various
            statistics.
        get_team_data: Returns a DataFrame with the team's data for a
            specified year.
    """

    def __init__(self, name, form):
        """
        Initializes the Team object with a name and initial form.

        Args:
            name (str): Name of the team.
            form (float): Initial form of the team.
        """
        self._id_game = []
        self._name = name
        self._games_played = 0
        self._matchDay = []
        self._date = []

        self._res = []  # 3, 1, 0
        self._points = []

        self._goal_scored_h = []
        self._goal_scored_a = []
        self._goal_scored = []
        self._goal_scoredCum = []

        self._goal_conceded_h = []
        self._goal_conceded_a = []
        self._goal_conceded = []
        self._goal_conceded_cum = []

        self._goal_difference = []  # list of the goal difference # F

        self._shot_target = []  # nb of shots # F
        self._corner = []  # nb of corners # F
        self._where_played = []

        self._form = [form]
        self._gamma = 0.33

        self._streak = []
        self._weighted_streak = []

    def update(self, goal_scored, goal_conceded, shotTarget, corner,
               opponent_form, where_played, id_game, date):
        """
        Updates team statistics based on the result of a single game.

        Args:
            goal_scored (int): Goals scored by the team in the game.
            goal_conceded (int): Goals conceded by the team in the game.
            shotTarget (int): Number of shots on target.
            corner (int): Number of corners.
            opponent_form (float): Form of the opponent team.
            where_played (str): 'H' for home or 'A' for away.
            id_game (int): ID of the game.
            date (str): Date of the game.
        """
        self.update_calendar(date, id_game)
        self.add_res(goal_scored, goal_conceded, opponent_form)
        self.add_goal_scored(goal_scored, where_played)
        self.add_goal_conceded(goal_conceded, where_played)
        self.add_goal_difference()
        self.add_shot_target(shotTarget)
        self.add_corner(corner)
        self.add_where_played(where_played)

    def compute_statistics(self, k):
        """
        Computes and returns average statistics over the last 'k' games.

        Args:
            k (int): The number of recent games to consider.

        Returns:
            tuple: A tuple containing average statistics
                (shot target, goals scored and conceded, etc.).
        """
        past_k_shot_target = sum(self._shot_target[-k:])/k  # mean
        past_k_goal_scored = sum(self._goal_scored[-k:])/k  # mean
        past_k_goal_conceded = sum(self._goal_conceded[-k:])/k  # mean
        past_k_corners = sum(self._corner[-k:])/k  # mean
        goal_dif = self._goal_difference[-1]
        return past_k_shot_target, past_k_goal_scored, past_k_goal_conceded, \
            past_k_corners, goal_dif

    def compute_streak(self, k):
        """
        Calculates the team's performance streak over the last 'k' games.

        Args:
            k (int): The number of recent games to consider.

        Returns:
            tuple: A tuple containing the calculated streak.
        """
        streak = sum(self._res[-k:])/(3*k)
        self._streak.append(streak)
        return (streak,)  # ( ,) used to create a tuple

    def compute_weighted_streak(self, k):
        """
        Calculates a weighted streak that gives more importance to
        recent games.

        Args:
            k (int): The number of recent games to consider.

        Returns:
            tuple: A tuple containing the calculated weighted streak.
        """
        K = 3*k*(k+1)
        weight = self._games_played-k-1
        weighted_streak = 0
        for i, p in enumerate(range(self._games_played-k, self._games_played)):
            weighted_streak += 2*(p-weight)*self._res[-k+i]*(1/K)
        self._weighted_streak.append(weighted_streak)
        return (weighted_streak,)  # ( ,) used to create a tuple

    def compute_differential_statistics(home_statistics, away_statistics):
        """
        Computes differential statistics between home and away teams.

        Args:
            home_statistics (tuple): A tuple of statistics for the home team.
            away_statistics (tuple): A tuple of statistics for the away team.

        Returns:
            tuple: A tuple containing differential statistics.
        """
        past_k_shot_target_dif = home_statistics[0] - away_statistics[0]
        past_k_goal_dif = home_statistics[1] - away_statistics[1]
        past_k_corner_dif = home_statistics[2] - away_statistics[2]
        goal_dif_dif = home_statistics[3] - away_statistics[3]
        return past_k_shot_target_dif, past_k_goal_dif, \
            past_k_corner_dif, goal_dif_dif

    def compute_differential_streak(home_streak, away_streak):
        """
        Computes the difference in performance streaks between home
        and away teams.

        Args:
            home_streak (float): The streak of the home team.
            away_streak (float): The streak of the away team.

        Returns:
            tuple: A tuple containing the differential streak.
        """
        # ( ,) used to create a tuple
        return ((home_streak[0] - away_streak[0]),)

    def compute_differential_weighted_streak(home_weighted_streak,
                                             away_weighted_streak):
        """
        Computes the difference in weighted streaks between home
        and away teams.

        Args:
            home_weighted_streak (float): Weighted streak of the home team.
            away_weighted_streak (float): Weighted streak of the away team.

        Returns:
            tuple: A tuple containing the differential weighted streak.
        """
        return ((home_weighted_streak[0] - away_weighted_streak[0]),)

    def get_form(self):
        """
        Gets the current form of the team.

        Returns:
            tuple: A tuple containing the current form.
        """
        return (self._form[-1],)

    def compute_differential_form(home_form, away_form):
        """
        Computes the difference in form between home and away teams.

        Args:
            home_form (float): The form of the home team.
            away_form (float): The form of the away team.

        Returns:
            tuple: A tuple containing the differential form.
        """
        return ((home_form[0] - away_form[0]),)

    def add_res(self, goal_scored, goal_conceded, opponent_form):
        """
        Adds the result of a game and updates the form and points.

        Args:
            goal_scored (int): Goals scored by the team.
            goal_conceded (int): Goals conceded by the team.
            opponent_form (float): Form of the opponent team.
        """
        if goal_scored >= goal_conceded:
            if goal_scored == goal_conceded:
                self._res.append(1)
                self.update_form(opponent_form, 1)
            else:
                self._res.append(3)
                self.update_form(opponent_form, 3)
        else:
            self._res.append(0)
            self.update_form(opponent_form, 0)
        self.update_cumulative_list(self._res, self._points)

    def add_goal_scored(self, goals, where_played):
        """
        Adds goals scored by the team and updates cumulative goals.

        Args:
            goals (int): Number of goals scored.
            where_played (str): 'H' for home, 'A' for away.
        """
        if where_played == 'H':
            self._goal_scored_h.append(goals)
        else:
            self._goal_scored_a.append(goals)
        self._goal_scored.append(goals)
        self.update_cumulative_list(self._goal_scored, self._goal_scoredCum)

    def add_goal_conceded(self, goals, where_played):
        """
        Adds goals conceded by the team and updates cumulative goals conceded.

        Args:
            goals (int): Number of goals conceded.
            where_played (str): 'H' for home, 'A' for away.
        """
        if where_played == 'H':
            self._goal_conceded_h.append(goals)
        else:
            self._goal_conceded_a.append(goals)
        self._goal_conceded.append(goals)
        self.update_cumulative_list(
            self._goal_conceded,
            self._goal_conceded_cum
            )

    def add_goal_difference(self):
        """
        Updates the goal difference for the team.
        """
        self._goal_difference.append(
            sum(self._goal_scored) - sum(self._goal_conceded))

    def add_shot_target(self, shotTarget):
        """
        Adds the number of shots on target for a game.

        Args:
            shotTarget (int): Number of shots on target.
        """
        self._shot_target.append(shotTarget)

    def add_corner(self, corner):
        """
        Adds the number of corners for a game.

        Args:
            corner (int): Number of corners.
        """
        self._corner.append(corner)

    def add_where_played(self, where_played):
        """
        Records where the game was played (home or away).

        Args:
            where_played (str): 'H' for home, 'A' for away.
        """
        if where_played == 'H':
            self._where_played.append(where_played)
        else:
            self._where_played.append(where_played)

    def update_form(self, opponent_form, res):
        """
        Updates the team's form based on the game result and opponent's form.

        Args:
            opponent_form (float): Form of the opponent team.
            res (int): Result of the game (3 for win, 1 for draw, 0 for loss).
        """
        if res == 3:
            self._form.append(self._form[-1] + self._gamma*opponent_form)
        elif res == 0:
            self._form.append(self._form[-1] - self._gamma*self._form[-1])
        else:
            self._form.append(
                self._form[-1] - self._gamma*(self._form[-1] - opponent_form)
                )

    def update_calendar(self, date, id_game):
        """
        Updates the team's calendar with game date and ID.

        Args:
            date (str): Date of the game.
            id_game (int): ID of the game.
        """
        self._games_played += 1
        self._id_game.append(id_game)
        self._matchDay.append(self._games_played)
        self._date.append(date)

    def update_cumulative_list(self, var, var_cum):
        """
        Updates a cumulative list with the latest value from another list.

        This method is used for maintaining a running total of various
        statistics like goals scored, points, etc. It adds the latest
        value from 'var' to the last value in 'var_cum' to update
        the cumulative total.

        Args:
            var (list): The list from which the latest value is taken.
            var_cum (list): The cumulative list to be updated.
        """
        if not var_cum:
            var_cum.append(var[-1])  # len(var) = 1
        else:
            var_cum.append(var_cum[-1] + var[-1])

    def get_team_data(self, year):
        """
        Creates a DataFrame with various statistics for the team for a
        specified year.

        Args:
            year (str): The year for which data is to be compiled.

        Returns:
            DataFrame: A DataFrame containing the team's data for the
            specified year.
        """
        year_col = [year for i in range(len(self._id_game))]
        dataset = [
            self._date, year_col, self._id_game, self._matchDay, self._res,
            self._points, self._goal_scored, self._goal_scoredCum,
            self._goal_conceded, self._goal_conceded_cum,
            self._goal_difference, self._shot_target, self._corner,
            self._where_played, self._form[0:-1]
            ]

        dataset = pd.DataFrame(dataset).transpose()
        dataset.columns = FEATURES_NAME

        return dataset
