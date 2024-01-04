import pandas as pd
import numpy as np
#from scipy.stats import poisson
import math

WEIGTH_PAST_SEASON = 1
WEIGTH_CURRENT_SEASON = 1
BOOKING_MARGIN = 1

class PoissonDistribution:
    def __init__(self, statistics, teamsGames, fixtures, pastSeason, curSeason, nbGames):
        self._statistics = statistics
        self._teamsGames = teamsGames
        self._fixtures = fixtures
        self._pastSeason = pastSeason
        self._curSeason = curSeason
        self._nbGames = nbGames
            
    def get_poisson_probabilities(self):
        futureFixtures = self.get_future_fixtures(self._fixtures, self._nbGames)
        mostRecentMatchDay = self._teamsGames.query(f"season == '{self._curSeason}'")['matchDay'].sort_values(ascending=False).to_list()[0]

        poissonResults = []
        for index, row in futureFixtures.iterrows():
            probs, hTAttStrength, hTDefStrength, aTAttStrength, aTDefStrength = self.get_probs_fixture(row['homeTeam'], row['awayTeam'], mostRecentMatchDay, self._pastSeason, self._curSeason)
            line1 = row.to_list()
            line2 = [probs['Win'], probs['Loose'], probs['Draw'], probs['BothScore'], probs['Over 1.5'], probs['Over 2.5'],  probs['Over 3.5'], self._curSeason]
            line3 = [hTAttStrength, hTDefStrength, aTAttStrength, aTDefStrength]
            poissonResults.append([*line1, *line2, *line3])
        
        colNames = ['homeTeam', 'awayTeam', 'Win', 'Loose', 'Draw', 'BothScore', '>1.5', '>2.5', '>3.5', 'Season',
            'hTAttStrength', 'hTDefStrength', 'aTAttStrength', 'aTDefStrength']

        return pd.DataFrame(poissonResults, columns=colNames) 


    def get_future_fixtures(self, fixtures, nbGames = 10):
        fixtures = fixtures.query('played ==  False')[0:nbGames]
        return fixtures[['homeTeam', 'awayTeam']]

    def get_probs_fixture(self, homeTeam, awayTeam, matchDay, pastSeason, curSeason):
        leagueAvgScoredHome,leagueAvgScoredAway, pastYearGames = self.extract_league_averages(pastSeason,
                                                                                        curSeason,
                                                                                        matchDay = matchDay)
        leagueAvgConcededAway = leagueAvgScoredHome
        leagueAvgConcededHome = leagueAvgScoredAway

        curYearGames = self.extract_current_year_games(curSeason, matchDay = matchDay)

        hTGoalExpect, aTGoalExpect, hTAttStrength, hTDefStrength, aTAttStrength, aTDefStrength = self.get_teams_goal_expectancy(
            homeTeam, awayTeam,
            curYearGames, pastYearGames,
            leagueAvgScoredHome, leagueAvgConcededHome,
            leagueAvgScoredAway, leagueAvgConcededAway,
            pastSeason)

        probabilities = self.get_fixture_probs(hTGoalExpect, aTGoalExpect)
        return probabilities, hTAttStrength, hTDefStrength, aTAttStrength, aTDefStrength

    def extract_league_averages(self, pastSeason, curSeason, matchDay = False):
        # matchDay has to be minimum equal to 5
        # Extract the number of goals scored by EPL team (H/A) in the current season
        curNbHomeGoalScored = self.extract_league_goals_scored(curSeason, 'H', matchDay)
        curNbAwayGoalScored = self.extract_league_goals_scored(curSeason, 'A', matchDay)
        
        # Extract the number of games played by EPL team (H/A) in the past season
        pastHomeGamesToExtract = 380 - self.extract_nb_games_played(curSeason, 'H', matchDay = False)
        pastAwayGamesToExtract = 380 - self.extract_nb_games_played(curSeason, 'A', matchDay = False)
        #
        pastYearGames = self._teamsGames.query(f"season == '{str(pastSeason)}'")[['gameNumber', 'GS', 'GC', 'WP', 'team']]
        pastYearGames = pastYearGames.sort_values(by=['gameNumber'], ascending=False)

        pastYearHomeGames = pastYearGames.query("WP == 'H'")[0:pastHomeGamesToExtract]
        pastYearAwayGames = pastYearGames.query("WP == 'A'")[0:pastAwayGamesToExtract]

        pastNbHomeGoalScored = int(sum(pastYearHomeGames['GS']))
        pastNbAwayGoalScored = int(sum(pastYearAwayGames['GS']))

        # League average goal scored at home and away
        leagueAvgScoredHome = (pastNbHomeGoalScored + curNbHomeGoalScored)/380
        leagueAvgScoredAway = (pastNbAwayGoalScored + curNbAwayGoalScored)/380

        return leagueAvgScoredHome,leagueAvgScoredAway, pastYearGames
    
    def extract_league_goals_scored(self, season, wherePlayed, matchDay = False):
        goalScored = self._statistics.query(f"season == '{str(season)}'")['FT'+wherePlayed+'G']
        if matchDay:
            return int(sum(goalScored[0:matchDay*10]))
        else:
            return int(sum(goalScored))
    
    def extract_nb_games_played(self, season, wherePlayed, matchDay = False):
        if matchDay:
            return 10*matchDay
        else:
            return self._statistics.query(f"season == '{str(season)}'")['FT'+wherePlayed+'G'].shape[0]


    def extract_current_year_games(self, curSeason, matchDay = False):
        curYearGames = self._teamsGames.query(f"season == '{str(curSeason)}'")[['matchDay', 'GS', 'GC', 'WP', 'team']]
        curYearGames = curYearGames.sort_values(by=['matchDay'], ascending=False)
        if matchDay:
            curYearGames = curYearGames.query('matchDay < ' + str(matchDay))
        return curYearGames

    def get_fixture_probs(self, homeTeamGoalExpectancy, awayTeamGoalExpectancy):

        goals = [g for g in range(0, 7)]
        homeTeamGoalProb = []
        awayTeamGoalProb = []
        for goal in goals:
            homeTeamGoalProb.append(self.poisson(mu=homeTeamGoalExpectancy, k=goal))
            awayTeamGoalProb.append(self.poisson(mu=awayTeamGoalExpectancy, k=goal))

        homeTeamGoal = []
        homeTeamProb = []
        maxGoals = 7
        goals = range(0,maxGoals)
        for i in goals:
            for j in goals:
                homeTeamGoal.append(j)
                homeTeamProb.append(homeTeamGoalProb[j])

        awayTeamGoal = []
        awayTeamProb = []
        goals = range(0,maxGoals)
        for i in goals:
            for j in goals:
                awayTeamGoal.append(i)
                awayTeamProb.append(awayTeamGoalProb[i])

        tab = pd.DataFrame([], columns=['HomeTeamGoal', 'AwayTeamGoal']) #, 'AwayTeamGoal', 'AwayTeamProb', 'OutcomeProb'])

        tab['HomeTeamGoal'] = homeTeamGoal
        tab['AwayTeamGoal'] = awayTeamGoal
        tab['TotalGoalScored'] = tab['HomeTeamGoal'] + tab['AwayTeamGoal']
        tab['HomeTeamProb'] = homeTeamProb
        tab['AwayTeamProb'] = awayTeamProb
        tab['OutcomeProb'] = tab['HomeTeamProb'] * tab['AwayTeamProb']
        tab['probs'] = 1/tab['OutcomeProb']
        tab['probs'] = tab['probs']*BOOKING_MARGIN 
        tab.sort_values(by=['OutcomeProb'], ascending=False)

        finalProbabilities = {'Win':None, 'Loose':None, 'Draw':None, 'BothScore':None,
                            'Over 1.5':None, 'Over 2.5':None, 'Over 3.5':None}
                            
        finalProbabilities['Win'] = sum(tab.query("HomeTeamGoal > AwayTeamGoal")["OutcomeProb"])*100
        finalProbabilities['Draw'] = sum(tab.query("HomeTeamGoal == AwayTeamGoal")["OutcomeProb"])*100
        finalProbabilities['Loose'] = sum(tab.query("HomeTeamGoal < AwayTeamGoal")["OutcomeProb"])*100
        finalProbabilities['BothScore'] = sum(tab.query("(HomeTeamGoal > 0) and (AwayTeamGoal > 0)")["OutcomeProb"])*100
        finalProbabilities['Over 1.5'] = sum(tab.query("TotalGoalScored > 1")["OutcomeProb"])*100
        finalProbabilities['Over 2.5'] = sum(tab.query("TotalGoalScored > 2")["OutcomeProb"])*100
        finalProbabilities['Over 3.5'] = sum(tab.query("TotalGoalScored > 3")["OutcomeProb"])*100

        return finalProbabilities

    def get_teams_goal_expectancy(self, homeTeam, awayTeam, curYearGames, pastYearGames, leagueAvgScoredHome, leagueAvgConcededHome, leagueAvgScoredAway, leagueAvgConcededAway, pastSeason):
        curHTScored, curHTConceded, curHTPlayed = self.extract_team_goals_current_season(curYearGames, homeTeam, 'H')
        curATScored, curATConceded, curATPlayed = self.extract_team_goals_current_season(curYearGames, awayTeam, 'A')

        pastHTScored, pastHTConceded, pastHTPlayed = self.extract_team_goals_past_season(pastYearGames, homeTeam, 'H', curHTPlayed, pastSeason)
        pastATScored, pastATConceded, pastATPlayed = self.extract_team_goals_past_season(pastYearGames, awayTeam, 'A', curATPlayed, pastSeason)

        homeTeamGameScored = self.concat_vectors(pastHTScored, curHTScored)
        homeTeamGameConceded = self.concat_vectors(pastHTConceded, curHTConceded)
        awayTeamGameScored = self.concat_vectors(pastATScored, curATScored)
        awayTeamGameConceded = self.concat_vectors(pastATConceded, curATConceded)
        
        weigthVecHomeTeam = self.create_weigth_vector(pastHTPlayed, curHTPlayed, WEIGTH_PAST_SEASON, WEIGTH_CURRENT_SEASON)
        weigthVecAwayTeam = self.create_weigth_vector(pastATPlayed, curATPlayed, WEIGTH_PAST_SEASON, WEIGTH_CURRENT_SEASON)

        avgScoredHomeTeam = self.weigthed_average(homeTeamGameScored, weigthVecHomeTeam)
        avgConcededHomeTeam = self.weigthed_average(homeTeamGameConceded, weigthVecHomeTeam)
        avgScoredAwayTeam = self.weigthed_average(awayTeamGameScored, weigthVecAwayTeam)
        avgConcededAwayTeam = self.weigthed_average(awayTeamGameConceded, weigthVecAwayTeam)
        
        # Home team attack and defense strengths
        homeTeamAttackStrength = avgScoredHomeTeam / leagueAvgScoredHome
        homeTeamDefenseStrength = avgConcededHomeTeam / leagueAvgConcededHome
        # Away team attack and defense strengths
        awayTeamAttackStrength = avgScoredAwayTeam / leagueAvgScoredAway
        awayTeamDefenseStrength = avgConcededAwayTeam / leagueAvgConcededAway

        # Goal expectancy for each team
        # Home team goal expectancy
        homeTeamGoalExpectancy = homeTeamAttackStrength * awayTeamDefenseStrength * leagueAvgScoredHome
        # Away team goal expectancy
        awayTeamGoalExpectancy = awayTeamAttackStrength * homeTeamDefenseStrength * leagueAvgScoredAway

        return homeTeamGoalExpectancy, awayTeamGoalExpectancy, homeTeamAttackStrength, homeTeamDefenseStrength, awayTeamAttackStrength, awayTeamDefenseStrength
        
    def extract_team_goals_current_season(self, curYearGames, team, wherePlayed):
        curTeamGames = curYearGames.query("WP == '"+ wherePlayed + "' and team == '" + team + "'")
        curTeamGames = curTeamGames[['GS', 'GC']]
        return curTeamGames['GS'], curTeamGames['GC'], curTeamGames.shape[0]

    def extract_team_goals_past_season(self, pastYearGames, team, wherePlayed, curGPlayed, pastSeason):
        # Nb of games played and goals scored/conceded by home team
        pastGames = pastYearGames.query("WP == '" + wherePlayed + "' and team == '" + team + "'")
        pastGames = pastGames[['GS', 'GC']]
        if len(pastGames.index)==0:
            # relagated team -> Assumption :
            # This team will have the same past behavior as the relagated team of the previous year
            pastGames = self.extract_goals_relagated_team('H', pastSeason)

        return pastGames['GS'][0:(19-curGPlayed)], pastGames['GC'][0:(19-curGPlayed)], 19-curGPlayed

    def create_weigth_vector(self, pastNbGames, curNbGames, pastWeigthVal, curWeigthVal):
        pastWeigth = [pastWeigthVal for i in range(pastNbGames)]
        curWeigth = [curWeigthVal for i in range(curNbGames)]
        return [*pastWeigth, *curWeigth]

    def concat_vectors(self, pastGoals, curGoals):
        return [*pastGoals.to_list(), *curGoals.to_list()]
    
    def poisson(self, mu, k):
        return (pow(mu,k)*np.exp(-mu))/math.factorial(k)

    def weigthed_average(self, x, w):
        product = 0
        for i in range(len(x)):
            product += x[i]*w[i]
        return product/sum(w)

    def extract_goals_relagated_team(self, wherePlayed, pastSeason):
        data = self._teamsGames.query(f"season == '{str(pastSeason)}'").groupby('team').max("PTS")
        data = data.sort_values(by=['PTS', 'GD', 'GS', 'GC'], ascending=False).reset_index()

        relagatedTeam = data.iloc[19]['team']

        queryYear = (f"season == '{str(pastSeason)}'")
        queryTeams = " and team == '" + relagatedTeam + "'"
        queryWherePlayed = " and WP == " + "'" + wherePlayed +"'" # or away
        fullQuery = queryYear + queryTeams + queryWherePlayed
        relagatedTeamsGames = self._teamsGames.query(fullQuery)[['WP', 'matchDay', 'GS', 'GC', 'team']]
        relagatedTeamsGames = relagatedTeamsGames.sort_values(by=['matchDay'], ascending=False)[['GS', 'GC']]
        return relagatedTeamsGames
