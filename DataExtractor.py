import pandas as pd
import numpy as np
from datetime import datetime

featuresAbbrev = {'Points': 'PTS',
                  'Goal Scored': 'GSCUM',
                  'Goal Conceded': 'GCCUM',
                  'Goal difference': 'GD',
                  'Form': 'FORM',
                  'Average goals scored the 4 last game': ['HPKGS', 'APKGS'],
                  'Average goals conceded the 4 last game': ['HPKGC', 'APKGC'],
                  'Average shoots on target the 4 last game': ['HPKST',
                                                               'APKST'],
                  'Average corners obtained the 4 last game': ['HPKC', 'APKC'],
                  'Average goals scored': 'GSCUM',
                  'Average goals scored home': 'avgGoalSHH',
                  'Average goals scored away': 'avgGoalSAA',
                  'Average goals conceded': 'GCCUM',
                  'Average goals conceded home': 'avgGoalCHH',
                  'Average goals conceded away': 'avgGoalCAA'}

featuresDefinition = {'Form': 'The Form gives a value for the current team ' +
                      'strength. The Form of a team is updated after each ' +
                      'game and its new value depends on the form of its ' +
                      'opponent. More points are given  to weak teams ' +
                      'that beat strong teams.'}


class DataExtractor:
    def __init__(self, statistics, teamsGames, fixtures, probabilities):
        self._statistics = statistics
        self._teamsGames = teamsGames
        self._fixtures = fixtures
        self._featuresAbbrev = featuresAbbrev
        self._featuresDefinition = featuresDefinition
        self._probabilities = probabilities

    def get_seasons(self, firstSeason, lastSeason):
        return [str(year) + '/'+str(year+1) for year in range(firstSeason, lastSeason)][::-1]  # reversed # noqa: E501

    def get_team_stats(self):
        return ['Average goals scored home', 'Average goals scored away',
                'Average goals conceded home', 'Average goals conceded away']

    def get_features_abbrev(self, userInput):
        return self._featuresAbbrev[userInput]

    def get_teams_season(self, season):
        # team selection in function of the year
        year = int(season.split('/')[1])
        teams = self._teamsGames \
            .query(f"season == '{year}' and matchDay == 1")['team'] \
            .to_list()
        teams.append('All')
        teams.sort()
        return teams, year

    def get_actual_match_day(self, year):
        data = self._teamsGames \
            .query(f"season == '{year}'") \
            .sort_values(by=['matchDay'], ascending=False)
        lastMatchDay = int(data['matchDay'].iloc[0])
        if lastMatchDay == 38:
            return lastMatchDay
        else:
            return lastMatchDay + 1

    def extract_fixtures(self, nbGames=10):
        nextFixtures = self._fixtures.query(f"played == {False}")[['date',
                                                                   'homeTeam',
                                                                   'awayTeam']]
        # Ensure the 'date' column is in datetime format
        nextFixtures['date'] = pd.to_datetime(nextFixtures['date'])

        # Filter for dates after the current date
        current_date = datetime.now()
        nextFixtures = nextFixtures[nextFixtures['date'] > current_date]

        nextFixtures = nextFixtures.iloc[0:nbGames]
        dates = nextFixtures['date'].to_list()
        homeTeams = nextFixtures['homeTeam'].to_list()
        awayTeams = nextFixtures['awayTeam'].to_list()

        datesFixtures = [f"{dates[i]} : {homeTeams[i]}  vs {awayTeams[i]}" for i in range(nbGames)]

        return datesFixtures, homeTeams, awayTeams

    # =====================================================
    # LEAGUE
    # =====================================================

    def get_data_ligue_plot(self, seasonChoice, statChoice, getDict=False):
        if 'home' in statChoice.split() or 'away' in statChoice.split():
            return self.get_data_teams_performance_away_or_home(
                seasonChoice, statChoice, getDict=getDict)
        else:
            return self.get_data_teams_performance(
                seasonChoice, statChoice, getDict=getDict)

    def get_data_teams_performance(self, seasonChoice, statChoice, getDict=False):
        stat = self.get_features_abbrev(statChoice)
        teams, year = self.get_teams_season(seasonChoice)
        data = self._teamsGames \
            .query(f"season == '{year}'")[['gameNumber', 'team', stat]] \
            .sort_values(by=['gameNumber'])
        teamStats = {}
        teams.remove('All')
        for team in teams:
            nbGames = data.query(f"team == '{team}'").shape[0]
            teamStats[team] = data.query(f"team == '{team}'").iloc[-1][stat]/nbGames

        if getDict:
            return teamStats
        else:
            x = list(dict(sorted(teamStats.items(), key=lambda item: item[1], reverse=True)).keys())
            y = list(dict(sorted(teamStats.items(), key=lambda item: item[1], reverse=True)).values())

            yAvg = np.mean(y)
            yLeague = [yAvg for i in range(len(x))]

            return x, y, yLeague

    def get_data_teams_performance_away_or_home(self, seasonChoice, statChoice, getDict=False):
        teams, year = self.get_teams_season(seasonChoice)
        teams.remove('All')
        if 'home' in statChoice.split():
            place = True
        else:
            place = False
        stat = self.get_features_abbrev(statChoice)
        teamStats = {}
        for team in teams:
            data = self._statistics.query(f"season == '{year}'")[['gameNumber', 'homeTeam', 'awayTeam', stat]]
            if place:
                teamStats[team] = data.query(f"homeTeam == '{team}'") \
                    .sort_values(['gameNumber']).iloc[-1][stat]
            else:
                teamStats[team] = data.query(f"awayTeam == '{team}'") \
                    .sort_values(['gameNumber']).iloc[-1][stat]

        if getDict:
            return teamStats
        else:
            x = list(dict(sorted(teamStats.items(), key=lambda item: item[1], reverse=True)).keys())
            y = list(dict(sorted(teamStats.items(), key=lambda item: item[1], reverse=True)).values())

            yAvg = np.mean(y)
            yLeague = [yAvg for i in range(len(x))]

            return x, y, yLeague

    # =====================================================
    # TEAMS
    # =====================================================

    def get_data_stats_evolution_teams(self, year, stat, teamsChoice, allTeams):
        if 'All' in teamsChoice:
            teamsChoice = allTeams

        data = self._teamsGames \
            .query(f"season == '{year}' and team == {teamsChoice}")
        data = data.sort_values(by=['matchDay'], ascending=False)
        statAbbrev = self.get_features_abbrev(stat)

        return data, statAbbrev

    def get_ranking(self, data, teamName):
        data = data.groupby('team').max("PTS")
        data = data \
            .sort_values(by=['PTS', 'GD', 'GS', 'GC'], ascending=False) \
            .reset_index()
        data['ranking'] = data.index + 1
        return int(data.query(f"team == '{teamName}'")['ranking'].iloc[0])

    def get_team_data(self, year, teamName):
        teamData = {'Ranking': None, 'PointsLeague': None, 'Target': None,
                    'Scored': None, 'Conceded': None, 'Difference': None,
                    'MatchDay': None, 'Form': None, 'Outcomes': None,
                    'Corner': None}

        data = self._teamsGames.query(f"season == '{year}'")
        teamData['Ranking'] = self.get_ranking(data, teamName)

        data = data.query(f"team == '{teamName}'")
        data = data.sort_values(by=['matchDay'], ascending=False)

        teamData['PointsLeague'] = data['PTS'].iloc[0]
        teamData['MatchDay'] = data['matchDay']
        teamData['Points'] = data['PTS']
        teamData['Scored'] = data['GSCUM']
        teamData['Conceded'] = data['GCCUM']
        teamData['Difference'] = data['GD']
        teamData['Form'] = data['FORM']
        teamData['Target'] = data['ST']
        teamData['Corner'] = data['C']
        teamData['res'] = data['RES']
        teamData['gs'] = data['GS']
        teamData['gc'] = data['GC']

        outcomes = data['RES'].value_counts().to_dict()
        win = outcomes.get(3, 0)
        draw = outcomes.get(1, 0)
        loose = outcomes.get(0, 0)
        teamData['Outcomes'] = [loose, draw, win]

        return teamData

    # =====================================================
    # FIXTURES
    # =====================================================
    def process_team_data_fixture(self, teamName, year, season):
        output = {}

        data = self.get_team_data(year, teamName)

        output['name'] = teamName
        output['points'] = int(data['PointsLeague'])
        output['ranking'] = int(data['Ranking'])
        output['outcomes'] = data['Outcomes']
        output['scored'] = int(data['Scored'].iloc[0])
        output['conceded'] = int(data['Conceded'].iloc[0])
        output['difference'] = output['scored'] - output['conceded']
        output['form'] = data['Form'].iloc[0].round(decimals=3)

        recentOutcomes = data['res'].iloc[0:4].value_counts().to_dict()
        win = recentOutcomes.get(3, 0)
        draw = recentOutcomes.get(1, 0)
        loose = recentOutcomes.get(0, 0)
        output['recent_outcomes'] = [win, draw, loose]

        output['recent_scored'] = int(sum(data['gs'].iloc[0:4]))
        output['recent_conceded'] = int(sum(data['gc'].iloc[0:4]))
        output['avgs'] = np.mean(data['gs'])
        output['avgc'] = np.mean(data['gc'])

        avgGSHDict = self.get_data_ligue_plot(
            season, 'Average goals scored home', getDict=True)[teamName]
        avgGSADict = self.get_data_ligue_plot(
            season, 'Average goals scored away', getDict=True)[teamName]
        avgGCHDict = self.get_data_ligue_plot(
            season, 'Average goals conceded home', getDict=True)[teamName]
        avgGCADict = self.get_data_ligue_plot(
            season, 'Average goals conceded away', getDict=True)[teamName]

        output['avg_scored_home'] = avgGSHDict.round(decimals=3)
        output['avg_scored_away'] = avgGSADict.round(decimals=3)
        output['avg_conceded_home'] = avgGCHDict.round(decimals=3)
        output['avg_conceded_away'] = avgGCADict.round(decimals=3)

        return output

    def process_fixtures(self, homeTeam, awayTeam, curYear, nbGames):
        nbGames = 6

        queryHomeTeam = "(homeTeam == " + "'"+ homeTeam + "'" + " or homeTeam ==  '" + awayTeam + "')"
        queryAwayTeam = " and (awayTeam == " + "'"+ homeTeam + "'" + " or awayTeam ==  '" + awayTeam + "')"
        queryText = queryHomeTeam + queryAwayTeam
        df = self._statistics.query(queryText).sort_index(axis=0, ascending=True)[['homeTeam', 'awayTeam', 'FTHG', 'FTAG', 'date']]
        df = df.sort_values(by=['date'], ascending=False)
        df = df[0:nbGames]

        fthg = df['FTHG'].to_list()
        ftag = df['FTAG'].to_list()

        results = {homeTeam : [], awayTeam : []}

        for i in range(df.shape[0]):
            if df.iloc[i]['homeTeam'] == homeTeam:
                results[homeTeam].append(int(df.iloc[i]['FTHG']))
                results[awayTeam].append(int(df.iloc[i]['FTAG']))
            else:
                results[homeTeam].append(int(df.iloc[i]['FTAG']))
                results[awayTeam].append(int(df.iloc[i]['FTHG']))

        resultScores = [str(int(fthg[i]))+' - '+str(int(ftag[i])) for i in range(df.shape[0])]
        dates = df['date'].to_list()

        win = 0
        draw = 0
        loose = 0
        for i in range(len(resultScores)):
            if results[homeTeam][i] > results[awayTeam][i]:
                win += 1
            elif results[homeTeam][i] < results[awayTeam][i]:
                loose += 1
            else:
                draw += 1

        homeTeamStats = [-np.mean(results[awayTeam]).round(decimals=3), -np.mean(results[homeTeam]).round(decimals=3), -loose, -draw, -win]
        awayTeamStats = [np.mean(results[homeTeam]).round(decimals=3), np.mean(results[awayTeam]).round(decimals=3), win, draw, loose] 

        return results, resultScores, dates, df['homeTeam'].to_list(), df['awayTeam'].to_list(), homeTeamStats, awayTeamStats

    def extract_home_away_team_data(self, homeTeamName, awayTeamName, season, nbGames):
        year = int(season.split('/')[1])

        homeTeamData = self.process_team_data_fixture(homeTeamName,
                                                      year, season)

        awayTeamData = self.process_team_data_fixture(awayTeamName,
                                                      year, season)

        scored, scores, dates, homeTeams, awayTeams, homeTeamStats, awayTeamStats = self.process_fixtures(homeTeamName, awayTeamName, year, nbGames)

        return [homeTeamData, awayTeamData, scored, scores,
                homeTeams, awayTeams, homeTeamStats, awayTeamStats, dates]

    def extract_teams_strength_prob(self, homeTeam, awayTeam):

        data = self._probabilities \
            .query(f"homeTeam == '{homeTeam}' and awayTeam == '{awayTeam}'")

        hTAttStrength = round(data['hTAttStrength'].iloc[0], 2)
        hTDefStrength = round(data['hTDefStrength'].iloc[0], 2)
        aTAttStrength = round(data['aTAttStrength'].iloc[0], 2)
        aTDefStrength = round(data['aTDefStrength'].iloc[0], 2)

        win = round(data['win'].iloc[0], 2)
        loose = round(data['loose'].iloc[0], 2)
        draw = round(data['draw'].iloc[0], 2)
        both = round(data['bothScore'].iloc[0], 2)
        more25 = round(data['>2.5'].iloc[0], 2)
        more35 = round(data['>3.5'].iloc[0], 2)

        return hTAttStrength, hTDefStrength, aTAttStrength, aTDefStrength, win, loose, draw, both, more25, more35

    # =====================================================
    # BETTINGS
    # =====================================================

    def get_league_statistics(self, season):
        year = int(season.split('/')[1])

        leagueStats = {}
        curSeasonGames = self._statistics.query(f"season == '{year}'").copy()
        nbGames = curSeasonGames.shape[0]

        leagueStats['NbGames'] = round(nbGames, 2)

        homeWins = curSeasonGames[curSeasonGames['FTHG'] > curSeasonGames['FTAG']].shape[0]/nbGames*100

        leagueStats['HomeWins'] = round(homeWins, 2)

        awayWins = curSeasonGames[curSeasonGames['FTHG'] < curSeasonGames['FTAG']].shape[0]/nbGames*100

        leagueStats['AwayWins'] = round(awayWins, 2)

        draws = curSeasonGames[curSeasonGames['FTHG'] == curSeasonGames['FTAG']].shape[0]/nbGames*100

        leagueStats['Draws'] = round(draws, 2)

        leagueStats['AvgLeagueGoals'] = round(
            (curSeasonGames['FTHG'] + curSeasonGames['FTAG']).sum()/nbGames, 2)

        leagueStats['HomeGoals'] = round(
            (curSeasonGames['FTHG']).sum() / nbGames, 2)

        leagueStats['AwayGoals'] = round(
            (curSeasonGames['FTAG']).sum() / nbGames, 2)

        return leagueStats

    def get_teams_percentage_nb_goals(self, season):
        year = int(season.split('/')[1])
        matchDay = self.get_actual_match_day(year)

        games = self._teamsGames.query(f"season == '{year}'").copy()
        games['goals'] = games['GS'] + games['GC']
        games.loc[:, '>1.5'] = games['goals'] > 1.5
        games['>2.5'] = games['goals'] > 2.5
        games['>3.5'] = games['goals'] > 3.5
        games = games[['team', '>1.5', '>2.5', '>3.5']]

        df = pd.DataFrame()

        df['>1.5'] = (
            games.groupby('team')['>1.5']
            .apply(lambda x: x[x].count()) / matchDay * 100
            ).round(decimals=1)

        df['>2.5'] = (
            games.groupby('team')['>2.5']
            .apply(lambda x: x[x].count()) / matchDay*100
            ).round(decimals=1)

        df['>3.5'] = (
            games.groupby('team')['>3.5']
            .apply(lambda x: x[x].count()) / matchDay*100
            ).round(decimals=1)

        return df.reset_index()
