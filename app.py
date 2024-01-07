from DataExtractor import DataExtractor
from Visualizer import Visualizer
from Visualizer import *
import streamlit as st
import pandas as pd
import logging
from PIL import Image
from azure.storage.blob import ContainerClient
from io import BytesIO

st.set_page_config(layout="wide")

# size of the white space above the title
reduce_header_height_style = """
    <style>
        div.block-container {padding-top:0.4rem;}
    </style>
"""
st.markdown(reduce_header_height_style, unsafe_allow_html=True)

# size of the side bar

st.markdown(
    """
    <style>
    [data-testid="stSidebar"][aria-expanded="true"] > div:first-child {
        width: 250px;
    }
    [data-testid="stSidebar"][aria-expanded="false"] > div:first-child {
        width: 250px;
        margin-left: -250px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

EPL_LOGO = Image.open('pictures/logo.png')
WIDTH_LOGO = 150

LEAGUE = 'epl'
STATISTICS = 'game_statistics'
TEAM_GAMES = 'team_games'
FIXTURES = 'fixtues'
WEEK_PROBABILITIES = 'poisson_probabilities'

CURSEASON = '2023/2024'
FIRST_SEASON = 2005
LAST_SEASON = 2024


@st.cache_data
def initialize_classes(statistics, team_games, fixtures, week_probabilities,
                       FIRST_SEASON, LAST_SEASON):

    dataExtractor = DataExtractor(
        statistics, team_games, fixtures, week_probabilities)
    visualizer = Visualizer(
        statistics, team_games, FIRST_SEASON, LAST_SEASON)

    return dataExtractor, visualizer


# Extract the number of goals scored in a game
# for each team for >1.5, >2.5, >3.5
@st.cache_data
def extract_teams_percentages_goal_scoring(team_games, season):
    year = int(season.split('/')[1])
    data = team_games \
        .query(f"season == '{year}'") \
        .sort_values(by=['matchDay'], ascending=False)

    matchDay = int(data['matchDay'].iloc[0])
    if matchDay != 38:
        matchDay = matchDay + 1

    games = team_games.query(f"season == '{year}'").copy()
    games['goals'] = games['GS'] + games['GC']
    games.loc[:, '>1.5'] = games['goals'] > 1.5
    games['>2.5'] = games['goals'] > 2.5
    games['>3.5'] = games['goals'] > 3.5
    games['bothScore'] = (games['GS'] > 0) & (games['GC'] > 0)
    games = games[['team', 'bothScore', '>1.5', '>2.5', '>3.5']]

    df = pd.DataFrame()

    df['>1.5'] = (
        games.groupby('team')['>1.5']
        .apply(lambda x: x[x].count()) / matchDay * 100
    ).round(decimals=1)

    df['>2.5'] = (
        games.groupby('team')['>2.5']
        .apply(lambda x: x[x].count())/matchDay * 100
        ).round(decimals=1)

    df['>3.5'] = (
        games.groupby('team')['>3.5']
        .apply(lambda x: x[x].count())/matchDay * 100
        ).round(decimals=1)

    df['bothScore'] = (
        games.groupby('team')['bothScore']
        .apply(lambda x: x[x].count())/matchDay * 100
        ).round(decimals=1)

    return df.reset_index()


@st.cache_data
def extract_week_suggestions(col):
    return week_probabilities \
        .sort_values(by=col, ascending=False)\
        .iloc[0:3][["homeTeam", "awayTeam", col]]\
        .round(2)


def download_parquet(container_client, blob_name):
    blob_client = container_client.get_blob_client(blob=blob_name)
    download_stream = blob_client.download_blob()
    stream = BytesIO()
    download_stream.readinto(stream)
    df = pd.read_parquet(stream, engine='pyarrow')
    return df


@st.cache_data
def extract_dataset():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    con_str = st.secrets["CONNECTION_STRING"]

    #  Container and folder name
    container_name = 'gold'
    folder_name = f'/{LEAGUE}'

    #  Create a blob client
    container_client = ContainerClient.from_connection_string(
        conn_str=con_str, container_name=container_name
    )

    # Datasets
    datasets = {
        'epl/team_games': [],
        'epl/fixtures': [],
        'epl/game_statistics': [],
        'epl/poisson_probabilities': []
    }

    # List and read parquet files
    blob_list = container_client.list_blobs(name_starts_with=folder_name)

    for blob in blob_list:
        for dataset in datasets:
            if (blob.name.startswith(dataset) and blob.name.endswith('.parquet')):
                try:
                    df = download_parquet(container_client, blob.name)
                    datasets[dataset].append(df)
                except Exception as e:
                    logger.error(f'Error downloading {blob.name}: {e}')

    # Concatenate all dataframes
    for dataset in datasets:
        datasets[dataset] = pd.concat(datasets[dataset], ignore_index=True)

    return datasets


# get the start time
team_games, fixtures, statistics, week_probabilities = extract_dataset().values()

dataExtractor, visualizer = initialize_classes(statistics, team_games,
                                               fixtures, week_probabilities,
                                               FIRST_SEASON, LAST_SEASON)

dataExtractor = DataExtractor(
    statistics, team_games, fixtures, week_probabilities)
visualizer = Visualizer(
    statistics, team_games, FIRST_SEASON, LAST_SEASON)

page = st.sidebar.selectbox(' ', ['Project presentation',
                                  'Betting suggestions',
                                  'Next fixtures',
                                  'League overview',
                                  'Teams overview'])

if page == 'Project presentation':
    # two columns -> first is two times larger than the second one
    pageTitle, eplLogo = st.columns([2, 1])

    with pageTitle:
        st.title("A web application providing dashboards on " +
                 "different statistics on the " +
                 "English Premier League.")

    with eplLogo:
        image = EPL_LOGO
        st.image(image, width=WIDTH_LOGO, use_column_width=True)

    st.markdown("***")

    txt1 = "#### This personal project aims to link two of my passions which "
    txt2 = "are the art of handling data and the beautiful game of football."
    txt3 = "#### I invite you to visit this web application that aims "
    txt4 = "to support bettors in their quest for profit! Or simply for "
    txt5 = "any lover of football statistics !"

    st.markdown(txt1 + txt2)
    st.markdown(txt3 + txt4 + txt5)
    st.markdown("***")

    st.header("Author")

    st.markdown("Please feel free to contact me with any issues, comments, or questions.")  # noqa: E501

    st.subheader("Guy Girineza")

    st.markdown("- Email : guy.girineza@hotmail.com or girineza.guy@gmail.com")
    st.markdown("- Linkdin : https://www.linkedin.com/in/guy-girineza/")

    st.markdown("***")

    st.markdown("Developed and Maintained by Guy Girineza")
    st.markdown("Most Recently Deposited Entry 2022-")
    st.markdown("Copyright (c) 2022 Guy Girineza")

elif page == 'League overview':

    pageTitle, eplLogo = st.columns([2, 1])

    with pageTitle:
        st.title("League")

    with eplLogo:
        image = EPL_LOGO
        st.image(image, width=WIDTH_LOGO, use_column_width=False)

    seasonChoice = st.selectbox("Seasons", visualizer._seasons)

    ligueScored, ligueConceded = st.columns(2)

    with ligueScored:
        teamsName, avgGS, leagueAvgGS = dataExtractor.get_data_ligue_plot(
            seasonChoice, 'Average goals scored')
        visualizer.create_teams_plot(
            teamsName, avgGS, leagueAvgGS, seasonChoice,
            'Average goals scored', fontSize=12)

    with ligueConceded:
        teamsName, avgGC, leagueAvgGC = dataExtractor.get_data_ligue_plot(
            seasonChoice, 'Average goals conceded')
        visualizer.create_teams_plot(teamsName, avgGC, leagueAvgGC,
                                     seasonChoice, 'Average goals conceded',
                                     fontSize=12)

    # scored home/away
    avgGSHDict = dataExtractor.get_data_ligue_plot(
        seasonChoice, 'Average goals scored home', getDict=True)
    avgGSADict = dataExtractor.get_data_ligue_plot(
        seasonChoice, 'Average goals scored away', getDict=True)

    teamsName = list(dict(sorted(avgGSHDict.items(),
                                 key=lambda item: item[1],
                                 reverse=True)).keys())
    avgGSH = list(dict(sorted(avgGSHDict.items(),
                              key=lambda item: item[1],
                              reverse=True)).values())
    avgGSA = [avgGSADict[team] for team in teamsName]
    visualizer.conceded_scored_home_away_plot(teamsName, avgGSH, avgGSA,
                                              fontSize=12)

    # conceded home/away
    avgGCHDict = dataExtractor.get_data_ligue_plot(
        seasonChoice, 'Average goals conceded home', getDict=True)
    avgGCADict = dataExtractor.get_data_ligue_plot(
        seasonChoice, 'Average goals conceded away', getDict=True)

    teamsName = list(dict(sorted(avgGCHDict.items(),
                                 key=lambda item: item[1],
                                 reverse=True)).keys())
    avgGCH = list(dict(sorted(avgGCHDict.items(),
                              key=lambda item: item[1],
                              reverse=True)).values())
    avgGCA = [avgGCADict[team] for team in teamsName]
    visualizer.conceded_scored_home_away_plot(teamsName, avgGCH, avgGCA,
                                              scored=False, fontSize=12)


elif page == 'Teams overview':
    pageTitle, eplLogo = st.columns([2, 1])

    with pageTitle:
        st.title("Teams")

    with eplLogo:
        image = EPL_LOGO
        st.image(image, width=WIDTH_LOGO, use_column_width=False)

    tab1, tab2 = st.tabs(["Team season", "Compare teams"])

    with tab1:
        season, team = st.columns(2)
        with season:
            seasonChoice = st.selectbox("Seasons", visualizer._seasons, key=1)

        with team:
            teams, year = visualizer.get_teams_season(seasonChoice)
            teams.remove('All')
            teamsChoice = st.selectbox("Teams", teams)

        teamData = dataExtractor.get_team_data(year, teamsChoice)

        matchDay = teamData['MatchDay']

        ranking, points, results = st.columns(3)
        height = 200
        fontSize = 12
        with ranking:
            visualizer.ranking_points(teamsChoice, teamData['Ranking'],
                                      height=170, fontSize=fontSize)
        with points:
            visualizer.ranking_points(teamsChoice,
                                      points=teamData['PointsLeague'],
                                      height=170, fontSize=fontSize)
        with results:
            visualizer.outcomes(teamsChoice, teamData['Outcomes'],
                                height=170, fontSize=fontSize)

        pointsEvolution, goals = st.columns(2)
        with pointsEvolution:
            visualizer.team_stat_evolution(matchDay, teamData['Corner'],
                                           'Corners', height=height,
                                           fontSize=fontSize)
        with goals:
            scored = teamData['Scored']
            conceded = teamData['Conceded']
            difference = teamData['Difference']
            visualizer.team_stat_evolution(
                matchDay, [scored, conceded, difference],
                'Goals scored | conceded | difference', goals=True,
                height=height, fontSize=fontSize)

        form, target = st.columns(2)
        height = 200
        with form:
            visualizer.team_stat_evolution(matchDay, teamData['Form'],
                                           'Form', height=height)
        with target:
            visualizer.team_stat_evolution(matchDay, teamData['Target'],
                                           'Shots on target', height=height,
                                           fontSize=fontSize)

    with tab2:
        season, stat = st.columns(2)
        with season:
            seasonChoice = st.selectbox("Seasons", visualizer._seasons, key=2)

        with stat:
            statChoice = st.selectbox("Statistics", visualizer._leagueStats)

        teams, year = visualizer.get_teams_season(seasonChoice)
        teamsChoice = st.multiselect("Teams", teams)
        data, statAbbrev = dataExtractor.get_data_stats_evolution_teams(
            year, statChoice, teamsChoice, teams)
        visualizer.stat_evolution_teams(
            teamsChoice, data, statChoice, statAbbrev,
            seasonChoice, teams, 600, fontSize)

elif page == 'Next fixtures':

    pageTitle, eplLogo = st.columns([2, 1])

    with pageTitle:
        st.title("Fixtures")

    with eplLogo:
        image = EPL_LOGO
        st.image(image, width=WIDTH_LOGO, use_column_width=False)

    nextFixtures, homeTeams, awayTeams = dataExtractor.extract_fixtures()
    fixtureChoice = st.selectbox('Fixtures', nextFixtures)
    index = nextFixtures.index(fixtureChoice)
    homeTeamName = homeTeams[index]
    awayTeamName = awayTeams[index]

    fixtureAnalysis = dataExtractor.extract_home_away_team_data(homeTeamName,
                                                                awayTeamName,
                                                                CURSEASON, 6)
    team1 = fixtureAnalysis[0]
    team2 = fixtureAnalysis[1]
    lastGamesResult = fixtureAnalysis[2]
    lastGamesScores = fixtureAnalysis[3]
    homeTeamPlace = fixtureAnalysis[4]
    awayTeamPlace = fixtureAnalysis[5]
    homeTeamStats = fixtureAnalysis[6]
    awayTeamStats = fixtureAnalysis[7]
    datesPastFixtures = fixtureAnalysis[8]

    hTAS, hTDS, aTAS, aTDS, win, loose, draw, both, more25, more35 = \
        dataExtractor.extract_teams_strength_prob(homeTeamName, awayTeamName)

    matchUp, last4games, last6games = st.tabs(["Match up",
                                               "Last 4 games",
                                               "Last 6 matchups"])

    with matchUp:
        col1, col2, col3, col4, col5, col6, col7, col8 = st.columns([1, 1, 1, 1, 1, 1, 1, 1])  # noqa: E501
        fontSize = 10
        height = 130
        with col1:
            visualizer.epl_season_stats(team1["name"]+"<br><b>Points",
                                        team1["points"], height=height,
                                        fontSize=fontSize,
                                        color=HOME_TEAM_POINTS)
        with col2:
            visualizer.epl_season_stats(team1["name"]+"<br><b>Ranking",
                                        team1["ranking"], height=height,
                                        fontSize=fontSize,
                                        color=HOME_TEAM_RANKING)
        with col3:
            visualizer.epl_season_stats(team1["name"]+"<br><b>Attack strength",
                                        hTAS, height=height, fontSize=fontSize,
                                        color=HOME_TEAM_POINTS)
        with col4:
            visualizer.epl_season_stats(team1["name"]+"<br><b>Defense strength",  # noqa: E501
                                        hTDS, height=height, fontSize=fontSize,
                                        color=HOME_TEAM_POINTS)
        with col5:
            visualizer.epl_season_stats(team2["name"]+"<br><b>Points",
                                        team2["points"], height=height,
                                        fontSize=fontSize,
                                        color=HOME_TEAM_POINTS)
        with col6:
            visualizer.epl_season_stats(team2["name"]+"<br><b>Ranking",
                                        team2["ranking"], height=height,
                                        fontSize=fontSize,
                                        color=HOME_TEAM_POINTS)
        with col7:
            visualizer.epl_season_stats(team2["name"]+"<br><b>Attack strength",
                                        aTAS, height=height, fontSize=fontSize,
                                        color=HOME_TEAM_POINTS)
        with col8:
            visualizer.epl_season_stats(team2["name"]+"<br><b>Defense strength",  # noqa: E501
                                        aTDS, height=height, fontSize=fontSize,
                                        color=HOME_TEAM_POINTS)

        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            fontSize = 12
            visualizer.create_plot_teams_games_outcomes(team1, team2,
                                                        230, fontSize)
            visualizer.create_plot_teams_goals(team1, team2,
                                               230, fontSize)
        with col2:
            fontSize = 12
            height = 145
            visualizer.odd_circle(team1["name"]+" - Win", win, 120,
                                  height, fontSize)
            visualizer.odd_circle(team2["name"]+" - Win", loose, 120,
                                  height, fontSize)
            visualizer.odd_circle("Draw", draw, 120,
                                  height, fontSize)
        with col3:
            fontSize = 12
            height = 145
            visualizer.odd_circle("Both teams score", both, 120,
                                  height, fontSize)
            visualizer.odd_circle("More than 2.5 goals", more25, 120,
                                  height, fontSize)
            visualizer.odd_circle("More than 3.5 goals", more35, 120,
                                  height, fontSize)

    with last4games:
        homeOutcomesTab, awayOutcomesTab = st.columns(2)
        height = 170
        fontSize = 12
        with homeOutcomesTab:
            visualizer.recent_outcomes(team1['name'], team1['recent_outcomes'],
                                       height, fontSize)
        with awayOutcomesTab:
            visualizer.recent_outcomes(team2['name'], team2['recent_outcomes'],
                                       height, fontSize)

        formTab, goalsTab = st.columns([1, 2])

        teamsName = [team1['name'], team2['name']]
        height = 190
        with formTab:
            forms = [team1['form'], team2['form']]
            visualizer.recent_form(teamsName, forms, height, fontSize)
        with goalsTab:
            homeTeam = [team1['recent_conceded'], team1['recent_scored']]
            awayTeam = [team2['recent_conceded'], team2['recent_scored']]
            visualizer.recent_goals_scored_conceded(teamsName, homeTeam,
                                                    awayTeam, height, fontSize)

        avgScored, avgConceded = st.columns(2)
        height = 170
        fontSize = 12
        with avgScored:
            avgHometeam = [team1['avg_scored_home'],
                           team1['avg_conceded_home']]
            visualizer.average_goals(team1['name'], avgHometeam,
                                     height=height, fontSize=fontSize)
            expander = st.expander("See 'Form' explanation")
            expander.write(visualizer._featuresDefinition['Form'])

        with avgConceded:
            avgAwayTeam = [team2['avg_scored_away'],
                           team2['avg_conceded_away']]
            visualizer.average_goals(team2['name'], avgAwayTeam, home=False,
                                     height=height, fontSize=fontSize)

    with last6games:
        avgPointsHomeTeam, avgPointsAwayTeam = st.columns(2)
        with avgPointsHomeTeam:
            teamsName = [team1['name'], team2['name']]
            avgNbPoints = (-(homeTeamStats[4]*3) - (homeTeamStats[3]*1))/6
            visualizer.avg_points_against(teamsName, avgNbPoints,
                                          170, fontSize=fontSize)
        with avgPointsAwayTeam:
            avgNbPoints = ((awayTeamStats[4]*3) + (awayTeamStats[3]*1))/6
            visualizer.avg_points_against([team2['name'], team1['name']],
                                          avgNbPoints, 170, fontSize=fontSize,
                                          away=True)

        visualizer.outcomes_matchup(teamsName, homeTeamStats,
                                    awayTeamStats, 250,
                                    fontSize=fontSize)

        visualizer.last_6_games_result(teamsName, lastGamesScores,
                                       homeTeamPlace, awayTeamPlace,
                                       datesPastFixtures, 250,
                                       fontSize=15)


else:
    pageTitle, eplLogo = st.columns([2, 1])

    with pageTitle:
        st.title("Betting")

    with eplLogo:
        image = EPL_LOGO
        st.image(image, width=WIDTH_LOGO, use_column_width=False)

    goalScoredPerTeams = extract_teams_percentages_goal_scoring(team_games,
                                                                CURSEASON)

    nbGPlayed, homeWin, awayWin, draw, goalPerGame, homeGoalPerGame, awayGoalPerGame = st.columns(7)  # noqa: E501

    fontSize = 10
    height = 130
    width = 550
    leagueStats = dataExtractor.get_league_statistics(CURSEASON)

    with nbGPlayed:
        visualizer.epl_season_stats("Number of<br>games played",
                                    leagueStats['NbGames'],
                                    height=height,
                                    fontSize=fontSize,
                                    color=HOME_TEAM_POINTS)
    with homeWin:
        visualizer.epl_season_stats("Home wins<br>percentage",
                                    leagueStats['HomeWins'],
                                    height=height,
                                    fontSize=fontSize,
                                    color=HOME_TEAM_POINTS)
    with awayWin:
        visualizer.epl_season_stats("Away wins<br>percentage",
                                    leagueStats['AwayWins'],
                                    height=height,
                                    fontSize=fontSize,
                                    color=HOME_TEAM_POINTS)
    with draw:
        visualizer.epl_season_stats("Draws<br>percentage",
                                    leagueStats['Draws'],
                                    height=height,
                                    fontSize=fontSize,
                                    color=HOME_TEAM_POINTS)
    with goalPerGame:
        visualizer.epl_season_stats("Goals<br>per game",
                                    leagueStats['AvgLeagueGoals'],
                                    height=height,
                                    fontSize=fontSize,
                                    color=HOME_TEAM_POINTS)
    with homeGoalPerGame:
        visualizer.epl_season_stats("Home goals<br>per game",
                                    leagueStats['HomeGoals'],
                                    height=height,
                                    fontSize=fontSize,
                                    color=HOME_TEAM_POINTS)
    with awayGoalPerGame:
        visualizer.epl_season_stats("Away goals<br>per game",
                                    leagueStats['AwayGoals'],
                                    height=height,
                                    fontSize=fontSize,
                                    color=HOME_TEAM_POINTS)

    col1, col2, col3 = st.columns([1, 1, 2])
    # average for the league
    fontSize = 33
    widthBarPlot = 300
    heightBarPlot = 350
    with col1:
        visualizer.plot_teams_percentages_goal_scoring(goalScoredPerTeams,
                                                       'bothScore',
                                                       "Both teams score",
                                                       widthBarPlot,
                                                       heightBarPlot,
                                                       fontSize)
        visualizer.plot_teams_percentages_goal_scoring(goalScoredPerTeams,
                                                       '>3.5',
                                                       "More than 3.5 goals",
                                                       widthBarPlot,
                                                       heightBarPlot,
                                                       fontSize)
    with col2:
        visualizer.plot_teams_percentages_goal_scoring(goalScoredPerTeams,
                                                       '>2.5',
                                                       "More than 2.5 goals",
                                                       widthBarPlot,
                                                       heightBarPlot,
                                                       fontSize)
        visualizer.plot_teams_percentages_goal_scoring(goalScoredPerTeams,
                                                       '>1.5',
                                                       "More than 1.5 goals",
                                                       widthBarPlot,
                                                       heightBarPlot,
                                                       fontSize)
    with col3:
        df = extract_week_suggestions('win')
        visualizer.plot_advised_bets(
            df['homeTeam'], df['awayTeam'], df['win'],
            "Weekly suggestions: Home team win",
            fontSize=13)

        df = extract_week_suggestions('bothScore')
        visualizer.plot_advised_bets(
            df['homeTeam'], df['awayTeam'], df['bothScore'],
            "Weekly suggestions: Both team score",
            fontSize=13)

        df = extract_week_suggestions('>2.5')
        visualizer.plot_advised_bets(
            df['homeTeam'], df['awayTeam'], df['>2.5'],
            "Weekly suggestions: More than 2.5 goals",
            fontSize=13)

        df = extract_week_suggestions('>3.5')
        visualizer.plot_advised_bets(
            df['homeTeam'], df['awayTeam'], df['>3.5'],
            "Weekly suggestions: More than 3.5 goals",
            fontSize=13)
