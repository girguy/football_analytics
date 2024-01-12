from re import A
from sys import set_asyncgen_hooks
import streamlit as st
import plotly.graph_objects as go
import pandas as pd

COLOR1 = "#13d6d0"
COLOR2 = "#FA8072"
COLOR3 = "white"
BOX_COLOR = "#1f2c56"
TITLE_COLOR = "white"
AVERAGE_LINE_COLOR = "white"
INDICATOR_COLOR = "white"
TEAM_POINTS_RANKING = "#d4af37"
HOME_TEAM_POINTS = "#d4af37"
HOME_TEAM_RANKING = "#d4af37"
HOME_TEAM_AS = "#1b750f"
HOME_TEAM_DS = COLOR3
AWAY_TEAM_POINTS = "#d4af37"
AWAY_TEAM_RANKING = "#d4af37"
AWAY_TEAM_AS = "#1b750f"
AWAY_TEAM_DS = COLOR3

featuresAbbrev = {'Points' : 'pts',
                'Goal Scored' : 'gs_cum',
                'Goal Conceded' : 'gc_cum',
                'Goal difference' : 'gd',
                'Form' : 'form',
                'Average goals scored the 4 last game' : ['HPKGS', 'APKGS'],
                'Average goals conceded the 4 last game' : ['HPKGC', 'APKGC'],
                'Average shoots on target the 4 last game' : ['HPKST', 'APKST'],
                'Average corners obtained the 4 last game' : ['HPKC', 'APKC'],
                'Average goals scored' : 'gs_cum',
                'Average goals scored at home' : 'avgGoalSHH',
                'Average goals scored away' : 'avgGoalSAA',
                'Average goals conceded' : 'gc_cum',
                'Average goals conceded at home' : 'avgGoalCHH',
                'Average goals conceded away' : 'avgGoalCAA'
                }

featuresDefinition = {'Form' : 'The Form gives a value for the current team strength. ' +
                               'The Form of a team is updated after each game and its new ' +
                               'value depends on the form of its opponent. More points are given ' +
                               'to weak teams that beat strong teams.'
                     }

class Visualizer:
    def __init__(self, statistics, teamsGames, firstSeason, lastSeason):
        self._statistics = statistics
        self._teamsGames = teamsGames
        self._firstSeason = firstSeason
        self._lastSeason = lastSeason
        self._featuresAbbrev = featuresAbbrev
        self._featuresDefinition = featuresDefinition
        self._seasons = self.get_seasons()
        self._matchDay = [i for i in range(1,39)]
        self._leagueStats = self.get_league_stats()
        self._teamStats = self.get_team_stats()
        
    def get_seasons(self):
        return [str(year) +'/'+str(year+1) for year in range(self._firstSeason, self._lastSeason)][::-1] # reversed

    def get_league_stats(self):
        return ['Points', 'Goal Scored', 'Goal Conceded', 'Goal difference', 'Form']

    def get_team_stats(self):
        return ['Average goals scored home', 'Average goals scored away',
                'Average goals conceded home', 'Average goals conceded away']

    def get_features_abbrev(self, userInput):
        return self._featuresAbbrev[userInput]

    def get_teams_season(self, season):
        # Extract the year from the season
        year = int(season.split('/')[1])
        # Query teams that played on matchDay 1 of the season
        season_teams = list(self._teamsGames.query(f"season == '{year}'")['team'].unique())
        # Add 'All' to the list and sort
        season_teams.append('All')
        season_teams = sorted(season_teams)
        return season_teams, year

    def get_league_stat(self, year, season, stat, teams):
        fig = go.Figure()
        data = self._teamsGames.query(f"season == '{year}' and team == '{teams}'")
        data = data.sort_values(by=['matchDay'], ascending=False)
        colStat = self.get_features_abbrev(stat)

        for team in teams:
            teamData = data.query(f"team == '{team}'")
            fig.add_trace(go.Scatter(name=team, mode='lines',
                                     x=teamData['matchDay'],
                                     y=teamData[colStat]))

        fig.update_layout(
            title='Evolution of '+stat+' during season '+str(season),
            xaxis_title="Match day",
            yaxis_title='Number of '+stat,
            dragmode=False,
            font=dict(
                size=14,
            )
        )
        return fig


    def get_league_average_stat(self, season, stat, teams):
        fig = go.Figure()
        year = int(season.split('/')[1])
        features = ['id','HN', 'AN', 'HPKGS', 'APKGS', 'HPKGC', 'APKGC', 'HPKST', 'APKST', 'HPKC', 'APKC', 'season']

        stats = self.get_features_abbrev(stat)

        data = self._statistics.query(f"season == '{year}' and (HN == '{teams}' or AN == '{teams}')")[features]
        for team in teams:
            matchDay = [i+5 for i in range(data.shape[0])]
            homeTeamData = data.query(f"HN == '{team}'")[['id', stats[0]]]
            homeTeamData.columns = ['id', 'AVG']
            awayTeamData = data.query(f"AN == '{team}'")[['id', stats[1]]]
            awayTeamData.columns = ['id', 'AVG']
            teamData = pd.concat([homeTeamData, awayTeamData]).sort_values(by=['id'])
            fig.add_trace(go.Scatter(name=team ,mode='lines', x=matchDay, y=teamData['AVG'], marker_symbol="star"))

        fig.update_layout(
            title='Evolution of the '+stat+' during season '+str(season), #
            xaxis_title="Match day",
            yaxis_title=stat,
            dragmode=False,
            font=dict(
                size=14,
            )
        )
        return fig
    

    def create_league_plot(self, season, stat, teams, allTeams):
        year = int(season.split('/')[1])

        if 'All' in teams:
            teams = allTeams
  
        type = stat.split()
        if 'Average' in type:
            fig = self.get_league_average_stat(season, stat, teams)
            st.plotly_chart(fig, theme="streamlit",
                        use_container_width=True,
                        config={'displayModeBar': False,
                                'staticPlot': False})
        else:
            fig = self.get_league_stat(year, season, stat, teams)
            st.plotly_chart(fig, theme="streamlit",
                        use_container_width=True,
                        config={'displayModeBar': False,
                                'staticPlot': False})

    # =====================================================
    # LEAGUE
    # =====================================================

    def create_teams_plot(self, x, y, yLeague, seasonChoice, statChoice, fontSize):
        if 'home' in statChoice.split() or 'away' in statChoice.split():
            fig = self.get_team_performance_away_or_home_plot(x, y, yLeague, seasonChoice, statChoice, fontSize)
            st.plotly_chart(fig, theme="streamlit",
                        use_container_width=True,
                        config={'displayModeBar': False,
                                'staticPlot': False})
        else:
            fig = self.get_team_performance_plot(x, y, yLeague, seasonChoice, statChoice, fontSize)
            st.plotly_chart(fig, theme="streamlit",
                        use_container_width=True,
                        config={'displayModeBar': False,
                                'staticPlot': False})

    def get_team_performance_plot(self, x, y, yLeague, seasonChoice, statChoice, fontSize):
        fig = go.Figure()
        y = [round(i, 2) for i in y]
        if statChoice == 'Average goals scored':
            fig.add_trace(go.Bar(x=x, y=y, text=y, name='EPL Teams', marker_color=COLOR1))
        else:
            fig.add_trace(go.Bar(x=x, y=y, text=y, name='EPL Teams', marker_color=COLOR2))
        fig.add_trace(go.Scatter(x=x, y=yLeague, name='League average', marker_color=AVERAGE_LINE_COLOR))

        fig.update_layout(title={
                'text': "<span style='color:" + TITLE_COLOR + "'>" + statChoice + ' by EPL Teams',
                'y':0.9,
                'x':0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
            xaxis_title="EPL Teams",
            yaxis_title='Goals',
            font=dict(size=fontSize),
            xaxis = go.layout.XAxis(tickangle = 45),
            autosize=False,
            width=250,
            height=250,
            margin=dict(l=10, r=10, b=10, t=100, pad=4),
            paper_bgcolor=BOX_COLOR,
            plot_bgcolor=BOX_COLOR,
            showlegend=True,
            dragmode=False,
            legend=dict(orientation="h", yanchor="top", y=1.35, xanchor="left", x=0.4)
        )

        fig.update_xaxes(showgrid=False, zeroline=False)
        fig.update_yaxes(showgrid=False, zeroline=False)

        return fig

    def get_team_performance_away_or_home_plot(self, x, y, yLeague, seasonChoice, statChoice, fontSize):
        fig = go.Figure()
        fig.add_trace(go.Bar(x=x, y=y, name='EPL Teams', marker_color=COLOR2))
        fig.add_trace(go.Scatter(x=x, y=yLeague, name='League average', marker_color="#7E909A"))

        fig.update_layout(title={
                'text': statChoice + ' by EPL Team for '+seasonChoice,
                'y':0.9,
                'x':0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
            xaxis_title="EPL Teams",
            yaxis_title='Goals',
            font=dict(size=fontSize),
            xaxis = go.layout.XAxis(tickangle = 45),
            autosize=False,
            width=250,
            height=250,
            margin=dict(l=10, r=10, b=10, t=100, pad=4),
            paper_bgcolor=BOX_COLOR,
            plot_bgcolor=BOX_COLOR,
            dragmode=False,
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="top",
                y=0.40,
                xanchor="center",
                x=0.01
            )
        )

        fig.update_xaxes(showgrid=False, zeroline=False)
        fig.update_yaxes(showgrid=False, zeroline=False)

        return fig
    
    def conceded_scored_home_away_plot(self,teamsName, goalScoredHome, goalScoredAway, scored=True, fontSize = 12):

        goalScoredHome = [round(i, 2) for i in goalScoredHome]
        goalScoredAway = [round(i, 2) for i in goalScoredAway]

        colorHome = COLOR1
        colorAway = COLOR2
        if scored:
            title = "<span style='color:" + TITLE_COLOR + "'>" + "Goal scored home | away"
        else:
            title = "<span style='color:" + TITLE_COLOR + "'>" + "Goal conceded home | away"

        fig = go.Figure()
        fig.add_trace(go.Bar(
                        x=teamsName,
                        y=goalScoredHome,
                        text=goalScoredHome,
                        base=0,
                        marker_color=colorHome,
                        name='Home'))

        fig.add_trace(go.Bar(
                        x=teamsName,
                        y=goalScoredAway,
                        text=goalScoredAway,
                        base=0,
                        marker_color=colorAway,
                        name='Away'
                        ))

        fig.update_layout(title={
                'text': title,
                'y':0.87,
                'x':0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
                showlegend=True,
                legend=dict(
                    orientation="h",
                    yanchor="top",
                    y=0.99,
                    xanchor="center",
                    x=0.8
                ),
                dragmode=False,
                font=dict(size=fontSize), 
                autosize=False,
                width=550,
                height=250,
                margin=dict(l=10, r=10, b=10, t=40, pad=4),
                paper_bgcolor=BOX_COLOR,
                plot_bgcolor=BOX_COLOR
                )
        fig.update_traces(textposition='inside', textfont_size=15)

        fig.update_xaxes(showgrid=False, zeroline=False)
        fig.update_yaxes(showgrid=False, zeroline=False)

        st.plotly_chart(fig, theme="streamlit",
                        use_container_width=True,
                        config={'displayModeBar': False,
                                'staticPlot': False})

    # =====================================================
    # TEAMS
    # =====================================================


    def ranking_points(self, teamName, ranking=False, points=False, height=None, fontSize = 12):
        fig = go.Figure()

        if ranking:
            number = ranking
            title = "Ranking"
        else:
            number = points
            title = "Points"        

        fig.add_trace(go.Indicator(
        mode = "number",
        number_font_color=TEAM_POINTS_RANKING,
        value = number
        ))

        fig.update_layout(
            title={
                'text': "<span style='color:" + TITLE_COLOR + "'>"+title,
                'y':0.97,
                'x':0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
            font=dict(size=fontSize),
            autosize=False,
            height=height,
            dragmode=False,
            margin=dict(l=10, r=10, b=10, t=40, pad=4),
            paper_bgcolor=BOX_COLOR,
            plot_bgcolor=BOX_COLOR
            )
        
        fig.update_xaxes(showgrid=False, zeroline=False)
        fig.update_yaxes(showgrid=False, zeroline=False)
        
        st.plotly_chart(fig, theme="streamlit",
                        use_container_width=True,
                        config={'displayModeBar': False,
                                'staticPlot': False})
    
    def outcomes(self, teamName, outcomes, height, fontSize):
        
        labels = ["Loose", "Draw", "Win"]
        colors = [COLOR3, COLOR2, COLOR1]
        
        fig = go.Figure()
    
        fig.add_trace(go.Pie(labels=labels, values=outcomes, hole=.3, name=teamName))

        fig.update_traces(textinfo='value', textfont_size=20, title = "",
                        marker=dict(colors=colors))
        
        text = "<span style='color:" + TITLE_COLOR + "'>Season outcomes"
        
        fig.update_layout(title={
                'text': text, 
                'y':0.97,
                'x':0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
            legend=dict(orientation="v"), font=dict(size=fontSize), showlegend=True,
            autosize=False,
            height=height,
            margin=dict(l=10, r=10, b=10, t=40, pad=4),
            paper_bgcolor=BOX_COLOR,
            plot_bgcolor=BOX_COLOR,
            dragmode=False,
            annotations=[dict(text="", x=0.18, y=0.5, font_size=20, showarrow=False),
                        dict(text="", x=0.82, y=0.5, font_size=20, showarrow=False)]
            )

        fig.update_xaxes(showgrid=False, zeroline=False)
        fig.update_yaxes(showgrid=False, zeroline=False)

        st.plotly_chart(fig, theme="streamlit",
                        use_container_width=True,
                        config={'displayModeBar': False,
                                'staticPlot': False})

    def team_stat_evolution(self, matchDay, values, stat, goals=False, height=None, fontSize = 12):
        fig = go.Figure()

        if stat == 'Form':
            color = COLOR1
        elif stat == 'Shots on target':
            color = COLOR1
        else: # corners
            color = COLOR1

        if goals:
            fig.add_trace(go.Scatter(name='Scored', mode='lines', x=matchDay, y=values[0], marker_color=COLOR1))
            fig.add_trace(go.Scatter(name='Conceded', mode='lines', x=matchDay, y=values[1], marker_color=COLOR3))
            fig.add_trace(go.Scatter(name='Difference', mode='lines', x=matchDay, y=values[2], marker_color=COLOR2))
            showLegend = True
            
        else:
            fig.add_trace(go.Scatter(mode='lines', x=matchDay, y=values, marker_color=color))
            showLegend = False

        text = "<span style='color:" + TITLE_COLOR + "'>" + stat
        fig.update_layout(title={
                'text': text,
                'y':0.97,
                'x':0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
            legend=dict(orientation="v"), font=dict(size=fontSize), showlegend=showLegend,
            autosize=False,
            height=height,
            margin=dict(l=10, r=10, b=10, t=40, pad=4),
            paper_bgcolor=BOX_COLOR,
            plot_bgcolor=BOX_COLOR,
            dragmode=False,
            annotations=[dict(text="", x=0.18, y=0.5, font_size=20, showarrow=False),
                        dict(text="", x=0.82, y=0.5, font_size=20, showarrow=False)]
            )

        fig.update_xaxes(showgrid=False, zeroline=False)
        fig.update_yaxes(showgrid=False, zeroline=False)

        st.plotly_chart(fig, theme="streamlit",
                        use_container_width=True,
                        config={'displayModeBar': False,
                                'staticPlot': False})

    def stat_evolution_teams(self, teamsChoice, data, stat, statAbbrev, season, allTeams, height, fontSize):
        fig = go.Figure()

        if 'All' in teamsChoice:
            teams = allTeams
        else:
            teams = teamsChoice

        for team in teams:
            teamData = data.query(f"team == '{team}'")
            fig.add_trace(go.Scatter(name=team,
                                     mode='lines',
                                     x=teamData['matchDay'],
                                     y=teamData[statAbbrev]))

        text = "<span style='color:" + TITLE_COLOR + "'>" + stat + " during season " + season
        fig.update_layout(title={
                'text': text,
                'y':0.97,
                'x':0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
            xaxis_title="Match day",
            yaxis_title='Number of '+ stat,
            legend=dict(orientation="h"), font=dict(size=fontSize), showlegend=True,
            autosize=False,
            margin=dict(l=10, r=10, b=10, t=40, pad=4),
            paper_bgcolor=BOX_COLOR,
            plot_bgcolor=BOX_COLOR,
            height=height,
            dragmode=False,
            annotations=[dict(text="", x=0.18, y=0.5, font_size=20, showarrow=False),
                        dict(text="", x=0.82, y=0.5, font_size=20, showarrow=False)]
            )
 
        fig.update_xaxes(showgrid=False, zeroline=False)
        fig.update_yaxes(showgrid=False, zeroline=False)

        st.plotly_chart(fig, theme="streamlit",
                        use_container_width=True,
                        config={'displayModeBar': False,
                                'staticPlot': False})

    # =====================================================
    # FIXTURES : Match up
    # =====================================================

    def epl_season_stats(self, title, number, height, fontSize, color):
        fig = go.Figure()

        fig.add_trace(go.Indicator(
            mode = "number",
            number_font_color=color,
            number_font_size=50,
            value = number
        ))

        fig.update_layout(
            title={
                'text': "<span style='color:" + TITLE_COLOR + "'>"+title,
                'y':0.8,
                'x':0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
            legend=dict(orientation="h"), font=dict(size=fontSize), showlegend=False,
            autosize=False,
            height=height,
            margin=dict(l=10, r=10, b=10, t=40, pad=4),
            dragmode=False,
            paper_bgcolor=BOX_COLOR,
            plot_bgcolor=BOX_COLOR
            )
        
        st.plotly_chart(fig, theme="streamlit",
                        use_container_width=True,
                        config={'displayModeBar': False,
                                'staticPlot': False})


    def create_plot_teams_games_outcomes(self, homeTeam, awayTeam, height, fontSize):

        fig = go.Figure()

        fig.add_trace(go.Bar(
                        x=awayTeam['outcomes'],
                        y=['Loose', 'Draw', 'Win'],
                        text=awayTeam['outcomes'],
                        marker_color=[COLOR2, COLOR2, COLOR2],
                        name=awayTeam['name'],
                        width=0.40,
                        orientation='h'))

        fig.add_trace(go.Bar(
                        x=homeTeam['outcomes'],
                        y=['Loose', 'Draw', 'Win'],
                        text=homeTeam['outcomes'],
                        marker_color=[COLOR1, COLOR1, COLOR1],
                        name=homeTeam['name'],
                        width=0.40,
                        orientation='h'))


        fig.update_traces(textposition='inside')

        fig.update_layout(
            title={
                'text': "<span style='color:" + TITLE_COLOR + "'>" + "League games outcomes",
                'y':0.98,
                'x':0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
            legend=dict(orientation="v"), legend_traceorder="reversed",
            showlegend=True,
            font=dict(size=fontSize),
            autosize=False,
            height=height,
            dragmode=False,
            margin=dict(l=10, r=10, b=10, t=40, pad=4),
            annotations=[dict(text="", x=0.18, y=0.5, font_size=20, showarrow=False),
                         dict(text="", x=0.82, y=0.5, font_size=20, showarrow=False)],
            paper_bgcolor=BOX_COLOR,
            plot_bgcolor=BOX_COLOR
            )

        fig.update_xaxes(visible=False)

        st.plotly_chart(fig, theme="streamlit",
                        use_container_width=True,
                        config={'displayModeBar': False,
                                'staticPlot': False})
    
    def create_plot_teams_goals(self, homeTeam, awayTeam, height, fontSize):
        
        teamsName = [homeTeam['name'], awayTeam['name']]
        goalScored = [homeTeam['scored'], awayTeam['scored']]
        goalConceded = [homeTeam['conceded'], awayTeam['conceded']]
        goalDifference = [homeTeam['difference'], awayTeam['difference']]

        fig = go.Figure()
        fig.add_trace(go.Bar(
                        x=teamsName,
                        y=goalScored,
                        base=0,
                        text=goalScored,
                        marker_color=COLOR1,
                        name='Goals scored'))

        fig.add_trace(go.Bar(
                        x=teamsName,
                        y=goalConceded,
                        base=0,
                        text=goalConceded,
                        marker_color='white',
                        name='Goals conceded'
                        ))

        fig.add_trace(go.Bar(
                        x=teamsName,
                        y=goalDifference,
                        text=goalDifference,
                        base=0,
                        marker_color=COLOR2,
                        name='Goal difference'
                        ))

        fig.update_layout(title={
                'text': "<span style='color:" + TITLE_COLOR + "'>" + " Goal scored | conceded | difference ",
                'y':0.97,
                'x':0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
            legend=dict(orientation="h"), font=dict(size=fontSize), showlegend=False,
            dragmode=False,
            autosize=False,
            height=height,
            margin=dict(l=10, r=10, b=10, t=40, pad=4),
            paper_bgcolor=BOX_COLOR,
            plot_bgcolor=BOX_COLOR
            )
                        
        fig.update_traces(textposition='inside', textfont_size=15)

        fig.update_xaxes(showgrid=False, zeroline=False)
        fig.update_yaxes(showgrid=False, zeroline=False)
        
        st.plotly_chart(fig, theme="streamlit",
                        use_container_width=True,
                        config={'displayModeBar': False,
                                'staticPlot': False})

    
    # =====================================================
    # FIXTURES : Last 4 Games
    # =====================================================

    def recent_outcomes(self, teamName, outcomes, height, fontSize):

        labels = ["Win", "Draw", "Loose"]
        colors = [COLOR1, COLOR2, COLOR3]

        # Create subplots: use 'domain' type for Pie subplot
        fig = go.Figure()

        fig.add_trace(go.Pie(labels=labels, values=outcomes, hole=.3, name=teamName))

        fig.update_traces(textinfo='value', textfont_size=20, title = "",
                        marker=dict(colors=colors))
        
        text1 = "<span style='color:" + TITLE_COLOR + "'>" + teamName
        text2 = "<span style='color:" + TITLE_COLOR + "'> : Four last games outcomes"
        text = text1 + text2

        fig.update_layout(title={
                'text': text,
                'y':0.97,
                'x':0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
            legend=dict(orientation="v"), font=dict(size=fontSize), showlegend=True,
            legend_traceorder="reversed",
            autosize=False,
            dragmode=False,
            height=height,
            margin=dict(l=10, r=10, b=10, t=40, pad=4),
            paper_bgcolor=BOX_COLOR,
            plot_bgcolor=BOX_COLOR,
            annotations=[dict(text="", x=0.18, y=0.5, font_size=20, showarrow=False),
                        dict(text="", x=0.82, y=0.5, font_size=20, showarrow=False)]
            )

        st.plotly_chart(fig, theme="streamlit",
                        use_container_width=True,
                        config={'displayModeBar': False,
                                'staticPlot': False})

    def recent_form(self, names, forms, height, fontSize):

        fig = go.Figure()

        fig.add_trace(go.Indicator(
            mode = "number",
            number_font_color=INDICATOR_COLOR,
            value = forms[0],
            number_font_size=60,
            title = {"text": "<span style='font-size:1em;color:" + COLOR1 + "'>" + names[0] + "</span>"},
            domain = {'x': [0, 0.5], 'y': [0, 1]}))

        fig.add_trace(go.Indicator(
            mode = "number",
            number_font_color=INDICATOR_COLOR,
            value = forms[1],
            number_font_size=60,
            title = {"text": "<span style='font-size:1em;color:"+ COLOR2 +"'>" + names[1] + "</span>"},
            domain = {'x': [0.6, 1], 'y': [0, 1]}))

        fig.update_layout(title={
                'text': "<span style='color:" + TITLE_COLOR + "'>Form",
                'y':0.97,
                'x':0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
            legend=dict(orientation="h"), font=dict(size=fontSize), showlegend=False,
            autosize=False,
            dragmode=False,
            height=height,
            margin=dict(l=10, r=10, b=10, t=40, pad=4),
            paper_bgcolor=BOX_COLOR,
            plot_bgcolor=BOX_COLOR
            )
        
        st.plotly_chart(fig, theme="streamlit",
                        use_container_width=True,
                        config={'displayModeBar': False,
                                'staticPlot': False})
        

    def recent_goals_scored_conceded(self, teamsName, homeTeam, awayTeam, height, fontSize):
        fig = go.Figure()

        text1 = "<span style='color:" + TITLE_COLOR + "'>"
        text2 = "Goals scored | conceded the last four games"
        text = text1 + text2

        fig = go.Figure()

        fig.add_trace(go.Bar(
            y=['Conceded', 'Scored'],
            x=awayTeam,
            text=awayTeam,
            name=teamsName[1],
            width=0.33,
            orientation='h',
            marker=dict(
                color=COLOR2
            )
        ))

        fig.add_trace(go.Bar(
            y=['Conceded', 'Scored'],
            x=homeTeam,
            text=homeTeam,
            name=teamsName[0],
            width=0.33,
            orientation='h',
            marker=dict(
                color=COLOR1
            )
        ))

        fig.update_traces(textposition='inside', textfont_size=13)

        fig.update_layout(title={
                'text': text,
                'y':0.97,
                'x':0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
            legend=dict(orientation="v"), font=dict(size=fontSize), showlegend=True,
            legend_traceorder="reversed",
            autosize=False,
            dragmode=False,
            height=height,
            margin=dict(l=10, r=10, b=10, t=40, pad=4),
            paper_bgcolor=BOX_COLOR,
            plot_bgcolor=BOX_COLOR,
            bargap=0.4, # gap between bars of adjacent location coordinates.
            bargroupgap=0.0 # gap between bars of the same location coordinate.
            )

        fig.update_xaxes(visible=False)

        st.plotly_chart(fig, theme="streamlit",
                        use_container_width=True,
                        config={'displayModeBar': False,
                                'staticPlot': False})

    def average_goals(self, teamName, avgs, home=True, height=None, fontSize=None):
        fig = go.Figure()

        sizeTitle = str(1)

        if home:
            color = COLOR1
            state = "at home"
        else:
            color = COLOR2
            state = "away"

        text1 = "<span style='font-size:" + sizeTitle + "em;color:" + TITLE_COLOR + "'>Average number of goals<br>"
        text2 = "<span style='font-size:" + sizeTitle + "em;color:" + color + "'>scored "+state+" by<br>"
        text3 = "<span style='font-size:" + sizeTitle + "em;color:" + color + "'>"+teamName+"<br>"
        text = text1 + text2 +text3

        fig.add_trace(go.Indicator(
            mode = "number",
            number_font_color=INDICATOR_COLOR,
            number_font_size=55,
            value = avgs[0],
            title = {"text": "<span style='font-size:" + sizeTitle + "em'>" + text + "</span>"},
            domain = {'row': 0, 'column': 0}))

        text2 = "<span style='font-size:" + sizeTitle + "em;color:" + color + "'>conceded "+state+" by<br>"
        text = text1 + text2 + text3

        fig.add_trace(go.Indicator(
            mode = "number",
            number_font_color=INDICATOR_COLOR,
            number_font_size=55,
            value = avgs[1],
            title = {"text": "<span style='font-size:" + sizeTitle + "em'>" + text + "</span>"},
            domain = {'row': 0, 'column': 1}))
                

        fig.update_layout(
            grid = {'rows': 1, 'columns': 2, 'pattern': "independent"},
            autosize=False,
            height=height,
            dragmode=False,
            margin=dict(l=10, r=10, b=10, t=40, pad=4),
            paper_bgcolor=BOX_COLOR,
            plot_bgcolor=BOX_COLOR
            )
        
        st.plotly_chart(fig, theme="streamlit",
                        use_container_width=True,
                        config={'displayModeBar': False,
                                'staticPlot': False})

    # =====================================================
    # FIXTURES : Last 6 matchups
    # =====================================================

    def avg_points_against(self, teamsName, avgPoints, height, fontSize, away=False):
        fig = go.Figure()

        sizeTitle = str(1)

        if away:
            color = COLOR2
        else:
            color = COLOR1

        text1 = "<span style='font-size:" + sizeTitle + "em;color:"+ color + "'>"+teamsName[0] +" "
        text2 = "<span style='font-size:" + sizeTitle + "em'>average number of<br>"
        text3 = "<span style='font-size:" + sizeTitle + "em'>point taken against<br>"
        text4 = "<span style='font-size:" + sizeTitle + "em'>"+ teamsName[1] +"<br>"
        text = text1 + text2 + text3 + text4

        fig.add_trace(go.Indicator(
        mode = "number",
        number_font_color=INDICATOR_COLOR,
        number_font_size=55,
        value = avgPoints,
        title = {"text": text}
        ))

        fig.update_layout(
            legend=dict(orientation="h"), font=dict(size=fontSize), showlegend=False,
            autosize=False,
            dragmode=False,
            height=height,
            margin=dict(l=10, r=10, b=10, t=40, pad=4),
            paper_bgcolor=BOX_COLOR,
            plot_bgcolor=BOX_COLOR
            )
        
        st.plotly_chart(fig, theme="streamlit",
                        use_container_width=True,
                        config={'displayModeBar': False,
                                'staticPlot': False})

    def outcomes_matchup(self, teamsName, homeTeamStats, awayTeamStats, height, fontSize):
        fig = go.Figure()

        stats = ['Avg conceded', 'Avg scored', 'Loss', 'Draw', 'Win']
        
        fig = go.Figure()

        fig.add_trace(go.Bar(
            y=stats,
            x=homeTeamStats,
            text=[-i for i in homeTeamStats],
            name=teamsName[0],
            orientation='h',
            marker=dict(
                color=COLOR1
            )
        ))
        fig.add_trace(go.Bar(
            y=stats,
            x=awayTeamStats,
            text=awayTeamStats,
            name=teamsName[1],
            orientation='h',
            marker=dict(
                color=COLOR2
            )
        ))
        text1 = "<span style='color:" + TITLE_COLOR + "'>Last 6 league statistics between "
        text2 = teamsName[0] + " and " + teamsName[1]
        text = text1 + text2 

        fig.update_layout(
            title={'text': text,
                   'y':0.97, 'x':0.5,
                   'xanchor': 'center', 'yanchor': 'top'},
            barmode='relative',
            legend=dict(orientation="h"), font=dict(size=fontSize), showlegend=True,
            autosize=False,
            dragmode=False,
            height=height,
            margin=dict(l=10, r=10, b=10, t=40, pad=4),
            paper_bgcolor=BOX_COLOR,
            plot_bgcolor=BOX_COLOR
            )

        fig.update_traces(textposition='inside', textfont_size=14)
        fig.update_xaxes(visible=False)

        st.plotly_chart(fig, theme="streamlit",
                        use_container_width=True,
                        config={'displayModeBar': False,
                                'staticPlot': False})

    def last_6_games_result(self, teamsName, results, homePlaces, awayPlaces, dates, height, fontSize):
        fig = go.Figure()

        columnNames = ['Home team', 'Score', 'Away team', 'Date']

        fig = go.Figure(data=[go.Table(
                 header=dict(values = columnNames,
                             line_color=BOX_COLOR,
                             fill_color="black"),
                 cells=dict(values = [homePlaces, results, awayPlaces, dates],
                            line_color=BOX_COLOR,
                            fill_color=BOX_COLOR))
                 ])
        text1 = "<span style='color:" + TITLE_COLOR + "'>Last 6 games between "
        text2 = teamsName[0] + " and " + teamsName[1]
        text = text1 + text2

        fig.update_layout(
            title={'text': text,
                   'y':0.97, 'x':0.5,
                   'xanchor': 'center', 'yanchor': 'top'},
            legend=dict(orientation="h"), font=dict(size=fontSize), showlegend=False,
            autosize=False,
            dragmode=False,
            height=height,
            margin=dict(l=10, r=10, b=10, t=40, pad=4),
            paper_bgcolor=BOX_COLOR,
            plot_bgcolor=BOX_COLOR
            )

        st.plotly_chart(fig, theme="streamlit",
                        use_container_width=True,
                        config= {'displayModeBar': False,
                                 'staticPlot': False})

    # =====================================================
    # FIXTURES : Bets
    # =====================================================

    def odd_circle(self, oddName, percentage, width, height, fontSize):
        colors = [HOME_TEAM_POINTS, INDICATOR_COLOR]
        percentages = [percentage, 100-percentage]

        fig = go.Figure()
        fig.add_trace(go.Pie(values=percentages, hole=.7, name=oddName))
        fig.update_traces(textinfo="none", marker=dict(colors=colors))
        
        text = "<span style='color:" + TITLE_COLOR + "'>"+oddName
        fig.update_layout(title={
                'text': text, 
                'y':0.97,
                'x':0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
            legend=dict(orientation="h"), font=dict(size=fontSize), showlegend=False,
            autosize=True,
            dragmode=False,
            width=width,
            height=height,
            margin=dict(l=10, r=10, b=10, t=40, pad=4),
            annotations=[dict(text="<span style='color:" + HOME_TEAM_POINTS + "'>"+str(percentage)+"%",
                              x=0.5,
                              y=0.5,
                              font_size=20,
                              showarrow=False)],
            paper_bgcolor=BOX_COLOR,
            plot_bgcolor=BOX_COLOR
            )

        st.plotly_chart(fig, theme="streamlit",
                        use_container_width=True,
                        config={'displayModeBar': False,
                                'staticPlot': False})

    # =====================================================
    # Betting
    # =====================================================        

    # add the league average !
    def plot_teams_percentages_goal_scoring(self, df, minGoal, title, width, height, fontSize):
        fig = go.Figure()

        df = df.sort_values(by = minGoal, ascending=True)

        fig.add_trace(go.Bar(
            x = df[minGoal],
            y = df['team'],
            #text=df[minGoal],
            orientation='h',
            width=0.7,
            marker=dict(
                color=COLOR1
            )
        ))

        fig.update_layout(title={
                'text': title,
                'y':0.97,
                'x':0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
            legend=dict(orientation="h"), font=dict(size=fontSize), showlegend=False,
            dragmode=False,
            width=width,
            height=height,
            margin=dict(l=10, r=10, b=10, t=40, pad=4),
            paper_bgcolor=BOX_COLOR,
            plot_bgcolor=BOX_COLOR,
            bargap=0.4, # gap between bars of adjacent location coordinates.
            bargroupgap=0.0 # gap between bars of the same location coordinate.
            )

        fig.update_xaxes(visible=False)

        st.plotly_chart(fig, theme="streamlit",
                        use_container_width=True,
                        config= {'displayModeBar': False,
                                 'staticPlot': False})

    
    def plot_advised_bets(self, homeTeams, awayTeams, results, title, fontSize):
        fig = go.Figure()

        fig = go.Figure(data=[go.Table(
                    header=dict(values=['Home team', 'Away team', 'Percentage'],
                                line_color=BOX_COLOR,
                                fill_color="black"),
                    cells=dict(values=[homeTeams, awayTeams, results],
                               line_color=BOX_COLOR,
                               fill_color=BOX_COLOR))
                               ])
        text = "<span style='color:" + TITLE_COLOR + "'>"+title

        fig.update_layout(
            title={'text': text,
                    'y':0.94, 'x':0.5,
                    'xanchor': 'center', 'yanchor': 'top'},
            legend=dict(orientation="h"), font=dict(size=fontSize), showlegend=False,
            dragmode=False,
            autosize=False,
            width=550,
            height=163,
            margin=dict(l=10, r=10, b=10, t=40, pad=4),
            paper_bgcolor=BOX_COLOR,
            plot_bgcolor=BOX_COLOR
            )

        st.plotly_chart(fig, theme="streamlit",
                        use_container_width=True,
                        config= {'displayModeBar': False,
                                 'staticPlot': False})
