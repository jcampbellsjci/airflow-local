import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
from src.database import db_functions
from src.march_madness import kenpom_data_prep 
from pathlib import Path

def data_prep(engine, season):
    raw_data = fetch_kaggle_tables(engine = engine)

    raw_game_log = game_to_long(df = raw_data['MRegularSeasonDetailedResults'])

    team_records = calculate_records(df = raw_game_log)
    stat_avg = season_stat_summary(df = raw_game_log)

    kp_html = kenpom_data_prep.kp_html_import(season = season)
    kenpom_df = kenpom_data_prep.kp_clean(
        kp_html = kp_html,
        season = season,
        kaggle_spelling_df = raw_data['MTeamSpellings']
    )

    tourney_seeds_clean = clean_tournament_seeds(seed_df = raw_data['MNCAATourneySeeds'])

    final_df = tourney_game_raw(
        tourney_game_df = raw_data['MNCAATourneyDetailedResults'],
        stat_df = stat_avg,
        seed_df = tourney_seeds_clean,
        teams_df = raw_data['MTeams'],
        engine = engine
    )

    final_df_diff = tourney_game_diff(df = final_df, engine = engine)

    print("Success")


def fetch_kaggle_tables(engine, kaggle_tables = ['MNCAATourneyDetailedResults', 'MNCAATourneySeeds', 'MRegularSeasonDetailedResults', 'MTeamSpellings', 'MTeams']):
    raw_data = {}
    data_table_names = sorted(kaggle_tables)
    
    for i in data_table_names:
        df = db_functions.fetch_data(
            engine = engine,
            is_file = False,
            sql_text = "select * from march_madness." + i.lower()
        )
        raw_data[i] = df

    print("Created dictionary of following tables: " + ", ".join(list(raw_data.keys())))
    return(raw_data)


def game_to_long(df):
    w_team1_names = [
        i.replace("W", "Team1") if i.startswith("W")
        else i.replace("L", "Team2") if i.startswith("L")
        else i
        for i in df.columns
    ]
    l_team1_names = [
        i.replace("W", "Team2") if i.startswith("W")
        else i.replace("L", "Team1") if i.startswith("L")
        else i
        for i in df.columns
    ]

    w_team1 = df.copy()
    l_team1 = df.copy()
    w_team1.columns = w_team1_names
    l_team1.columns = l_team1_names

    raw_game_log = pd.concat([w_team1, l_team1])

    return(raw_game_log)


def calculate_records(df):
    team_records = (
        df
        .assign(
            Team1Win = lambda x: x["Team1Score"] > x["Team2Score"],
            Team1Loss = lambda x: x["Team1Score"] < x["Team2Score"]
        )
        .groupby(["Season", "Team1TeamID"])
        .agg(
            GamesPlayed = ("Team1TeamID", "count"),
            Wins = ("Team1Win", "sum"),
            Losses = ("Team1Loss", "sum")
        )
        .assign(WinRatio = lambda x: x["Wins"] / x["GamesPlayed"])
        .reset_index()
    )

    return(team_records)


def season_stat_summary(df):
    stat_fields = [
        i for i in df.columns if
        (i.startswith("Team1") or i.startswith("Team2"))
        and not (i.endswith("TeamID") or i.endswith("Loc"))
    ]

    # Grouping by team and season and finding averages of stat fields
    stat_avg = (
        df
        .groupby(["Season", "Team1TeamID"])[stat_fields]
        .agg("mean")
        .reset_index()
    )

    # For some of these averages, we'll calculate ratios as input features
    stat_avg = (
        stat_avg
        .assign(
            Team1FGP = lambda x: x["Team1FGM"] / x["Team1FGA"],
            Team1FGP3 = lambda x: x["Team1FGM3"] / x["Team1FGA3"],
            Team1FTP = lambda x: x["Team1FTM"] / x["Team1FTA"],
            Team2FGP = lambda x: x["Team2FGM"] / x["Team2FGA"],
            Team2FGP3 = lambda x: x["Team2FGM3"] / x["Team2FGA3"],
            Team2FTP = lambda x: x["Team2FTM"] / x["Team2FTA"]
        )
    )

    return(stat_avg)


def clean_tournament_seeds(seed_df):
    tourney_seeds_clean = (
        seed_df
        .assign(Seed = lambda x: pd.to_numeric(x["Seed"].str.replace(r"[A-Za-z]", "", regex = True)))
    )

    return(tourney_seeds_clean)


def tourney_game_raw(tourney_game_df, stat_df, seed_df, teams_df, engine):
    wtourney_team1_names = [
        i.replace("W", "Team1") if i.startswith("W")
        else i.replace("L", "Team2") if i.startswith("L")
        else i
        for i in tourney_game_df.columns
    ]
    ltourney_team1_names = [
        i.replace("W", "Team2") if i.startswith("W")
        else i.replace("L", "Team1") if i.startswith("L")
        else i
        for i in tourney_game_df.columns
    ]

    wtourney_team1 = tourney_game_df.copy()
    ltourney_team1 = tourney_game_df.copy()
    wtourney_team1.columns = wtourney_team1_names
    ltourney_team1.columns = ltourney_team1_names

    wtourney_team1_sample = wtourney_team1.sample(frac = .5)
    ltourney_team1_sample = (
        ltourney_team1
        .merge(
            wtourney_team1_sample[["Season", "Team2TeamID", "Team1TeamID"]],
            left_on = ["Season", "Team1TeamID", "Team2TeamID"],
            right_on = ["Season", "Team2TeamID", "Team1TeamID"],
            how = "left",
            suffixes=("", "_b")
        )
        .query("Team2TeamID_b.isnull()")
        .drop(columns = ["Team2TeamID_b", "Team1TeamID_b"])
    )

    raw_tourney_game_log = (
        pd.concat([wtourney_team1_sample, ltourney_team1_sample])
        .assign(Outcome = lambda x: np.where(x["Team1Score"] > x["Team2Score"], 1, 0))
        [["Season", "Team1TeamID", "Team2TeamID", "Outcome"]]
    )

    final_df = (
        raw_tourney_game_log
        .merge(
            seed_df,
            left_on = ["Season", "Team1TeamID"],
            right_on = ["Season", "TeamID"],
            how = "left"
        )
        .drop(columns = ["TeamID"])
        .merge(
            seed_df,
            left_on = ["Season", "Team2TeamID"],
            right_on = ["Season", "TeamID"],
            how = "left"
        )
        .drop(columns = ["TeamID"])
        .rename(columns = {"Seed_x": "Team1Seed", "Seed_y": "Team2Seed"})
    )

    final_df = (
        final_df
        .merge(
            stat_df,
            on = ["Season", "Team1TeamID"],
            how = "left"
        )
    )
    final_df.columns = [
        i.replace("Team1", "TeamA") if i.startswith("Team1")
        else i.replace("Team2", "TeamB") if i == ("Team2TeamID") or i == ("Team2Seed")
        else i.replace("Team2", "TeamAOpp") if i.startswith("Team2")
        else i
        for i in final_df.columns
    ]

    final_df = (
        final_df
        .merge(
            stat_df,
            left_on = ["Season", "TeamBTeamID"],
            right_on = ["Season", "Team1TeamID"],
            how = "left"
        )
        .drop(columns = ["Team1TeamID"])
    )
    final_df.columns = [
        i.replace("Team1", "TeamB") if i.startswith("Team1")
        else i.replace("Team2", "TeamBOpp") if i.startswith("Team2")
        else i
        for i in final_df.columns
    ]

    final_df = (
        final_df
        .assign(
            GameID = lambda x: x["Season"].astype("str") + "-" +
            x["TeamATeamID"].astype("str") + "-" +
            x["TeamBTeamID"].astype("str")
        )
        .assign(CreatedAt = pd.Timestamp.now(tz = "UTC"))
        .merge(
            teams_df[["TeamID", "TeamName"]],
            left_on = "TeamATeamID",
            right_on = "TeamID",
            how = "left"
        )
        .drop(columns = "TeamID")
        .rename(columns = {"TeamName":"TeamAName"})
        .merge(
            teams_df[["TeamID", "TeamName"]],
            left_on = "TeamBTeamID",
            right_on = "TeamID",
            how = "left"
        )
        .drop(columns = "TeamID")
        .rename(columns = {"TeamName":"TeamBName"})
    )
    final_df = (
        final_df[["GameID", "CreatedAt", "Season", "TeamATeamID", "TeamAName", "TeamBTeamID", "TeamBName"] +
        final_df.columns[3:-4].tolist()]
    )

    (
        final_df
        .to_sql(
            "ncaa_game_stats_raw",
            engine,
            if_exists = "replace",
            #if_exists = "append",
            index = False
        )
    )

    return(final_df)


def tourney_game_diff(df, engine):
    team_a_long = (
        df
        .melt(
            id_vars = "GameID",
            value_vars = ["TeamASeed"] + list(df.loc[:, "TeamAScore":"TeamAOppFTP"].columns),
            var_name = "Variable",
            value_name = "AValue"
        )
        .assign(Variable = lambda x: x["Variable"].str.replace("TeamA", ""))
    )

    team_b_long = (
        df
        .melt(
            id_vars = "GameID",
            value_vars = ["TeamBSeed"] + list(df.loc[:, "TeamBScore":"TeamBOppFTP"].columns),
            var_name = "Variable",
            value_name = "BValue"
        )
        .assign(Variable = lambda x: x["Variable"].str.replace("TeamB", ""))
    )

    long_diff_df = (
        team_a_long
        .merge(
            team_b_long,
            on = ["GameID", "Variable"],
            how = "inner"
        )
        .assign(DiffValue = lambda x: x["AValue"] - x["BValue"])
        .pivot(
            index = "GameID",
            columns = "Variable",
            values = "DiffValue"
        )
        .reset_index()
    )
    long_diff_df.columns.name = None

    final_diff_df = (
        df.loc[:, "GameID":"Outcome"]
        .merge(
            long_diff_df,
            on = "GameID",
            how = "inner"
        )
    )

    (
        final_diff_df
        .to_sql(
            "ncaa_game_stats_diff_raw",
            engine,
            if_exists = "replace",
            #if_exists = "append",
            index = False
        )
    )

    return(final_diff_df)
