import pandas as pd
import numpy as np
from sqlalchemy.engine import Engine
from src.march_madness import utils

def clean_tournament_seeds(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes data frame of kaggle tournament seeds and turns them into a data frame of integer seeds.

    Args:
        df (pd.DataFrame): Data frame of kaggle tournament seeds.
    
    Returns:
        pd.DataFrame: Data frame where tournament seeds are represented as a integer.
    """

    tourney_seeds_clean = (
        df
        .assign(Seed = lambda x: pd.to_numeric(x["Seed"].str.replace(r"[A-Za-z]", "", regex = True)))
    )

    return(tourney_seeds_clean)


def game_sampler(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes game log, splits and samples it so that each game has a 50% chance at team 1 being represented by the winning or losing team. Also adds an outcome field to identify if team 1 wins or losses.

    Args:
        df (pd.DataFrame): Data frame of game logs where team 1 is considered the winning team.

    Returns:
        pd.DataFrame: Data frame where each record has a 50% chance of team 1 winning or losing, with an additional outcome field.
    """

    tourney_game_dict = utils.team_renamer(df = df)

    wtourney_team1_sample = tourney_game_dict["winner_team1"].sample(frac = .5)
    ltourney_team1_sample = (
        tourney_game_dict["loser_team1"]
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

    return(raw_tourney_game_log)


def tourney_joiner(tourney_game_df: pd.DataFrame, seed_df: pd.DataFrame, record_df: pd.DataFrame, stat_df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes data frame of tourney games and joins seeds, season records and statistical summaries for each team.

    Args:
        tourney_game_df (pd.DataFrame): Data frame of tournament games.
        seed_df (pd.DataFrame): Data frame of tournament seeds.
        record_df (pd.DataFrame): Data frame of season records.
        stat_df (pd.DataFrame): Data frame of season statistical summaries.

    Returns:
        Data frame of tournament games complete with seeds, records and statistics.
    """
    
    final_df = game_sampler(df = tourney_game_df)

    final_df = (
        final_df
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

    return(final_df)


def tourney_df_writer(df: pd.DataFrame, teams_df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """
    Takes tournament data frame, finalizes it, and writes to database.

    Args:
        df (pd.DataFrame): Data frame of tourney games.
        teams_df (pd.DataFrame): Data frame of team names from kaggle.
        engine (Engine): SQL alchemy engine.

    Returns:
        pd.DataFrame: Finalized version of tournament data frame that is written to the database.
    """

    final_df = (
        df
        .assign(
            GameID = lambda x: x["Season"].astype("str") + "-" +
            x["TeamATeamID"].astype("str") + "-" +
            x["TeamBTeamID"].astype("str")
        )
        .assign(CreatedAt = pd.Timestamp.now(tz = "UTC"))
    )

    final_df = (
        final_df
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
            index = False
        )
    )

    return(final_df)


def tourney_diff_df_writer(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """
    Takes final tourney data frame and calculates differences between the two teams (rather than just showing stat x for both teams), writing results to database.

    Args:
        df (pd.DataFrame): Data frame of tourney games with joined stats.
        engine (Engine): SQL alchemy engine.

    Returns:
        pd.DataFrame: Finalized version of tournament data frame where rather than individual stats for each team, we have the difference in stats between teams.
    """

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
