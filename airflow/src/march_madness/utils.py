from sqlalchemy.engine import Engine
from src.database import db_functions
import pandas as pd

def fetch_kaggle_tables(engine: Engine, kaggle_tables: str = ['MNCAATourneyDetailedResults', 'MNCAATourneySeeds', 'MRegularSeasonDetailedResults', 'MTeamSpellings', 'MTeams']) -> dict[str, pd.DataFrame]:
    """
    Pulls relevant kaggle tables from database and stores in a dictionary.

    Args:
        engine (Engine): SQL connection.
        kaggle_tables (str): Table names to pull. Defaults to predefined list.
    
    Returns:
        dict[str, pd.DataFrame]: Dictionary of data frames for each specified kaggle table.
    """
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


def game_to_long(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes dataframe of game outcomes and doubles it so that we have a row where the winner is represented as team 1 and a row where the loser is.

    Args:
        df (pd.DataFrame): Data frame of game outcomes.

    Returns:
        pd.DataFrame: Data frame with two rows per game.
    """
    team_rename_dict = team_renamer(df = df)

    raw_game_log = pd.concat(
        [
            team_rename_dict["winner_team1"],
            team_rename_dict["loser_team1"]
        ]
    )

    return(raw_game_log)


def team_renamer(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Takes data frame of game outcomes that specify teams by wins and losses and renames fields to specify teams by team 1 or team 2. Returns dictionary with dataframe where winners are referenced as team 1 and another where losers are team 1.

    Args:
        df (pd.DataFrame): Data frame of game outcomes. Assumes teams are differentiated by wins and losses across fields.
    
    Returns:
        dict[str, pd.DataFrame]: Dictionary of two data frames, one where winning team is referenced as team 1, and another where the losing team is.
    """
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

    output_dict = {
        "winner_team1": w_team1,
        "loser_team1": l_team1
    }

    return(output_dict)