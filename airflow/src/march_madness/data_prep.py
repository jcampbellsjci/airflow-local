from src.march_madness import utils, kenpom_data_prep, regular_season, tournament
import pandas as pd
from sqlalchemy.engine import Engine
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


def data_prep(engine: Engine):
    """
    Takes raw kaggle and kenpom data, cleans, combines, and finds differences. The function will write two data frames to the database, both of which capture all available NCAA tournament games for training. The first table contains all available stats and info for teams A and B in separate fields, the second captures the difference in each of those fields between team A and team B.

    Args:
        engine(Engine): SQL connection.

    Returns:
        None: No actual object returned. Data frames are written database.
    """
    
    log.info("Gathering raw kaggle data from db")
    raw_data = utils.fetch_kaggle_tables(engine = engine)

    log.info("Putting regular season game logs into long format")
    raw_game_log = utils.game_to_long(df = raw_data['MRegularSeasonDetailedResults'])

    log.info("Calculating regular season records")
    team_records = regular_season.calculate_records(df = raw_game_log)
    log.info("Calculating regular season statistics")
    stat_avg = regular_season.season_stat_summary(df = raw_game_log)

    kenpom_dict = {}
    season_iterable = range(2002, 2025, 1)
    
    log.info(f"Prepping kenpom data for seasons from {min(season_iterable)} to {max(season_iterable)}")
    for i in season_iterable:
        kenpom_dict["kenpom_" + str(i)] = kenpom_data_prep.kenpom_data_prep(
            season = i,
            spelling_df = raw_data['MTeamSpellings']
        )
    kenpom_df = (
        pd.concat(kenpom_dict, names = ["Season"])
        .reset_index(level = "Season")
        .assign(Season = lambda x: pd.to_numeric(x["Season"].str.replace("kenpom_", "", regex = True)))
    )


    log.info("Cleaning NCAA tourney seeds")
    tourney_seeds_clean = tournament.clean_tournament_seeds(df = raw_data['MNCAATourneySeeds'])
    log.info("Joining prepped data to tournament game logs")
    tourney_game_log = tournament.tourney_joiner(
        tourney_game_df = raw_data['MNCAATourneyDetailedResults'],
        seed_df = tourney_seeds_clean,
        record_df = team_records,
        stat_df = stat_avg,
        kenpom_df = kenpom_df
    )

    log.info("Writing raw tournament data to db")
    final_df = tournament.tourney_df_writer(
        df = tourney_game_log,
        teams_df = raw_data["MTeams"],
        engine = engine
    )
    log.info("Writing diff tournament data to db")
    tournament.tourney_diff_df_writer(df = final_df, engine = engine)

    print("Success")