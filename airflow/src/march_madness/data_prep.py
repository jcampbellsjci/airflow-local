from src.march_madness import utils, kenpom_data_prep, regular_season, tournament
import pandas as pd


def data_prep(engine, season):
    raw_data = utils.fetch_kaggle_tables(engine = engine)

    raw_game_log = utils.game_to_long(df = raw_data['MRegularSeasonDetailedResults'])

    team_records = regular_season.calculate_records(df = raw_game_log)
    stat_avg = regular_season.season_stat_summary(df = raw_game_log)

    kenpom_dict = {}
    for i in season:
        kenpom_dict["kenpom_" + str(i)] = kenpom_data_prep.kenpom_data_prep(
            season = i,
            spelling_df = raw_data['MTeamSpellings']
        )
    kenpom_df = (
        pd.concat(kenpom_dict, names = ["Season"])
        .reset_index(level = "Season")
        .assign(Season = lambda x: pd.to_numeric(x["Season"].str.replace("kenpom_", "", regex = True)))
    )


    tourney_seeds_clean = tournament.clean_tournament_seeds(df = raw_data['MNCAATourneySeeds'])
    tourney_game_log = tournament.tourney_joiner(
        tourney_game_df = raw_data['MNCAATourneyDetailedResults'],
        seed_df = tourney_seeds_clean,
        record_df = team_records,
        stat_df = stat_avg,
        kenpom_df = kenpom_df
    )

    final_df = tournament.tourney_df_writer(
        df = tourney_game_log,
        teams_df = raw_data["MTeams"],
        engine = engine
    )
    final_diff_df = tournament.tourney_diff_df_writer(df = final_df, engine = engine)

    print("Success")