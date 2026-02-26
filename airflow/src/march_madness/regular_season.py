import pandas as pd

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
