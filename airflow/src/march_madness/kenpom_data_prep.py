from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


def kenpom_data_prep(season: int, spelling_df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes raw HTML from kenpom website, cleans and formats it into a table, and then fixes spelling of teams to align with kaggle datasets. Returns a clean data frame.

    Args:
        season(int): NCAA season we're getting kenpom data for.
        spelling_df(pd.DataFrame): Kaggle dataset of team spellings.

    Returns:
        pd.DataFrame: Clean dataframe of kenpom data for a given season with kaggle spellings.
    """
    
    kenpom_html = kenpom_html_import(season = season)

    kenpom_df = kenpom_clean(html = kenpom_html)

    kenpom_df = kenpom_speller(input_df = kenpom_df, spelling_df = spelling_df)

    return(kenpom_df)


def kenpom_html_import(season: int) -> BeautifulSoup:
    """
    Imports kenpom site HTML for a given season as a beautiful soup object.

    Args:
        season(int): NCAA season we're getting kenpom data for.

    Returns:
        BeautifulSoup: Beautiful soup object for given season.
    """
    
    project_root = Path(__file__).resolve().parents[2]
    file_path = project_root / "data/march_madness/kp_html.csv"
    kp_html = pd.read_csv(file_path)

    raw_string = (
        kp_html
        .query("Season == @season")
    )["HTML"].tolist()

    raw_table = BeautifulSoup(raw_string[0], "html.parser")

    return(raw_table)


def kenpom_clean(html: BeautifulSoup) -> pd.DataFrame:
    """
    Takes raw beautiful soup object and transforms it into a clean pandas dataframe.

    Args:
        html(BeautifulSoup): Beautiful soup object containing raw kenpom html.

    Returns:
        pd.DataFrame: Clean version of kenpom table, ready for analysis.
    """

    raw_table = html
    rows = raw_table.find_all("tr")

    headers = []
    for row in rows[:]:
        cells = row.find_all(["th"])
        headers.append([cell.get_text(strip = True) for cell in cells])
    headers = headers[1]
    headers = headers[0:headers.index("Luck") + 1] + ["SOSNetRtg", "SOSORtg", "SOSDRtg", "NCSOSNetRtg"]

    data = []
    for row in rows[:]:
        cells = row.find_all(["td"])
        data.append([cell.get_text(strip = True) for cell in cells])
    data = list(filter(None, data))

    kp_finalized = [[headers[i] for i in [1] + list(range(4, 13))]]
    for i in data[:]:
        kp_finalized.append([i[j] for j in [1, 4, 5, 7, 9, 11, 13, 15, 17, 19]])

    kenpom_df = pd.DataFrame(kp_finalized[1:], columns = kp_finalized[0])

    kenpom_df["Team"] = kenpom_df["Team"].str.replace(r"\d+", "", regex = True)
    kenpom_df = (
        kenpom_df
        .assign(Team = lambda x: x["Team"].str.replace(r"\d+", "", regex = True))
        .assign(Team = lambda x: x["Team"].str.replace("\\*", "", regex = True))
    )

    kenpom_df = (
        kenpom_df
            .assign(
                **{
                    i: lambda x, col = i: pd.to_numeric(
                        x[col].str.replace("\\+", "", regex = True
                    ))
                    for i in kenpom_df.columns[1:]
                }
            )
        )

    return(kenpom_df)


def kenpom_speller(input_df: pd.DataFrame, spelling_df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes kenpom data and applies kaggle team names, allowing for easy joins to kaggle data.

    Args:
        input_df(pd.DataFrame): Cleaned kenpom data frame.
        spelling_df(pd.DataFrame): Team spelling data frame from kaggle.

    Returns:
        pd.DataFrame: Cleaned kenpom data frame with team spellings aligning to kaggle data.
    """

    kenpom_df = (
        input_df
        .assign(Team = lambda x: x["Team"].str.lower())
        .merge(
            spelling_df,
            how = "left",
            left_on = "Team",
            right_on = "TeamNameSpelling"
        )
        .assign(TeamID = lambda x: np.where(
            x["Team"] == "texas a&m corpus chris", 1394, np.where(
                x["Team"] == "illinois chicago", 1227, np.where(
                    x["Team"] == "southeast missouri", 1369, np.where(
                        x["Team"] == "queens", 1474, np.where(
                            x["Team"] == "ut rio grande valley", 1410, np.where(
                                x["Team"] == "cal st. bakersfield", 1167, np.where(
                                    x["Team"] == "bethune cookman", 1126, np.where(
                                        x["Team"] == "tarleton st.", 1470, np.where(
                                            x["Team"] == "tennessee martin", 1404, np.where(
                                                x["Team"] == "saint francis", 1384, np.where(
                                                    x["Team"] == "louisiana monroe", 1419, np.where(
                                                        x["Team"] == "arkansas pine bluff", 1115, np.where(
                                                            x["Team"] == "mississippi valley st.", 1290, np.where(
                                                                x["Team"] == "arkansas little rock", 1114, np.where(
                                                                    x["Team"] == "louisiana lafayette", 1418, np.where(
                                                                        x["Team"] == "southwest missouri st.", 1283, np.where(
                                                                            x["Team"] == "texas pan american", 1410, np.where(
                                                                                x["Team"] == "southwest texas st.", 1402, np.where(
                                                                                    x["Team"] == "st. francis ny", 1383, np.where(
                                                                                        x["Team"] == "southeast missouri st.", 1369, np.where(
                                                                                            x["Team"] == "st. francis pa", 1384, np.where(
                                                                                                x["Team"] == "winston salem st.", 1445, np.where(
                                                                                                    x["Team"] == "dixie st.", 1469, np.where(
                                                                                                        x["Team"] == "texas a&m commerce", 1477, x["TeamID"]
                                                                                                    )
                                                                                                )
                                                                                            )
                                                                                        )
                                                                                    )
                                                                                )
                                                                            )
                                                                        )
                                                                    )
                                                                )
                                                            )
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        ))
        .assign(TeamID = lambda x: x["TeamID"].astype("int"))
    )

    if len(kenpom_df.query("TeamID.isna()")["TeamID"]) > 0:
        log.error(f"Missing team ID's for following kenpom teams: {kenpom_df.query("TeamID.isna()")["TeamNameSpelling"]}")
        kenpom_df = None
    else:
        kenpom_df = (
            kenpom_df
            .drop(columns = ["Team", "TeamNameSpelling"])
        )

    return(kenpom_df)
