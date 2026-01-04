import pandas as pd
import numpy as np
import re


class MediaCleaningUtils:

    def IMDB_acceptable_filter(df: pd.DataFrame, year_to_title: set) -> pd.DataFrame:

        df = df.copy()

        df["startYear"] = pd.to_numeric(df["startYear"].str.strip(), errors="coerce")

        valid_title_test = (
            df["primaryTitle"].astype("string").str.strip().replace("\\N", pd.NA).ne("")
        )

        mask = (
            df["titleType"].isin(["movie", "tvSeries", "tvMiniSeries"])
            & df["startYear"].notna()
            & df["startYear"].isin(year_to_title)
            & valid_title_test
            & df["primaryTitle"].str.len().gt(1)
        )

        df = df[mask]

        return df

    def clean_characters(df: pd.DataFrame) -> pd.DataFrame:
        return (
            df["characters"]
            .replace("\\N", pd.NA, regex=False)
            .str.strip("[]")
            .str.replace(r"[\"\']", "", regex=True)
            .str.replace("Self -", "", regex=True)
            .str.strip()
            .replace(["", "Self"], pd.NA, regex=False)
        )

    def clean_job(df: pd.DataFrame) -> pd.DataFrame:
        return df["job"].replace("\\N", pd.NA, regex=False)

    def clean_category(dataframe):
        PLAY_METHOD_MAP = {"actress": "actor"}
        return (
            dataframe["category"]
            .str.lower()
            .str.strip()
            .replace("\\N", pd.NA, regex=False)
            .replace(PLAY_METHOD_MAP, regex=False)
        )

    def filter_year_equivalent_candidates(chunk_df, targets_df):
        year_equivalent_targets = pd.merge(
            chunk_df, targets_df, left_on="startYear", right_on="join_year", how="inner"
        )

        if year_equivalent_targets.empty:
            return year_equivalent_targets

        return year_equivalent_targets

    def filter_runtime_equivalent_targets(targets_df, runtime_minutes_interval=2):
        # Turn to num minutes, even nulls
        targets_df["runtimeMinutes"] = pd.to_numeric(
            targets_df["runtimeMinutes"], errors="coerce"
        ).fillna(0)
        targets_df["target_runtime"] = targets_df["target_runtime"].fillna(0)

        runtime_equivalence_mask = (
            targets_df["target_runtime"] - targets_df["runtimeMinutes"]
        ).abs() <= runtime_minutes_interval
        unknown_mask = (targets_df["target_runtime"] == 0) | (
            targets_df["runtimeMinutes"] == 0
        )  # in case one of them was None

        runtime_equivalent_targets = targets_df[runtime_equivalence_mask | unknown_mask]
        return runtime_equivalent_targets
