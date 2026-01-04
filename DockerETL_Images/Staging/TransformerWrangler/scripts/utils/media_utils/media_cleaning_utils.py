import pandas as pd
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

    def clean_role(role: str | None) -> str | None:
        if not isinstance(role, str) or role == "\\N":
            return None
        print(role)
        cleaned = cleaned.strip("[]")
        cleaned = re.sub(r"[\"\']", "", cleaned)
        # cleaned = re.sub(r"\s*,\s*", ", ", cleaned)
        print("after: ", cleaned.strip())
        return cleaned.strip() if cleaned else None

    def normalize_play_method(play_method: str | None) -> str | None:
        if not isinstance(play_method, str) and play_method is None:
            return None

        play_method = play_method.lower().strip()

        PLAY_METHOD_MAP = {"actress": "actor"}

        return PLAY_METHOD_MAP.get(play_method, play_method)

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

        runtime_equivalent_targets = targets_df[runtime_equivalence_mask]
        return runtime_equivalent_targets
