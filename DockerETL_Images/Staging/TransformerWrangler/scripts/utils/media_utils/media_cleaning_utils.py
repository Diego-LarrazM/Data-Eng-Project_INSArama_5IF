import pandas as pd
import re


class MediaCleaningUtils:

    def IMDB_clean_filter(df: pd.DataFrame, year_to_title: set) -> pd.DataFrame:

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
        cleaned = role.strip()
        cleaned = cleaned.strip("[]")

        cleaned = cleaned.replace('"', "").replace("'", "")
        # cleaned = re.sub(r"[^\w^\s]", "", cleaned)

        cleaned = re.sub(r"\s*,\s*", ", ", cleaned)
        # cleaned = re.sub(r"\s+", " ", cleaned)
        print("after: ", cleaned.strip())
        return cleaned.strip() if cleaned else None

    def normalize_play_method(play_method: str | None) -> str | None:
        if not isinstance(play_method, str) and play_method is None:
            return None

        play_method = play_method.lower().strip()

        PLAY_METHOD_MAP = {"actress": "actor"}

        return PLAY_METHOD_MAP.get(play_method, play_method)
