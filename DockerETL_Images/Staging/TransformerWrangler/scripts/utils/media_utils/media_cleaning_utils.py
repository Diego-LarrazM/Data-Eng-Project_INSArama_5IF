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

    def clean_play_method(play_method: str | None) -> str | None:
        if not isinstance(play_method, str) or play_method == "\\N":
            return None

        cleaned = play_method.strip()
        cleaned = cleaned.strip("[]")

        cleaned = cleaned.replace('"', "").replace("'", "")

        cleaned = re.sub(r"\s*,\s*", ", ", cleaned)

        return cleaned.strip() if cleaned else None

    def normalize_role(role: str | None) -> str | None:
        if not isinstance(role, str) and role is None:
            return None

        role = role.lower().strip()

        ROLE_MAP = {"actress": "actor"}

        return ROLE_MAP.get(role, role)
