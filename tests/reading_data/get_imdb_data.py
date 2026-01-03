import pandas as pd
from pathlib import Path
import uuid
import re


def map_distinct_value(new_row_to_add, distinct_value_set, pk_attributes):
    # new_row[pk_attributes] is the string_key that mixes all differenciating primary key values
    ref_id = new_row_to_add["ref_id"]
    del new_row_to_add["ref_id"]
    # create a key so that for each pk_att: key = "lower(new_data[pk_att[0]])lower(new_data[pk_att[1]])..."
    distinct_pk_key = "".join(
        str(new_row_to_add[pk_att]).lower() for pk_att in pk_attributes
    )
    if distinct_pk_key in distinct_value_set:
        distinct_value_set[distinct_pk_key]["refs"].append(ref_id)
    else:
        distinct_value_set[distinct_pk_key] = {
            "id": uuid.uuid4(),
            "refs": [ref_id],
        } | new_row_to_add


def tokenize_title(title: str) -> set[str] | None:
    if not title and not isinstance(title, str):
        return None
    title = title.lower()
    # Remove startYear (1900–2099)
    title = re.sub(r"\b(19|20)\d{2}\b", "", title)

    # Remove empty brackets left after year removal
    title = re.sub(r"[\(\[\{]\s*[\)\]\}]", "", title)

    # Remove extra punctuation
    title = re.sub(r"\s*[:;,\.\-–—]\s+", "", title)

    # Collapse multiple spaces
    title = re.sub(r"\s+", " ", title)

    # Strip leading/trailing spaces
    title = title.strip()

    # Erase stopwords
    tokens = set(title.split()) - {"the", "a", "and", "as"}

    return tokens


def extract_year_from_release_date(release_date: str | None) -> int | None:
    if not isinstance(release_date, str):
        return None
    try:
        return int(release_date.strip()[-4:])
    except ValueError:
        return None


def jaccard_similarity(a: str, b: str) -> float:
    set_a = tokenize_title(a)
    set_b = tokenize_title(b)

    if not set_a and not set_b:
        return 1.0
    if not set_a or not set_b:
        return 0.0

    return len(set_a & set_b) / len(set_a | set_b)


def is_subset_false_positive(meta_title: str, imdb_title: str) -> bool:
    meta_tokens = tokenize_title(meta_title)
    imdb_tokens = tokenize_title(imdb_title)

    if not meta_tokens or not imdb_tokens:
        return False

    if imdb_tokens.issubset(meta_tokens):
        missing = meta_tokens - imdb_tokens
        return len(missing) >= 1

    return False


def could_be_same_media(
    expected_year: int,
    imdb_year: int,
    expected_runtime: int,
    imdb_runtime: str,
    acceptance_year_interval: int,
    acceptance_runtime_interval: int,
) -> bool:
    year_equivalence = False
    if imdb_year != "\\N" and expected_year is not None and imdb_year is not None:
        year_equivalence = (
            abs(expected_year - int(imdb_year)) <= acceptance_year_interval
        )

    runtime_equivalence = True
    if (
        imdb_runtime != "\\N"
        and expected_runtime is not None
        and imdb_runtime is not None
    ):
        runtime_equivalence = (
            abs(expected_runtime - int(imdb_runtime)) <= acceptance_runtime_interval
        )

    return runtime_equivalence and year_equivalence


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


def extract_runtime_minutes(runtime_str: str | None) -> int | None:
    if not runtime_str or runtime_str == "\\N":
        return None
    try:  # "duration": "X h YZ m",
        mins = 0
        h_split = runtime_str.split("h")
        minsplit = h_split[1].split("m")
        mins += int(h_split[0].strip()) * 60
        mins += int(minsplit[0].strip())
        return mins
    except ValueError:
        return None


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


def build_imdb_tconst_lookup_by_primary_title(
    media_rows: dict,
    title_basics_path: Path,
    chunksize: int = 500_000,
    year_interval=1,
    runtime_minutes_interval=2,
) -> dict:

    targets = {}
    year_to_title = set()
    for media_id, row in media_rows.items():
        if row.get("media_type") not in {"Movie", "TV Series"}:
            continue

        title = row.get("primary_title")
        year = extract_year_from_release_date(row.get("release_date"))

        if not isinstance(title, str) or year is None:
            continue

        targets[media_id] = {
            "title": title,
            "year": year,
            "duration": row.get("duration"),
            "runtime_minutes": extract_runtime_minutes(row.get("duration")),
        }

        year_to_title.add(year)

    remaining_ids = set(targets.keys())
    result = {}

    print(
        f"Looking for {len(remaining_ids)} titles "
        f"across {len(year_to_title)} candidate years...\n"
    )

    for chunk_idx, chunk_df in enumerate(
        pd.read_csv(
            title_basics_path,
            sep="\t",
            compression="gzip",
            chunksize=chunksize,
            usecols=[
                "tconst",
                "primaryTitle",
                "startYear",
                "titleType",
                "runtimeMinutes",
            ],
            dtype=str,
        ),
        start=1,
    ):

        chunk_df = IMDB_clean_filter(chunk_df, year_to_title=year_to_title)

        # remove remaining ids for production !!
        for media_id in remaining_ids.copy():
            tgt = targets[media_id]
            meta_title = tgt["title"]
            meta_year = tgt["year"]
            meta_duration = tgt["duration"]
            meta_runtime_minutes = tgt["runtime_minutes"]

            chunk_df["similarity"] = chunk_df[
                ["primaryTitle", "startYear", "runtimeMinutes"]
            ].apply(
                lambda x: (
                    jaccard_similarity(x["primaryTitle"], meta_title)
                    if could_be_same_media(
                        meta_year,
                        x["startYear"],
                        meta_runtime_minutes,
                        x["runtimeMinutes"],
                        year_interval,
                        runtime_minutes_interval,
                    )
                    else 0.0
                ),
                axis=1,
            )

            candidates = chunk_df[(chunk_df["similarity"] >= 0.50)]

            if not candidates.empty:
                row = candidates.sort_values("similarity", ascending=False).iloc[0]

                result[row["tconst"]] = media_id
                # Add/change all mediaInfo relevant info here for media_id row, more optimized we we ennumerate to have index and do media_rows.iloc
                remaining_ids.remove(media_id)

                print(
                    f"{tgt['title']} ({tokenize_title(tgt['title'])}): {meta_year} {meta_duration} == {meta_runtime_minutes} min"
                    f"-> {row['similarity']} {row['tconst']}: "
                    f"{row['primaryTitle']} ({tokenize_title(row['primaryTitle'])}) ({row['startYear']} : {int(row['runtimeMinutes'])//60 if row['runtimeMinutes'] != "\\N" else "X"} H {int(row['runtimeMinutes']) % 60 if row['runtimeMinutes'] != "\\N" else "X"} min)"
                )
        if not remaining_ids:
            print("\nAll titles found. Stopping IMDb scan.")
            break

        if chunk_idx % 5 == 0:
            print(f"Scanned {chunk_idx * chunksize:,} IMDb rows...")

    if remaining_ids:
        print(f"\nNOT FOUND ({len(remaining_ids)}):")
        for media_id in remaining_ids:
            print(" -", targets[media_id]["title"])
    else:
        print("\nAll titles resolved")

    return result


def extract_roles_for_media(
    media_rows: dict,
    imdb_dir: Path,
    title_basics_path: Path,
    chunksize: int = 500_000,
) -> pd.DataFrame:
    print("\n[IMDb] Extracting roles for media")

    imdb_matches = build_imdb_tconst_lookup_by_primary_title(
        media_rows=media_rows,
        title_basics_path=title_basics_path,
        chunksize=chunksize,
    )

    role_connection = {}
    required_nconsts = set()

    principals_path = imdb_dir / "title.principals.tsv.gz"

    print("[IMDb] Scanning title.principals.tsv.gz")

    for chunk_idx, chunk in enumerate(
        pd.read_csv(
            principals_path,
            sep="\t",
            compression="gzip",
            dtype=str,
            usecols=["tconst", "nconst", "category", "job", "characters"],
            chunksize=chunksize,
        ),
        start=1,
    ):
        chunk = chunk[chunk["tconst"].isin(imdb_matches)]
        if chunk.empty:
            continue

        for row in chunk.itertuples(index=False):
            media_id = imdb_matches[row.tconst]

            play_method = None
            if row.characters != "\\N":
                play_method = clean_play_method(row.characters)
            elif row.job != "\\N":
                play_method = row.job

            role = normalize_role(row.category)

            map_distinct_value(
                {
                    "ref_id": media_id,
                    "nconst": row.nconst,
                    "role": role,
                    "play_method": play_method,
                },
                role_connection,
                pk_attributes=["nconst", "role", "play_method"],
            )
            required_nconsts.add(row.nconst)

        if chunk_idx % 10 == 0:
            print(f"Scanned {chunk_idx * chunksize:,} IMDb CHARACTERS rows...")

    nconst_to_name = {}

    names_path = imdb_dir / "name.basics.tsv.gz"

    for chunk_idx, chunk in enumerate(
        pd.read_csv(
            names_path,
            sep="\t",
            compression="gzip",
            dtype=str,
            usecols=["nconst", "primaryName"],
            chunksize=chunksize,
        ),
        start=1,
    ):
        chunk = chunk[chunk["nconst"].isin(required_nconsts)]
        if chunk.empty:
            continue

        for row in chunk.itertuples(index=False):
            nconst_to_name[row.nconst] = row.primaryName

        if chunk_idx % 10 == 0:
            print(f"Scanned {chunk_idx * chunksize:,} IMDb NAMES rows...")

        for role_key, role_attributes in role_connection.items():
            primary_name = nconst_to_name.get(role_attributes["nconst"])
            if not primary_name:
                continue

            role_attributes["primaryName"] = primary_name

            if role_attributes.get("role") == "self":
                role_attributes["play_method"] = primary_name

            del role_attributes[role_key]["nconst"]

        role_connection[role_key]["primaryName"] = primary_name

    print("[IMDb] yey we done from reading !!")
    return role_connection
