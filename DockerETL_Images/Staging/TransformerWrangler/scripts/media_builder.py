from pathlib import Path
import pandas as pd
import uuid

from utils.media_extract_utils import MediaExtractUtils
from utils.media_cleaning_utils import MediaCleaningUtils
from utils.media_token_utils import MediaTokenUtils


class MediaBuilder:

    ##################################################################
    # ------< Metacritic >---------------------- ---------------------#
    ##################################################################

    def build_mediainfo_rows(data, media_type, year_to_titles):
        md = data.get("media_details", {})
        media_info_id = uuid.uuid4()
        genre_rows = [
            {"ref_id": media_info_id, "genre_title": g.strip()}
            for g in md.get("genres", [])
        ]
        companies = (
            [("developer", d) for d in md.get("developers", [])]
            + [("publisher", p) for p in md.get("publishers", [])]
            + [
                ("production_companies", pc)
                for pc in md.get("production_companies", [])
            ]
        )
        company_rows = [
            {"ref_id": media_info_id, "company_role": cr, "company_name": cn}
            for cr, cn in companies
        ]

        primary_title = MediaExtractUtils.extract_title(data)
        release_date = md.get("initial_release_date") or md.get("release_date")
        year_to_titles.add(
            MediaExtractUtils.extract_year_from_release_date(release_date)
        )
        return (
            {
                media_info_id: {
                    "primary_title": primary_title,
                    "title_language": md.get("original_language"),
                    "original_title": md.get("original_title"),
                    "media_type": media_type,
                    "duration": md.get("duration"),
                    "description": md.get("summary"),
                    "release_date": md.get("initial_release_date")
                    or md.get("release_date"),
                    "PEGI_MPA_Rating": md.get("rating"),
                    "genre_id": [],
                    "company_id": [],
                    "role_id": [],
                }
            },
            genre_rows,
            company_rows,
        )

    def build_fact_rows(data, mediainfo_id, section_type):
        review_rows = {}
        time_rows = []
        reviewer_rows = []
        section_rows = []

        # ["section1", [review1, review2], "section2", [review3, review4], ...]

        for section, reviews in MediaExtractUtils.extract_all_reviews(data):
            for r in reviews:
                review_id = uuid.uuid4()
                # Append review
                review_rows |= {
                    review_id: {
                        "time_id": None,
                        "section_id": None,
                        "reviewer_id": None,
                        "media_info_id": mediainfo_id,
                        "rating": r.get("rating"),
                    }
                }
                # Append time
                post_date = r.get("post_date")
                if post_date:
                    year, month, day = post_date.split("-")
                else:
                    year, month, day = (None, None, None)
                time_rows.append(
                    {"year": year, "month": month, "day": day, "ref_id": review_id}
                )
                # Append reviewer
                reviewer_rows.append(
                    {
                        "association": r.get("company"),
                        "reviewer_username": r.get("author"),
                        "is_critic": r.get("isCritic"),
                        "ref_id": review_id,
                    }
                )
                # Append platform
                section_rows.append(
                    {
                        "section_type": section_type,
                        "section_name": section,
                        "ref_id": review_id,
                    }
                )
        return review_rows, time_rows, reviewer_rows, section_rows

    ##################################################################
    # ---------< IMDB >-----------------------------------------------#
    ##################################################################

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
            year = MediaExtractUtils.extract_year_from_release_date(
                row.get("release_date")
            )

            if not isinstance(title, str) or year is None:
                continue

            targets[media_id] = {
                "title": title,
                "year": year,
                "duration": row.get("duration"),
                "runtime_minutes": MediaExtractUtils.extract_runtime_minutes(
                    row.get("duration")
                ),
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

            chunk_df = MediaCleaningUtils.IMDB_clean_filter(
                chunk_df, year_to_title=year_to_title
            )

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
                        MediaTokenUtils.jaccard_similarity(
                            x["primaryTitle"], meta_title
                        )
                        if MediaTokenUtils.could_be_same_media(
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
                        f"{tgt['title']} ({MediaTokenUtils.tokenize_title(tgt['title'])}): {meta_year} {meta_duration} == {meta_runtime_minutes} min"
                        f"-> {row['similarity']} {row['tconst']}: "
                        f"{row['primaryTitle']} ({MediaTokenUtils.tokenize_title(row['primaryTitle'])}) ({row['startYear']} : {int(row['runtimeMinutes'])//60 if row['runtimeMinutes'] != "\\N" else "X"} H {int(row['runtimeMinutes']) % 60 if row['runtimeMinutes'] != "\\N" else "X"} min)"
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

    def build_roles_for_media(
        imdb_matches: dict,
        imdb_dir: Path,
        chunksize: int = 500_000,
    ) -> pd.DataFrame:
        print("\n[IMDb] Extracting roles for media")

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
                    play_method = MediaCleaningUtils.clean_play_method(row.characters)
                elif row.job != "\\N":
                    play_method = row.job

                role = MediaCleaningUtils.normalize_role(row.category)

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

    ##################################################################
    # ---------< Generic >--------------------------------------------#
    ##################################################################

    def build_bridge_rows(main_rows, foreign_key_titles, main_title):
        bridge_dfs = {fkt: [] for fkt in foreign_key_titles}
        for main_id, row in main_rows.items():
            for fk_title in foreign_key_titles:
                if len(row[fk_title]) == 0:
                    del row[fk_title]
                    continue
                fk_weight = 1 / len(row[fk_title])
                for fk in row[fk_title]:
                    bridge_dfs[fk_title].append(
                        {main_title: main_id, f"{fk_title}": fk, "weight": fk_weight}
                    )
                del row[fk_title]
        return bridge_dfs

    def build_and_save_dataframe_from_rows(
        rows,
        output_file_dir=None,
        separator="|",
        id_attribute_names=["id"],
        is_dict=False,
    ):
        if is_dict:
            # data = {
            #     idrow1: {"colA": valA, "colB": valB},
            #     idrow2: {"colA": valA2, "colB": valB2},
            # }
            df = pd.DataFrame.from_dict(rows, orient="index")
            df.index.names = id_attribute_names
        else:
            # rows = [
            #     {"id": 101, "age": 30, "score": 88},
            #     {"id": 102, "age": 25, "score": 92},
            # ]
            df = pd.DataFrame.from_records(rows, index=id_attribute_names)
        if output_file_dir:
            df.to_csv(output_file_dir, sep=separator, encoding="utf-8")
        return df
