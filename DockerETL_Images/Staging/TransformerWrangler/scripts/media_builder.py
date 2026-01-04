from pathlib import Path
import pandas as pd
import uuid

from utils.media_utils import *


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

        release_date_str = md.get("initial_release_date") or md.get("release_date")
        if release_date_str:
            release_year = MediaExtractUtils.extract_year_from_release_date(
                release_date_str
            )
            for delta in range(-1, 2):
                year_to_titles.add(release_year + delta)

        return (
            {
                media_info_id: {
                    "primary_title": MediaExtractUtils.extract_title(data),
                    "media_type": media_type,
                    "duration": MediaExtractUtils.extract_runtime_minutes(
                        md.get("duration")
                    ),
                    "description": md.get("summary"),
                    "release_date": release_date_str,
                    "pegi_mpa_rating": md.get("rating"),
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

    def build_media_targets(media_rows, year_interval=1):
        targets_data = []
        for id, row in media_rows.items():
            if row["media_type"] == "Video Game":
                continue

            year = MediaExtractUtils.extract_year_from_release_date(row["release_date"])
            if year is None:
                continue  # On ne peut pas matcher sans année

            # Pour tenir en compte de l'intervalle des années on ajoute une ligne par possible année de jointure, filtrée après.
            for y in range(int(year) - year_interval, int(year) + year_interval + 1):
                targets_data.append(
                    {
                        "target_id": id,
                        "target_title": row["primary_title"],
                        "target_year": year,
                        "join_year": y,
                        "target_runtime": row["duration"],
                    }
                )

        if len(targets_data) <= 0:
            return None
        return pd.DataFrame(targets_data)

    def filter_year_equivalent_candidates(chunk_df, targets_df):
        year_equivalent_targets = pd.merge(
            chunk_df, targets_df, left_on="startYear", right_on="join_year", how="inner"
        )

        if year_equivalent_targets.empty:
            return year_equivalent_targets

        # Turn to num minutes, even nulls
        year_equivalent_targets["runtimeMinutes"] = pd.to_numeric(
            year_equivalent_targets["runtimeMinutes"], errors="coerce"
        ).fillna(0)
        year_equivalent_targets["target_runtime"] = year_equivalent_targets[
            "target_runtime"
        ].fillna(0)
        return year_equivalent_targets

    def filter_runtime_equivalent_targets(targets_df, runtime_minutes_interval=2):
        runtime_equivalence_mask = (
            targets_df["target_runtime"] - targets_df["runtimeMinutes"]
        ).abs() <= 2
        unknown_mask = (targets_df["target_runtime"] == 0) | (
            targets_df["runtimeMinutes"] == 0
        )  # in case one of them was None

        runtime_equivalent_targets = targets_df[runtime_equivalence_mask | unknown_mask]
        return runtime_equivalent_targets

    def map_best_candidate_to_target_title(candidates, result, best_found):

        # candidate: row of imdb merged with possible metacritic_title and similarity
        # result: tconst -> metacritic_title
        # best_similarity_found: metacritic_title_id -> [max_similarity_found_for_him, tconst]

        for row in candidates.itertuples():
            imdb_ttid = row.tconst
            candidate_id = row.target_id
            similarity = row.similarity
            if candidate_id not in best_found:
                print(
                    f"King of the Hill: {row.target_title} -> {row.primaryTitle} ({similarity})"
                )
                best_found[candidate_id] = {
                    "similarity": similarity,
                    "associated": imdb_ttid,
                }
                result[imdb_ttid] = candidate_id
            # Check if better match than prior (King of the Hill)
            elif best_found[candidate_id]["similarity"] < similarity:
                print(
                    f"New King of the Hill: {row.target_title} -> {row.primaryTitle} ({similarity})"
                )
                del result[
                    best_found[candidate_id]["associated"]
                ]  # takeout prior worse match
                best_found[candidate_id] = {
                    "similarity": similarity,
                    "associated": imdb_ttid,
                }
                result[imdb_ttid] = candidate_id

    def build_imdb_tconst_lookup_by_primary_title(
        media_rows: dict,
        title_basics_path: Path,
        title_year_set: set,
        chunksize: int = 5_000_000,
    ) -> dict:

        print(f"[ Starting Join: finding common titles ... ]")
        result = {}
        best_similarity_found = {}
        targets_df = MediaBuilder.build_media_targets(media_rows)

        if targets_df is None:
            return {}

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
                chunk_df, year_to_title=title_year_set
            )
            print(f"CLEANED : {chunksize - chunk_df.shape[0]}")

            candidates = MediaBuilder.filter_year_equivalent_candidates(
                chunk_df, targets_df
            )

            if candidates.empty:
                print(f"None found in same year")
                continue

            candidates = MediaBuilder.filter_runtime_equivalent_targets(candidates)
            if candidates.empty:
                print(f"None found for same runtime")
                continue

            # Recherche des ressamblants par jaccard parmis les candidats
            candidates["similarity"] = candidates.apply(
                lambda row: MediaTokenUtils.jaccard_title_similarity(
                    row["primaryTitle"], row["target_title"]
                ),
                axis=1,
            )
            candidates = candidates[candidates["similarity"] >= 0.85]
            print(f"MATCHES : {candidates.shape[0]}")

            MediaBuilder.map_best_candidate_to_target_title(
                candidates, result, best_similarity_found
            )

            print(f"Scanned {chunk_idx * chunksize:,} IMDb rows...")

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

                role = None
                if row.characters != "\\N":
                    role = MediaCleaningUtils.clean_role(row.characters)
                elif row.job != "\\N":
                    role = row.job

                play_method = MediaCleaningUtils.normalize_play_method(row.category)

                MediaMappingUtils.map_distinct_value(
                    {
                        "ref_id": media_id,
                        "nconst": row.nconst,
                        "play_method": play_method,
                        "role": role,
                    },
                    role_connection,
                    pk_attributes=["nconst", "role", "play_method"],
                )
                required_nconsts.add(row.nconst)

            if chunk_idx % 5 == 0:
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

            print(f"Scanned {chunk_idx * chunksize:,} IMDb NAMES rows...")

            for role_attributes in role_connection.values():
                primary_name = nconst_to_name.get(role_attributes["nconst"])
                if not primary_name:
                    continue

                role_attributes["person_name"] = primary_name

                if role_attributes.get("role") == "self":
                    role_attributes["play_method"] = primary_name

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
