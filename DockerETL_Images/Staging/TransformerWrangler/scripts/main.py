import os
from urllib.parse import quote_plus
from pathlib import Path
import json

from utils.mongo_loader import MongoLoader
from utils.media_utils import *
from media_builder import MediaBuilder

HOST = os.environ.get("MONGO_HOST_NAME")
PORT = int(os.environ.get("MONGO_PORT"))
USERNAME = os.environ.get("MONGO_USERNAME")
PASSWORD = os.environ.get("MONGO_PASSWORD")
MONGO_DB = os.environ.get("MONGO_DB")

credentials = ""
if USERNAME and PASSWORD:
    credentials = f"{quote_plus(USERNAME)}:{quote_plus(PASSWORD)}@"
mongo_url = f"mongodb://{credentials}{HOST}:{PORT}/"

METACRITIC_SOURCE_DIR = Path(os.environ.get("METACRITIC_DATA_FILE_DIRECTORY"))
IMDB_SOURCE_DIR = Path(os.environ.get("IMDB_DATA_FILE_DIRECTORY"))
OUTPUT_DIR = Path(os.environ.get("OUT_DATA_FILE_DIRECTORY"))

COLLECTIONS = [  # ORDER MATTERS WITH RELATIONSHIPS !
    # Bridged Entities
    "COMPANIES",
    "GENRES",
    "ROLES",
    # Dimensions
    "DIM_FRANCHISE",
    "DIM_MEDIA_INFO",
    "DIM_PLATFORM",
    "DIM_REVIEWER",
    "DIM_TIME",
    # Bridges
    "BRIDGE_MEDIA_ROLE",
    "BRIDGE_MEDIA_GENRE",
    "BRIDGE_MEDIA_COMPANY",
    # Fact
    "FACT_REVIEWS",
]

if __name__ == "__main__":

    # Transform
    # Merging utilities
    merging_utilities = {"year_to_titles": set()}

    # Sets of distinct value : [list of uuids that use this value as a dim]
    genre_connection = {}
    company_connection = {}
    time_connection = {}
    reviewer_connection = {}
    section_connection = {}
    role_connection = {}

    # All by default distinct rows
    media_rows = {}
    review_rows = {}

    for cat_dir in METACRITIC_SOURCE_DIR.iterdir():
        if not cat_dir.is_dir():
            continue
        media_type = "Movie"
        section_type = "Display"
        if cat_dir.name == "games":
            section_type = "Platform"
            media_type = "Video Game"
        elif cat_dir.name == "tvshows":
            section_type = "Season"
            media_type = "TV Series"

        for jf in cat_dir.glob("*.json"):
            # Openning source json
            with open(jf, "r", encoding="utf-8") as f:
                data = json.load(f)

            to_add_media_info_row, to_add_genres, to_add_companies = (
                MediaBuilder.build_mediainfo_rows(
                    data, media_type, merging_utilities["year_to_titles"]
                )
            )
            media_info_id = list(to_add_media_info_row.keys())[0]
            to_add_review_rows, to_add_timestamps, to_add_reviewers, to_add_sections = (
                MediaBuilder.build_fact_rows(data, media_info_id, section_type)
            )

            # Add new already known distinct rows
            media_rows |= to_add_media_info_row
            review_rows |= to_add_review_rows

            # Map new distinct values or add to already defined new rows that reference them to replace uuid later
            MediaMappingUtils.map_distinct_values(
                to_add_genres, genre_connection, ["genre_title"]
            )
            MediaMappingUtils.map_distinct_values(
                to_add_companies, company_connection, ["company_name", "company_role"]
            )
            MediaMappingUtils.map_distinct_values(
                to_add_timestamps, time_connection, ["year", "month", "day"]
            )
            MediaMappingUtils.map_distinct_values(
                to_add_reviewers,
                reviewer_connection,
                ["reviewer_username", "association"],
            )
            MediaMappingUtils.map_distinct_values(
                to_add_sections, section_connection, ["section_name", "section_type"]
            )

    # IMDB Joining for roles extraction
    imdb_title_mapping = MediaBuilder.build_imdb_tconst_lookup_by_primary_title(
        media_rows=media_rows,
        imdb_dir=IMDB_SOURCE_DIR,
        title_basics_path=IMDB_SOURCE_DIR / "title.basics.tsv.gz",
    )

    role_connection = MediaBuilder.build_roles_for_media(
        imdb_matches=imdb_title_mapping, imdb_dir=IMDB_SOURCE_DIR
    )

    # Remapping
    genre_rows = MediaMappingUtils.remap_foreign_keys_and_build_distinct_rows(
        media_rows, genre_connection, "genre_id"
    )
    company_rows = MediaMappingUtils.remap_foreign_keys_and_build_distinct_rows(
        media_rows, company_connection, "company_id"
    )
    time_rows = MediaMappingUtils.remap_foreign_keys_and_build_distinct_rows(
        review_rows, time_connection, "time_id"
    )
    reviewer_rows = MediaMappingUtils.remap_foreign_keys_and_build_distinct_rows(
        review_rows, reviewer_connection, "reviewer_id"
    )
    section_rows = MediaMappingUtils.remap_foreign_keys_and_build_distinct_rows(
        review_rows, section_connection, "section_id"
    )
    role_rows = MediaMappingUtils.remap_foreign_keys_and_build_distinct_rows(
        media_rows, role_connection, "role_id"
    )

    # Build bridge tables for media_info
    bridge_dfs_media_info = MediaBuilder.build_bridge_rows(
        media_rows, ["genre_id", "company_id", "role_id"], "media_id"
    )
    bridge_media_genre_rows = bridge_dfs_media_info["genre_id"]
    bridge_media_company_rows = bridge_dfs_media_info["company_id"]
    bridge_media_role_rows = bridge_dfs_media_info["role_id"]

    # DataFrames and CSVs
    media_df = MediaBuilder.build_and_save_dataframe_from_rows(media_rows, is_dict=True)
    media_df = MediaTokenUtils.cluster_attribute_jaccard(
        media_df, "primary_title", "franchise", type_attribute="media_type"
    )
    media_df.to_csv(OUTPUT_DIR / "media_info.csv", sep="|", encoding="utf-8")

    MediaBuilder.build_and_save_dataframe_from_rows(
        bridge_media_genre_rows,
        "bridge_media_genre.csv",
        id_attribute_names=["media_id", "genre_id"],
    )

    MediaBuilder.build_and_save_dataframe_from_rows(
        bridge_media_company_rows,
        "bridge_media_company.csv",
        id_attribute_names=["media_id", "company_id"],
    )

    MediaBuilder.build_and_save_dataframe_from_rows(
        bridge_media_role_rows,
        "bridge_media_role.csv",
        id_attribute_names=["media_id", "role_id"],
    )

    MediaBuilder.build_and_save_dataframe_from_rows(genre_rows, "genres.csv")

    MediaBuilder.build_and_save_dataframe_from_rows(company_rows, "companies.csv")

    MediaBuilder.build_and_save_dataframe_from_rows(role_rows, "roles.csv")

    MediaBuilder.build_and_save_dataframe_from_rows(
        review_rows, "reviews.csv", is_dict=True
    )

    MediaBuilder.build_and_save_dataframe_from_rows(time_rows, "time.csv")

    MediaBuilder.build_and_save_dataframe_from_rows(reviewer_rows, "reviewers.csv")

    section_df = MediaBuilder.build_and_save_dataframe_from_rows(section_rows)
    section_df = MediaTokenUtils.cluster_attribute_jaccard(
        section_df,
        "section_name",
        "section_group",
        type_attribute="section_type",
        blacklist_types=["Season", "Display"],
    )
    section_df.to_csv(OUTPUT_DIR / "sections.csv", sep="|", encoding="utf-8")

    # Load to transient database (MongoDB)
    print(f"[ Loading transformed data to transient MongoDB at: <{mongo_url}>... ]")
    loader = MongoLoader(mongo_conn_url=mongo_url, database=MONGO_DB)

    for collection in COLLECTIONS:
        print(f"Loading collection: {collection}...")
        if not loader.load_from_csv(f"{OUTPUT_DIR}/{collection}.csv", collection):
            raise Exception(f"Failed to load collection: {collection}!")

    print("All collections loaded successfully!")
