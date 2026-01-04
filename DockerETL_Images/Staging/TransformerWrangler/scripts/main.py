import os
from urllib.parse import quote_plus
from pathlib import Path
import json

from utils.mongo_loader import MongoLoader
from utils.media_utils import *
from media_builder import MediaBuilder

HOST = os.environ.get("MONGO_HOST_NAME")
PORT = int(os.environ.get("MONGO_PORT") or 0)
USERNAME = os.environ.get("MONGO_USERNAME")
PASSWORD = os.environ.get("MONGO_PASSWORD")
MONGO_DB = os.environ.get("MONGO_DB")

credentials = ""
if USERNAME and PASSWORD:
    credentials = f"{quote_plus(USERNAME)}:{quote_plus(PASSWORD)}@"
mongo_url = f"mongodb://{credentials}{HOST}:{PORT}/"

METACRITIC_SOURCE_DIR = Path(
    os.environ.get("METACRITIC_DATA_FILE_DIRECTORY") or "./data/metacritic"
)
IMDB_SOURCE_DIR = Path(os.environ.get("IMDB_DATA_FILE_DIRECTORY") or "./data/imdb")
OUTPUT_DIR = Path(os.environ.get("OUT_DATA_FILE_DIRECTORY") or "./data/output")

COLLECTIONS = [  # ORDER MATTERS WITH RELATIONSHIPS !
    # Bridged Entities
    "COMPANIES",
    "GENRES",
    "ROLES",
    # Dimensions
    "DIM_MEDIA_INFO",
    "DIM_SECTION",
    "DIM_REVIEWER",
    "DIM_TIME",
    # Bridges
    "BRIDGE_MEDIA_ROLE",
    "BRIDGE_MEDIA_GENRE",
    "BRIDGE_MEDIA_COMPANY",
    # Fact
    "FACT_REVIEWS",
]


def setup_metacritic_data():
    # Merging utilities
    title_year_set = set()

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

        print(f"[ Loading media data from: <{cat_dir.name}>... ]")

        media_type = "Movie"
        section_type = "Display"
        if cat_dir.name == "GAMES":
            section_type = "Platform"
            media_type = "Video Game"
        elif cat_dir.name == "TV_SHOWS":
            section_type = "Season"
            media_type = "TV Series"

        for jf in cat_dir.glob("*.json"):
            # Openning source json
            with open(jf, "r", encoding="utf-8") as f:
                data = json.load(f)

            to_add_media_info_row, to_add_genres, to_add_companies = (
                MediaBuilder.build_mediainfo_rows(data, media_type, title_year_set)
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

    # Remapping
    print(f"[ Remapping Metacritic Data ]")
    genre_rows = MediaMappingUtils.remap_foreign_keys_and_build_distinct_rows(
        media_rows, genre_connection, "genre_id"
    )
    del genre_connection
    print("-> Mapped Genres")

    company_rows = MediaMappingUtils.remap_foreign_keys_and_build_distinct_rows(
        media_rows, company_connection, "company_id"
    )
    del company_connection
    print("-> Mapped Companies")

    time_rows = MediaMappingUtils.remap_foreign_keys_and_build_distinct_rows(
        review_rows, time_connection, "time_id"
    )
    del time_connection
    print("-> Mapped Timestamps")

    reviewer_rows = MediaMappingUtils.remap_foreign_keys_and_build_distinct_rows(
        review_rows, reviewer_connection, "reviewer_id"
    )
    del reviewer_connection
    print("-> Mapped Reviewers")

    section_rows = MediaMappingUtils.remap_foreign_keys_and_build_distinct_rows(
        review_rows, section_connection, "section_id"
    )
    del section_connection
    print("-> Mapped Sections")

    print("[ Saving to CSV... ]")
    MediaBuilder.build_and_save_dataframe_from_rows(
        genre_rows, OUTPUT_DIR / "GENRES.csv"
    )
    del genre_rows

    MediaBuilder.build_and_save_dataframe_from_rows(
        company_rows, OUTPUT_DIR / "COMPANIES.csv"
    )
    del company_rows

    reviews_df = MediaBuilder.build_and_save_dataframe_from_rows(
        review_rows, is_dict=True
    )
    del review_rows
    reviews_df = reviews_df.dropna(subset=["rating"])
    reviews_df.to_csv(OUTPUT_DIR / "FACT_REVIEWS.csv", sep="|", encoding="utf-8")
    del reviews_df

    MediaBuilder.build_and_save_dataframe_from_rows(
        time_rows, OUTPUT_DIR / "DIM_TIME.csv"
    )
    del time_rows

    MediaBuilder.build_and_save_dataframe_from_rows(
        reviewer_rows, OUTPUT_DIR / "DIM_REVIEWER.csv"
    )
    del reviewer_rows

    print("[ Extracting Franchise for Sections ... ]")
    section_df = MediaBuilder.build_and_save_dataframe_from_rows(section_rows)
    del section_rows
    section_df = MediaTokenUtils.cluster_attribute_jaccard(
        section_df,
        "section_name",
        "section_group",
        type_attribute="section_type",
        blacklist_types=["Season", "Display"],
    )
    section_df.to_csv(OUTPUT_DIR / "DIM_SECTION.csv", sep="|", encoding="utf-8")

    print("< Finished with Metacritic >")

    return media_rows, title_year_set


def setup_and_join_imdb_data_for_roles(media_rows, title_year_set):
    # IMDB Joining for roles extraction
    imdb_title_mapping = MediaBuilder.build_imdb_tconst_lookup_by_primary_title(
        media_rows=media_rows,
        title_basics_path=IMDB_SOURCE_DIR / "title.basics.tsv.gz",
        title_year_set=title_year_set,
    )

    role_connection = MediaBuilder.build_roles_for_media(
        imdb_matches=imdb_title_mapping, imdb_dir=IMDB_SOURCE_DIR
    )
    del imdb_title_mapping

    role_rows = MediaMappingUtils.remap_foreign_keys_and_build_distinct_rows(
        media_rows, role_connection, "role_id"
    )
    del role_connection

    role_df = MediaBuilder.build_and_save_dataframe_from_rows(role_rows)
    role_df.drop(columns=["nconst"])
    role_df.to_csv(OUTPUT_DIR / "ROLES.csv", sep="|", encoding="utf-8")
    del role_rows


def setup_bridges(media_rows):
    print(f"[ Setting up Bridge Tables... ]")
    # Build bridge tables for media_info
    bridge_dfs_media_info = MediaBuilder.build_bridge_rows(
        media_rows, ["genre_id", "company_id", "role_id"], "media_id"
    )

    MediaBuilder.build_and_save_dataframe_from_rows(
        bridge_dfs_media_info["genre_id"],
        OUTPUT_DIR / "BRIDGE_MEDIA_GENRE.csv",
        id_attribute_names=["media_id", "genre_id"],
    )
    del bridge_dfs_media_info["genre_id"]

    MediaBuilder.build_and_save_dataframe_from_rows(
        bridge_dfs_media_info["company_id"],
        OUTPUT_DIR / "BRIDGE_MEDIA_COMPANY.csv",
        id_attribute_names=["media_id", "company_id"],
    )
    del bridge_dfs_media_info["company_id"]

    MediaBuilder.build_and_save_dataframe_from_rows(
        bridge_dfs_media_info["role_id"],
        OUTPUT_DIR / "BRIDGE_MEDIA_ROLE.csv",
        id_attribute_names=["media_id", "role_id"],
    )
    del bridge_dfs_media_info


if __name__ == "__main__":

    media_rows, title_year_set = setup_metacritic_data()

    setup_and_join_imdb_data_for_roles(media_rows, title_year_set)
    del title_year_set

    setup_bridges(media_rows)

    # DataFrames and CSVs
    print(f"[ Extracting Franchises... ]")
    media_df = MediaBuilder.build_and_save_dataframe_from_rows(media_rows, is_dict=True)
    media_df = MediaTokenUtils.cluster_attribute_jaccard(
        media_df, "primary_title", "franchise", type_attribute="media_type"
    )
    media_df.to_csv(OUTPUT_DIR / "DIM_MEDIA_INFO.csv", sep="|", encoding="utf-8")

    # Load to transient database (MongoDB)
    print(f"[ Loading transformed data to transient MongoDB at: <{mongo_url}>... ]")
    loader = MongoLoader(mongo_conn_url=mongo_url, database=MONGO_DB)

    for collection in COLLECTIONS:
        print(f"Loading collection: {collection}...")
        if not loader.load_from_csv(OUTPUT_DIR / f"{collection}.csv", collection):
            raise Exception(f"Failed to load collection: {collection}!")

    print("All collections loaded successfully!")
