import os
from urllib.parse import quote_plus
from pathlib import Path
import json

from utils import MongoLoader
from utils.logger import LOG
from utils.media_utils import *
from media_builder import MediaBuilder

HOST = os.environ.get("MONGO_HOST_NAME")
PORT = int(os.environ.get("MONGO_PORT", -1))
USERNAME = os.environ.get("MONGO_USERNAME")
PASSWORD = os.environ.get("MONGO_PASSWORD")
MONGO_DB = os.environ.get("MONGO_DB")

credentials = ""
if USERNAME and PASSWORD:
    credentials = f"{quote_plus(USERNAME)}:{quote_plus(PASSWORD)}@"
mongo_url = f"mongodb://{credentials}{HOST}:{PORT}/"

METACRITIC_SOURCE_DIR = Path(
    os.environ.get("METACRITIC_DATA_FILE_DIRECTORY", "./data/metacritic")
)
IMDB_SOURCE_DIR = Path(os.environ.get("IMDB_DATA_FILE_DIRECTORY", "./data/imdb"))
OUTPUT_DIR = Path(os.environ.get("OUT_DATA_FILE_DIRECTORY", "./data/output"))

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


def load_to_mongo(dictionary, collection, index_oriented=False):
    LOG.info(f"Loading collection: {collection}...")
    if loader.load_from_dict(
        dictionary,
        collection,
        index_oriented=index_oriented,
        batch_size=10000,
    ):
        raise Exception(f"Failed to load collection: {collection}!")


def setup_metacritic_data(loader: MongoLoader):
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

        LOG.info(f"[ Loading media data from: <{cat_dir.name}>... ]")

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
    LOG.info(f"[ Remapping Metacritic Data ]")
    genre_rows = MediaMappingUtils.remap_foreign_keys_and_build_distinct_rows(
        media_rows, genre_connection, "genre_id"
    )
    del genre_connection
    LOG.info("-> Mapped Genres")

    company_rows = MediaMappingUtils.remap_foreign_keys_and_build_distinct_rows(
        media_rows, company_connection, "company_id"
    )
    del company_connection
    LOG.info("-> Mapped Companies")

    time_rows = MediaMappingUtils.remap_foreign_keys_and_build_distinct_rows(
        review_rows, time_connection, "time_id"  # , null_check_collums=["year"]
    )
    del time_connection
    LOG.info("-> Mapped Timestamps")

    reviewer_rows = MediaMappingUtils.remap_foreign_keys_and_build_distinct_rows(
        review_rows, reviewer_connection, "reviewer_id"
    )
    del reviewer_connection
    LOG.info("-> Mapped Reviewers")

    section_rows = MediaMappingUtils.remap_foreign_keys_and_build_distinct_rows(
        review_rows, section_connection, "section_id"
    )
    del section_connection
    LOG.info("-> Mapped Sections")

    LOG.info("[ Saving to CSV... ]")
    MediaBuilder.build_and_save_dataframe_from_rows(
        genre_rows, OUTPUT_DIR / "GENRES.csv"
    )
    LOG.info(f"Loading collection: GENRES...")
    loader.load_from_dict(genre_rows, "GENRES", batch_size=10000)
    del genre_rows

    MediaBuilder.build_and_save_dataframe_from_rows(
        company_rows, OUTPUT_DIR / "COMPANIES.csv"
    )
    LOG.info(f"Loading collection: COMPANIES...")
    loader.load_from_dict(company_rows, "COMPANIES", batch_size=10000)
    del company_rows

    MediaBuilder.build_and_save_dataframe_from_rows(
        time_rows, OUTPUT_DIR / "DIM_TIME.csv"
    )
    LOG.info(f"Loading collection: DIM_TIME...")
    loader.load_from_dict(time_rows, "DIM_TIME", batch_size=10000)
    del time_rows

    MediaBuilder.build_and_save_dataframe_from_rows(
        reviewer_rows, OUTPUT_DIR / "DIM_REVIEWER.csv"
    )
    LOG.info(f"Loading collection: DIM_REVIEWER...")
    loader.load_from_dict(reviewer_rows, "DIM_REVIEWER", batch_size=10000)
    del reviewer_rows

    LOG.info("[ Extracting Franchise for Sections ... ]")
    section_df = MediaBuilder.build_and_save_dataframe_from_rows(section_rows)
    del section_rows
    section_df = MediaTokenUtils.cluster_attribute_jaccard(
        section_df,
        "section_name",
        "section_group",
        type_attribute="section_type",
        blacklist_types=["Season", "Display"],
    )
    section_df = section_df.where(section_df.notna(), None)
    LOG.info(f"Loading collection: DIM_SECTION...")
    loader.load_from_dict(
        section_df.to_dict(orient="index"),
        "DIM_SECTION",
        batch_size=10000,
        id_col_name="id",
    )
    section_df.to_csv(OUTPUT_DIR / "DIM_SECTION.csv", sep="|", encoding="utf-8")
    del section_df

    reviews_df = MediaBuilder.build_and_save_dataframe_from_rows(
        review_rows,
        is_dict=True,
        # id_attribute_names=["time_id", "section_id", "reviewer_id", "media_info_id"],
    )
    del review_rows
    reviews_df = reviews_df.dropna(subset=["rating"])
    reviews_df = reviews_df.drop_duplicates(
        subset=["time_id", "section_id", "reviewer_id", "media_info_id"]
    )
    reviews_df.to_csv(OUTPUT_DIR / "FACT_REVIEWS.csv", sep="|", encoding="utf-8")
    reviews_df = reviews_df.where(reviews_df.notna(), None)

    LOG.info("< Finished with Metacritic >")

    return media_rows, title_year_set, reviews_df.to_dict(orient="index")


def setup_and_join_imdb_data_for_roles(media_rows, title_year_set, loader: MongoLoader):
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
    role_df = role_df.drop(columns=["nconst"])
    role_df.to_csv(OUTPUT_DIR / "ROLES.csv", sep="|", encoding="utf-8")
    loader.load_from_dict(
        role_df.to_dict(orient="index"), "ROLES", batch_size=10000, id_col_name="id"
    )
    del role_rows
    del role_df


def setup_bridges(media_rows, loader: MongoLoader):
    LOG.info(f"[ Setting up Bridge Tables... ]")
    # Build bridge tables for media_info
    bridge_dfs_media_info = MediaBuilder.build_bridge_rows(
        media_rows, ["genre_id", "company_id", "role_id"], "media_id"
    )

    MediaBuilder.build_and_save_dataframe_from_rows(
        bridge_dfs_media_info["genre_id"],
        OUTPUT_DIR / "BRIDGE_MEDIA_GENRE.csv",
        id_attribute_names=["media_id", "genre_id"],
    )
    loader.load_from_dict(
        bridge_dfs_media_info["genre_id"], "BRIDGE_MEDIA_GENRE", batch_size=10000
    )
    del bridge_dfs_media_info["genre_id"]

    MediaBuilder.build_and_save_dataframe_from_rows(
        bridge_dfs_media_info["company_id"],
        OUTPUT_DIR / "BRIDGE_MEDIA_COMPANY.csv",
        id_attribute_names=["media_id", "company_id"],
    )
    loader.load_from_dict(
        bridge_dfs_media_info["company_id"], "BRIDGE_MEDIA_COMPANY", batch_size=10000
    )
    del bridge_dfs_media_info["company_id"]

    MediaBuilder.build_and_save_dataframe_from_rows(
        bridge_dfs_media_info["role_id"],
        OUTPUT_DIR / "BRIDGE_MEDIA_ROLE.csv",
        id_attribute_names=["media_id", "role_id"],
    )
    loader.load_from_dict(
        bridge_dfs_media_info["role_id"], "BRIDGE_MEDIA_ROLE", batch_size=10000
    )
    del bridge_dfs_media_info


if __name__ == "__main__":
    # Load to transient database (MongoDB)
    LOG.info(f"[ Loading transformed data to transient MongoDB at: <{mongo_url}>... ]")
    loader = MongoLoader(mongo_conn_url=mongo_url, database=MONGO_DB)

    media_rows, title_year_set, reviews_rows = setup_metacritic_data(loader)

    setup_and_join_imdb_data_for_roles(media_rows, title_year_set, loader)
    del title_year_set

    setup_bridges(media_rows, loader)

    # DataFrames and CSVs
    LOG.info(f"[ Extracting Franchises... ]")
    media_df = MediaBuilder.build_and_save_dataframe_from_rows(media_rows, is_dict=True)
    media_df = MediaTokenUtils.cluster_attribute_jaccard(
        media_df, "primary_title", "franchise", type_attribute="media_type"
    )
    media_df.to_csv(OUTPUT_DIR / "DIM_MEDIA_INFO.csv", sep="|", encoding="utf-8")
    LOG.info(f"Loading collection: DIM_MEDIA_INFO...")
    media_df = media_df.where(media_df.notna(), None)
    loader.load_from_dict(
        media_df.to_dict(orient="index"),
        "DIM_MEDIA_INFO",
        batch_size=10000,
        id_col_name="id",
    )
    del media_df

    LOG.info(f"Loading collection: FACT_REVIEWS...")
    loader.load_from_dict(reviews_rows, "FACT_REVIEWS", batch_size=10000)
    del reviews_rows

    """
    for collection in COLLECTIONS:
        LOG.info(f"Loading collection: {collection}...")
        if loader.load_from_csv(
            OUTPUT_DIR / f"{collection}.csv",
            collection,
            delimiter="|",
            batch_size=10000,
        ):
            raise Exception(f"Failed to load collection: {collection}!")
    """

    LOG.info("All collections loaded successfully!")
