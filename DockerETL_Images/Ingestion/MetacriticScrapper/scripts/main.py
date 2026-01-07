from urllib.parse import quote_plus
import os
import json
from metacritic_scrapper import *

from utils.logger import LOG

DATA_FILE_DIRECTORY = os.environ.get("DATA_FILE_DIRECTORY")
NUMBER_OF_MEDIA_TO_SCRAP = int(os.environ.get("NUMBER_OF_MEDIA_TO_SCRAP"))

if __name__ == "__main__":

    LOG.info("Starting Metacritic Scrapper...")

    def save_game_json(media, output_folder=f"{DATA_FILE_DIRECTORY}/GAMES"):
        os.makedirs(output_folder, exist_ok=True)

        filename = f"{media.element_pagination_title}.json"

        data = {
            "game_title": media.element_pagination_title.replace("-", " ").title(),
            "media_details": media.media_details,
            "critic_reviews": {
                platform: [rev.to_dict() for rev in reviews]
                for platform, reviews in media.critic_reviews.items()
            },
            "user_reviews": {
                platform: [rev.to_dict() for rev in reviews]
                for platform, reviews in media.user_reviews.items()
            },
        }

        with open(os.path.join(output_folder, filename), "w", encoding="utf8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        LOG.info(f"Saved JSON → {filename}")

    def save_movie_json(
        media: MediaInfoPages, output_folder=f"{DATA_FILE_DIRECTORY}/MOVIES"
    ):
        os.makedirs(output_folder, exist_ok=True)

        filename = f"{media.element_pagination_title}.json"

        data = {
            "title": media.element_pagination_title.replace("-", " ").title(),
            "media_details": media.media_details,
            "critic_reviews": {
                platform: [review.to_dict() for review in reviews]
                for platform, reviews in media.critic_reviews.items()
            },
            "user_reviews": {
                platform: [review.to_dict() for review in reviews]
                for platform, reviews in media.user_reviews.items()
            },
        }

        filepath = os.path.join(output_folder, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        LOG.info(f"Saved JSON: {filepath}")

    def save_tv_json(media, output_folder=f"{DATA_FILE_DIRECTORY}/TV_SHOWS"):
        os.makedirs(output_folder, exist_ok=True)

        filename = f"{media.element_pagination_title}.json"

        data = {
            "tv_title": media.element_pagination_title.replace("-", " ").title(),
            "media_details": media.media_details,
            "critic_reviews": {
                season: [rev.to_dict() for rev in reviews]
                for season, reviews in media.critic_reviews.items()
            },
            "user_reviews": {
                season: [rev.to_dict() for rev in reviews]
                for season, reviews in media.user_reviews.items()
            },
        }

        with open(os.path.join(output_folder, filename), "w", encoding="utf8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        LOG.info(f"Saved JSON → {filename}")

    scr_TV_SHOWS = MetacriticScrapper(
        MetacriticCategory.TV_SHOWS, user_agent={"User-agent": "Mozilla/5.0"}
    )
    count = 0
    for media in scr_TV_SHOWS:
        LOG.info(f"\n===  Saving Tv_shows {count+1}===")
        save_tv_json(media)
        count += 1
        if count >= NUMBER_OF_MEDIA_TO_SCRAP:
            break

    scr_GAMES = MetacriticScrapper(
        MetacriticCategory.GAMES, user_agent={"User-agent": "Mozilla/5.0"}
    )
    count = 0
    for media in scr_GAMES:
        LOG.info(f"\n===  Saving game {count+1}===")
        save_game_json(media)
        count += 1
        if count >= NUMBER_OF_MEDIA_TO_SCRAP:
            break

    scr_MOVIES = MetacriticScrapper(
        MetacriticCategory.MOVIES,
        user_agent={"User-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
    )
    count = 0
    for media in scr_MOVIES:
        LOG.info(f"\n=== Saving movie {count+1}===")
        save_movie_json(media)
        count += 1
        if count >= NUMBER_OF_MEDIA_TO_SCRAP:
            break
