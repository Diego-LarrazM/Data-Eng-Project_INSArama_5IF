from metacritic_scrapper import *
import os
import json


def save_movie_json(media: MediaInfoPages, output_folder="movies_results"):
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
        }
    }

    filepath = os.path.join(output_folder, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    print(f"Saved JSON: {filepath}")


scr = MetacriticScrapper(
    MetacriticCategory.MOVIES,
    user_agent={"User-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
)

print("Max pages:", scr.MAX_PAGES)

N = 50

count = 0

for movie in scr:
    print(
        f"\n=== Saving movie {count+1}: {movie.element_pagination_title} ===")
    save_movie_json(movie)
    count += 1

    if count == N:
        break

print(f"YESSIR! Saved {count} movies.")
