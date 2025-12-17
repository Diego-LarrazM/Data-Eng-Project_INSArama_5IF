from metacritic_scrapper import MetacriticScrapper, MetacriticCategory
import json
import os


def save_game_json(media, output_folder="games_results"):
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
        }
    }

    with open(os.path.join(output_folder, filename), "w", encoding="utf8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    print(f"Saved JSON → {filename}")


scr = MetacriticScrapper(
    MetacriticCategory.GAMES,
    user_agent={"User-agent": "Mozilla/5.0"}
)

print(f"Loaded first page — total elements: {len(scr.browse_element_list)}")

LIMIT = 50
count = 0

for media in scr:
    print("\n==============================================")
    print(f"GAME {count+1}: {media.element_pagination_title}")

    print("Platforms detected:", list(media.critic_reviews.keys()))

    for plat, revs in media.critic_reviews.items():
        print(f"Platform {plat} → {len(revs)} critic reviews")

    for plat, revs in media.user_reviews.items():
        print(f"Platform {plat} → {len(revs)} user reviews")

    save_game_json(media)

    count += 1
    if count >= LIMIT:
        break

print("\ YESSIR !! games test completed.")
