from metacritic_scrapper import MetacriticScrapper, MetacriticCategory
import json
import os


def save_tv_json(media, output_folder="tv_results"):
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
        }
    }

    with open(os.path.join(output_folder, filename), "w", encoding="utf8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    print(f"Saved JSON → {filename}")


scr = MetacriticScrapper(
    MetacriticCategory.TV_SHOWS,
    user_agent={"User-agent": "Mozilla/5.0"}
)

print(f"Loaded first page — total elements: {len(scr.browse_element_list)}")

LIMIT = 50
count = 0

for media in scr:
    print("\n==============================================")
    print(f"TV SERIES {count + 1}: {media.element_pagination_title}")

    if media.media_details:
        seasons = media.media_details.get("seasons", [])
        print(f"Seasons detected: {len(seasons)}")

        for s in seasons:
            print(
                f"{s.get('season')} → "
                f"{s.get('episodes')} eps | "
                f"{s.get('year')} | "
                f"Metascore: {s.get('metascore')}"
            )

    save_tv_json(media)

    count += 1
    if count >= LIMIT:
        break

print("\nYESSIR !! TV series test completed.")
