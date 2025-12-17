from bs4 import BeautifulSoup
from models import MetacriticReview
from attr import dataclass


@dataclass
class MediaInfoPages:
    element_pagination_title: str
    main_page: BeautifulSoup

    # platform/season: reviews list
    critic_reviews: dict[str, list[MetacriticReview]]
    user_reviews: dict[str, list[MetacriticReview]]

    media_details: dict | None = None

    def to_dict(self):
        return {
            "element_pagination_title": self.element_pagination_title,
            "media_details": self.media_details,
            "critic_reviews": {
                platform: [r.to_dict() for r in reviews]
                for platform, reviews in self.critic_reviews.items()
            },
            "user_reviews": {
                platform: [r.to_dict() for r in reviews]
                for platform, reviews in self.user_reviews.items()
            },
        }

    def __len__(self):
        # total number of critic + user reviews
        return sum(len(v) for v in self.critic_reviews.values()) + sum(
            len(v) for v in self.user_reviews.values()
        )
