from bs4 import BeautifulSoup
from enum import StrEnum

class MetacriticCategory(StrEnum):
    GAMES = "Games"
    MOVIES = "Movies"
    TV_SHOWS = "TV Shows"
    
class MetacriticReview:
    author: str
    company: str | None
    quote: str
    rating: int
    date : str # to chagne to date type
    url: str | None
    spoiler: bool
    isCritic: bool

    def __init__(self, data: dict, isCritic: bool):
        self.author = data.get("author", "")
        self.company = data.get("publicationName", None)
        if isCritic and self.author == "": 
            self.author = data.get("publicationSlug", "") # !!!!!!!! We're going to need to get author from url for games :/
        self.quote = data.get("quote", "")
        self.rating = data.get("score", None)
        self.date = data.get("date", "")
        self.url = data.get("url", None)
        self.spoiler = data.get("spoiler", False) # if not present assume no spoiler, specially for critic reviews
        self.isCritic = isCritic
    
class MediaInfo:
    element_pagination_title: str # title used in url to obtain page/critics (ex: the-legend-of-zelda)
    main_page: BeautifulSoup
    critic_reviews: dict[str,list[MetacriticReview]] # platform/season: reviews list
    user_reviews: dict[str,list[MetacriticReview]]

