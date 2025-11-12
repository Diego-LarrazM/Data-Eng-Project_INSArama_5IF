from bs4 import BeautifulSoup
from models import MetacriticReview

class MediaInfoPages:
    element_pagination_title: str # title used in url to obtain page/critics (ex: the-legend-of-zelda)
    main_page: BeautifulSoup
    critic_reviews: dict[str,list[MetacriticReview]] # platform/season: reviews list
    user_reviews: dict[str,list[MetacriticReview]]