import requests
import re

from metacritic_api_handler import MetacriticReviewAPIHandler
from models import MediaInfoPages
from utils import MetacriticCategory
from bs4 import BeautifulSoup
    
class MetacriticScrapper:
    
    class cssClassTags:
        NAV_SPAN = "c-navigationPagination_item " + \
                   "c-navigationPagination_item--page " + \
                   "enabled"
        NAV_INNER_SPAN = "c-navigationPagination_itemButtonContent "+ \
                         "u-flexbox "+ \
                         "u-flexbox-alignCenter "+ \
                         "u-flexbox-justifyCenter"
        PAGE_ELMTS_CONTAINER = "c-productListings"
        PAGE_ELMTS_SUBCONTAINER = re.compile("c-productListings_grid "+ \
                                  "g-grid-container "+ \
                                  "u-grid-columns "+ \
                                  "g-inner-spacing-bottom-large"+ \
                                  ".*")
        ELEMENT_CONTAINER = re.compile("c-finderProductCard.*")
        ELMT_a = re.compile("c-finderProductCard_container.*")
        
        PLATFORM_LI = "c-gameDetails_listItem "+\
                      "g-color-gray70 "+\
                      "u-inline-block"

    def __init__(self, category: MetacriticCategory, user_agent: dict[str:str]):
        if "User-agent" not in user_agent:
                raise f"User-agent not defined: {user_agent}"
        
        self.page_num = 1  # 581 for testing end
        self.current_elmt_num = 1
        self.USER_AGENT = user_agent
        # Pagination details to obtain browse page, review APIs and platform/season info container tag
        if category == MetacriticCategory.GAMES:
            self.pagination_info = {"browse":"game", "reviews": ("games","platform"), "sections_cssTag": None}
        elif category == MetacriticCategory.MOVIES:
            self.pagination_info =  {"browse":"movie", "reviews": ("movies", None), "sections_cssTag": None}
        elif category == MetacriticCategory.TV_SHOWS:
            self.pagination_info = {"browse":"tv", "reviews": ("seasons","season"), "sections_cssTag": None}
        else:
            raise f"Category {category} is not recognized... possible values: (GAMES, MOVIES, TV_SHOWS)."
        
        
        self.url = f"https://www.metacritic.com/browse/{self.pagination_info["browse"]}/"
        
        self._loadCurrentPage()

        self.MAX_PAGES = int(self.browse_page_soup \
                            .find_all("span",class_=self.cssClassTags.NAV_SPAN)[-1] \
                            .find("span", class_=self.cssClassTags.NAV_INNER_SPAN).text.strip())

        # self.MAX_PAGES = 582 # for testing end
    
    def __iter__(self):
        return self

    def __next__(self) -> MediaInfoPages:
        # if all elements viewed, load next page
        if self.current_elmt_num > self.max_elements:
            print("Loading next page...")
            self.page_num += 1
            if self.page_num > self.MAX_PAGES:
                print("All pages viewed.")
                raise StopIteration
            self._loadCurrentPage()
        
        return self._extractMediaInfoFromCurrent()
    
    def _loadCurrentPage(self) -> None:
        self.current_elmt_num = 1 # Reset current element number
        self.browse_page_soup = self._loadPageFromUrl(self.url + f"?page={self.page_num}") # Load page soup
        self._extractCurrentPageElements()
        print(f"Loaded page {self.page_num} with {self.max_elements} elements.")
    
    def _loadPageFromUrl(self, url) -> BeautifulSoup:
        response = requests.get(url, headers = self.USER_AGENT)
        return BeautifulSoup(response.text, "html.parser")
    
    def _extractCurrentPageElements(self) -> None:
        elements_container = self.browse_page_soup\
                             .find("div", class_=self.cssClassTags.PAGE_ELMTS_CONTAINER)\
                             .findChildren("div", class_=self.cssClassTags.PAGE_ELMTS_SUBCONTAINER)
        ## There are two subcontainers to search
        self.browse_element_list = []
        for i in range(2):
            for elmt in elements_container[i].find_all("div", class_=self.cssClassTags.ELEMENT_CONTAINER, recursive=False):
                a = elmt.find("a", class_=self.cssClassTags.ELMT_a)
                if a and a.has_attr("href"):
                    # Get title used in url to idenfitfy it
                    trimmed=a["href"][:-1] # url/TITLE/ we take out the end /
                    element_pagination_title = trimmed[trimmed.rfind("/") + 1:] # we find the first / from the right thus taking TITLE
                    self.browse_element_list.append(element_pagination_title)
        self.max_elements = len(self.browse_element_list)

    def _extractMediaInfoFromCurrent(self) -> MediaInfoPages:
        current_element_title = self.browse_element_list[self.current_elmt_num - 1]
        self.current_elmt_num += 1 # This element already taken

        review_p_inf = self.pagination_info["reviews"] 

        main_page_link = f"https://www.metacritic.com/{self.pagination_info["browse"]}/{current_element_title}/"
        critics_reviews_base_link = f"critic/{review_p_inf[0]}/{current_element_title}"
        user_reviews_base_link = f"user/{review_p_inf[0]}/{current_element_title}"

        critic_reviews = {}
        user_reviews = {}
        
        criticsReviewHandler = MetacriticReviewAPIHandler(critics_reviews_base_link, user_agent=self.USER_AGENT, isCritics=True)
        userReviewHandler = MetacriticReviewAPIHandler(user_reviews_base_link, user_agent=self.USER_AGENT, isCritics=False)

        rev = criticsReviewHandler.getReviews()

        return rev # for testing
        
        if review_p_inf[1]: # if contains sections 
            critics_reviews_base_link += f"{review_p_inf[1]}/"
            user_reviews_base_link += f"{review_p_inf[1]}/"
            #### ...
            #### Get sections and per each get for each offset until total items or max limit append reviews.
            #### Use self.pagination_info["sections_cssTag"] to get sections container (actual cssTag to be defined!!!!!)
            #### ...
            for section in sections: 
               criticsReviewHandler = MetacriticReviewAPIHandler(critics_reviews__base_link+f"{section}/web", user_agent=self.USER_AGENT, isCritics=True)
               userReviewHandler = MetacriticReviewAPIHandler(user_reviews__base_link+f"{section}/web", user_agent=self.USER_AGENT, isCritics=False)
               critic_reviews[section] = criticsReviewHandler.getReviews()
               user_reviews[section] = userReviewHandler.getReviews()
        else:
            # get total items and per each offset until total items or max limit append reviews.
            criticsReviewHandler = MetacriticReviewAPIHandler(critics_reviews_base_link, user_agent=self.USER_AGENT, isCritics=True)
            userReviewHandler = MetacriticReviewAPIHandler(user_reviews_base_link, user_agent=self.USER_AGENT, isCritics=False)

            critic_reviews["_default"] = criticsReviewHandler.getReviews()
            user_reviews["_default"] = userReviewHandler.getReviews()
        
        return MediaInfoPages(
            element_pagination_title=current_element_title,
            main_page = self._loadPageFromUrl(main_page_link),
            critic_reviews = critic_reviews,
            user_reviews = user_reviews
        )

        # We need to call api links for critics cause of lazy load :(  , either critic or user
        
        ## Games per paltform
        # https://backend.metacritic.com/reviews/metacritic/critic/games/the-outer-worlds-2/platform/xbox-series-x/web?offset=0
        # https://backend.metacritic.com/reviews/metacritic/critic/games/the-outer-worlds-2/platform/pc/web?offset=0
        
        ## Movies are easy
        # https://backend.metacritic.com/reviews/metacritic/critic/movies/nouvelle-vague/web?offset=0
        # https://backend.metacritic.com/reviews/metacritic/user/movies/nouvelle-vague/web?offset=0
        
        ## For Tv shows its by season !
        # https://backend.metacritic.com/reviews/metacritic/critic/seasons/the-witcher/season/season-1/web
