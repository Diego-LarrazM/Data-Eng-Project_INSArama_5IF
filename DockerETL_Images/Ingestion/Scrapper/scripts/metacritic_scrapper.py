import requests
from bs4 import BeautifulSoup
import re
from enum import StrEnum

    
class MetacriticScrapper:
    
    class Category(StrEnum):
        GAMES = "game/"
        MOVIES = "movie/"
        TV_SHOWS = "tv/"
    
    class MediaInfo:
        main_page: BeautifulSoup
        critic_review_pages: dict[str, BeautifulSoup]
        user_review_pages: dict[str, BeautifulSoup]

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
    
    USER_AGENT = {"User-agent": "Mozilla/5.0"}

    def __init__(self, category: Category):
        self.page_num = 1  # 581 for testing end
        self.current_elmt_num = 1
        self.url = "https://www.metacritic.com/browse/"+str(category)
        
        self._loadCurrentPage()

        self.MAX_PAGES = int(self.browse_page_soup \
                            .find_all("span",class_=self.cssClassTags.NAV_SPAN)[-1] \
                            .find("span", class_=self.cssClassTags.NAV_INNER_SPAN).text.strip())

        # self.MAX_PAGES = 582 # for testing end
    
    def _loadPage(self, url) -> BeautifulSoup:
        response = requests.get(url, headers = self.USER_AGENT)
        return BeautifulSoup(response.text, "html.parser")
    
    def _extractCurrentPageElements(self) -> None:
        elements_container = self.browse_page_soup\
                             .find("div", class_=self.cssClassTags.PAGE_ELMTS_CONTAINER)\
                             .findChildren("div", class_=self.cssClassTags.PAGE_ELMTS_SUBCONTAINER)
        ## There are two subcontainers to search
        topgroup = elements_container[0].find_all("div",class_=self.cssClassTags.ELEMENT_CONTAINER,recursive=False)
        downgroup = elements_container[1].find_all("div",class_=self.cssClassTags.ELEMENT_CONTAINER,recursive=False)
        # Combine both groups to get elements to browse
        self.browse_element_list = topgroup + downgroup
        self.max_elements = len(self.browse_element_list)

    def _loadCurrentPage(self) -> None:
        self.current_elmt_num = 1 # Reset current element number
        self.browse_page_soup = self._loadPage(self.url + f"?page={self.page_num}") # Load page soup
        self._extractCurrentPageElements()
        print(f"Loaded page {self.page_num} with {self.max_elements} elements.")

    def _extractMediaInfoFromCurrent(self) -> MediaInfo:
        current_element = self.browse_element_list[self.current_elmt_num - 1]
        self.current_elmt_num += 1

        main_page_link = "https://www.metacritic.com" + current_element.find("a", class_=self.cssClassTags.ELMT_a)["href"]
        critics_reviews_link = main_page_link + "/critic-reviews"
        user_reviews_link = main_page_link + "/user-reviews"

        critic_reviews = {}
        user_reviews = {}

        return main_page_link # for testing

        ####
        #### Extract reviews, if tv or film just add them to dict["all"], if games, extract by platform dict[<platform>]
        ####

        # return self.MediaInfo(
        #     main_page = self._loadPage(main_page_link),
        #     critic_review_pages = {},
        #     user_review_pages = {}
        # )
        
        
    def __iter__(self):
        return self

    def __next__(self) -> MediaInfo:
        # if all elements viewed, load next page
        if self.current_elmt_num > self.max_elements:
            print("Loading next page...")
            self.page_num += 1 # Next page number
            if self.page_num > self.MAX_PAGES:
                print("All pages viewed.")
                raise StopIteration  # Fin de l'itération
            self._loadCurrentPage()
        
        return self._extractMediaInfoFromCurrent()