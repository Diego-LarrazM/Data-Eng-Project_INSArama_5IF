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
        
    
    NAVIGATION_SPAN_CLASS = "c-navigationPagination_item " + \
                            "c-navigationPagination_item--page " + \
                            "enabled"
    NAVIGATION_INNER_SPAN_CLASS = "c-navigationPagination_itemButtonContent "+ \
                                  "u-flexbox "+ \
                                  "u-flexbox-alignCenter "+ \
                                  "u-flexbox-justifyCenter"
    PAGE_ELEMENTS_CONTAINER_CLASS = "c-productListings"
    PAGE_ELEMENTS_SUBCONTAINER_CLASS = "c-productListings_grid "+ \
                                       "g-grid-container "+ \
                                       "u-grid-coELEMENT_CONTAINER_CLASSlumns "+ \
                                       "g-inner-spacing-bottom-large"
    ELEMENT_CONTAINER_CLASS = "c-finderProductCard"
    
    USER_AGENT = {"User-agent": "Mozilla/5.0"}
    
    def __init__(self, category: Category):
        self.page_num = 0
        self.current_elmt_num = 1
        self.url = "https://www.metacritic.com/browse/"+str(category)
        
        self.loadNextPage()
        
        #self.elements_in_page_num = len(self.browse_page_soup \
        #                            .find("div", class_= self.PAGE_ELEMENTS_CONTAINER_CLASS)
        #                            .find_all...)
        
        self.MAX_PAGES = int(self.browse_page_soup \
                        .find_all("span",class_=self.NAVIGATION_SPAN_CLASS)[-1] \
                        .find("span", class_=self.NAVIGATION_INNER_SPAN_CLASS).text.strip())
        
    def loadNextPage(self):
        self.page_num += 1
        response = requests.get(self.url+f"?page={self.page_num}", headers = self.USER_AGENT)
        self.browse_page_soup = BeautifulSoup(response.text, "html.parser")
        elements_container = self.browse_page_soup.find("div", class_=self.PAGE_ELEMENTS_CONTAINER_CLASS).findChildren("div", class_=re.compile(f"{self.PAGE_ELEMENTS_SUBCONTAINER_CLASS}.*"))
        print(elements_container)
        self.browse_element_list = []
        self.max_elements = len(self.browse_element_list)

        
    def __iter__(self):
        return self

    def __next__(self):
        if self.page_num > self.MAX_PAGES:
            raise StopIteration  # Fin de l'itération

        # if all elements viewed, load next page
        if self.current_elmt_num > self.max_elements:
            self.loadNextPage()
        
        current_element = self.browse_element_list[self.current_elmt_num]
        print(current_element)
        print(current_element.text)
        
        self.current_elmt_num += 1
        
        
        
        # get main page url from element
        # get main page/review/critic_review pages and return them
        
        
        
        
# for media in Scrapper(url):
#     media.user_reviews_page
#     media.critic_reviews_page
#     media.main_page
    