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
        NAV_INNER_SPAN = "c-navigationPagination_itemButtonContent " + \
                         "u-flexbox " + \
                         "u-flexbox-alignCenter " + \
                         "u-flexbox-justifyCenter"
        PAGE_ELMTS_CONTAINER = "c-productListings"
        PAGE_ELMTS_SUBCONTAINER = re.compile("c-productListings_grid " +
                                             "g-grid-container " +
                                             "u-grid-columns " +
                                             "g-inner-spacing-bottom-large" +
                                             ".*")
        ELEMENT_CONTAINER = re.compile("c-finderProductCard.*")
        ELMT_a = re.compile("c-finderProductCard_container.*")

        PLATFORM_LI = "c-gameDetails_listItem " +\
                      "g-color-gray70 " +\
                      "u-inline-block"

    def __init__(self, category: MetacriticCategory, user_agent: dict[str:str]):
        if "User-agent" not in user_agent:
            raise f"User-agent not defined: {user_agent}"

        self.page_num = 1  # 581 for testing end
        self.current_elmt_num = 1
        self.USER_AGENT = user_agent
        # Pagination details to obtain browse page, review APIs and platform/season info container tag
        if category == MetacriticCategory.GAMES:
            self.pagination_info = {"browse": "game", "reviews": (
                "games", "platform"), "sections_cssTag": None}
        elif category == MetacriticCategory.MOVIES:
            self.pagination_info = {"browse": "movie", "reviews": (
                "movies", None), "sections_cssTag": None}
        elif category == MetacriticCategory.TV_SHOWS:
            self.pagination_info = {"browse": "tv", "reviews": (
                "seasons", "season"), "sections_cssTag": None}
        else:
            raise f"Category {category} is not recognized... possible values: (GAMES, MOVIES, TV_SHOWS)."

        self.url = f"https://www.metacritic.com/browse/{self.pagination_info["browse"]}/"

        self._loadCurrentPage()

        self.MAX_PAGES = int(self.browse_page_soup
                             .find_all("span", class_=self.cssClassTags.NAV_SPAN)[-1]
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
        self.current_elmt_num = 1  # Reset current element number
        self.browse_page_soup = self._loadPageFromUrl(
            self.url + f"?page={self.page_num}")  # Load page soup
        self._extractCurrentPageElements()
        print(
            f"Loaded page {self.page_num} with {self.max_elements} elements.")

    def _loadPageFromUrl(self, url) -> BeautifulSoup:
        response = requests.get(url, headers=self.USER_AGENT)
        return BeautifulSoup(response.text, "html.parser")

    def _extractCurrentPageElements(self) -> None:
        elements_container = self.browse_page_soup\
            .find("div", class_=self.cssClassTags.PAGE_ELMTS_CONTAINER)\
            .findChildren("div", class_=self.cssClassTags.PAGE_ELMTS_SUBCONTAINER)
        # There are two subcontainers to search
        self.browse_element_list = []
        for i in range(2):
            for elmt in elements_container[i].find_all("div", class_=self.cssClassTags.ELEMENT_CONTAINER, recursive=False):
                a = elmt.find("a", class_=self.cssClassTags.ELMT_a)
                if a and a.has_attr("href"):
                    # Get title used in url to idenfitfy it
                    # url/TITLE/ we take out the end /
                    trimmed = a["href"][:-1]
                    # we find the first / from the right thus taking TITLE
                    element_pagination_title = trimmed[trimmed.rfind("/") + 1:]
                    self.browse_element_list.append(element_pagination_title)
        self.max_elements = len(self.browse_element_list)

    def _extractCastFromCredits(self, slug: str, browse_type: str):
        """
        Reusable cast extractor for movies and TV shows.
        browse_type must be 'movie' or 'tv'
        """
        credits_url = f"https://www.metacritic.com/{browse_type}/{slug}/credits/"
        soup = self._loadPageFromUrl(credits_url)

        cast = []

        # Find the Cast section header
        cast_header = soup.find(
            "h3", string=lambda s: s and "cast" in s.lower()
        )
        if not cast_header:
            return cast

        dl = cast_header.find_next("dl")
        if not dl:
            return cast

        # Each cast member block
        for block in dl.select("div.u-grid-3column"):
            actor_dd = block.find("dd")
            role_dt = block.find("dt")

            if not actor_dd:
                continue

            actor_link = actor_dd.find("a")
            if not actor_link:
                continue

            name = actor_link.get_text(strip=True)
            role = role_dt.get_text(strip=True) if role_dt else None

            cast.append({
                "name": name,
                "role": role if role else None
            })

        return cast

    def _extractAwards(self, soup):
        awards = []

        container = soup.find(
            "div", attrs={"data-testid": "details-award-summary"}
        )
        if not container:
            return awards

        for award_div in container.select("div.c-productionAwardSummary_award"):
            name_div = award_div.find("div", class_="g-text-bold")
            summary_div = name_div.find_next_sibling(
                "div") if name_div else None

            if not name_div:
                continue

            summary = None
            if summary_div:
                summary = (
                    summary_div.get_text(strip=True)
                    .lstrip("•")
                    .strip()
                )

            awards.append({
                "name": name_div.get_text(strip=True),
                "summary": summary
            })

        return awards

    def _extract_summary_from_nuxt(self, soup):
        script = soup.find("script", string=re.compile(r"window\.__NUXT__"))
        if not script or not script.string:
            return None

        text = script.string

        match = re.search(
            r'description\s*:\s*"((?:\\.|[^"\\])*)"',
            text,
            re.DOTALL
        )

        if not match:
            return None

        raw = match.group(1)

        # Decode JS escape sequences properly
        return bytes(raw, "utf-8").decode("unicode_escape")

    def _extractMovieDetails(self, soup):
        details = {
            "summary": self._extract_summary_from_nuxt(soup),
            "genres": [],
            "directed_by": [],
            "written_by": [],
            "cast": [],
            "duration": None,
            "production_companies": [],
            "release_date": None,
            "rating": None,
            "awards": self._extractAwards(soup)
        }

        # Directors
        directors_div = soup.find(
            "div", class_="c-productDetails_staff_directors")
        if directors_div:
            details["directed_by"] = [
                a.get_text(strip=True)
                for a in directors_div.select("a.c-crewList_link")
            ]

        # Writers
        writers_div = soup.find("div", class_="c-productDetails_staff_writers")
        if writers_div:
            details["written_by"] = [
                a.get_text(strip=True)
                for a in writers_div.select("a.c-crewList_link")
            ]

        # Production / release / duration / rating / genres
        details_container = soup.find("div", class_="c-ProductionDetails")
        if details_container:
            for section in details_container.select("div.c-movieDetails_sectionContainer"):
                label = section.find("span", class_="g-text-bold")
                if not label:
                    continue

                label_text = label.get_text(strip=True).lower()

                if "production company" in label_text:
                    details["production_companies"] = [
                        c.get_text(strip=True)
                        for c in section.select("ul li span")
                    ]

                elif "release date" in label_text:
                    value = label.find_next_sibling("span")
                    if value:
                        details["release_date"] = value.get_text(strip=True)

                elif "duration" in label_text:
                    value = label.find_next_sibling("span")
                    if value:
                        details["duration"] = value.get_text(strip=True)

                elif "rating" in label_text:
                    value = label.find_next_sibling("span")
                    if value:
                        details["rating"] = value.get_text(strip=True)

                elif "genre" in label_text:
                    details["genres"] = [
                        span.get_text(strip=True)
                        for span in section.select("span.c-globalButton_label")
                    ]

        return details

    def _extractGameDetails(self, soup, platforms=None):
        details = {
            "summary": self._extract_summary_from_nuxt(soup),
            "rating": None,
            "initial_release_date": None,
            "developers": [],
            "publishers": [],
            "genres": [],
            "platforms": platforms if platforms else []
        }

        esrb_title = soup.select_one(
            "div.c-productionDetailsGame_esrb_title span.u-block"
        )
        if esrb_title:
            details["rating"] = esrb_title.get_text(strip=True)

        game_details = soup.find("div", class_="c-gameDetails")
        if not game_details:
            return details

        release_span = game_details.select_one(
            "div.c-gameDetails_ReleaseDate span.g-color-gray70"
        )
        if release_span:
            details["initial_release_date"] = release_span.get_text(strip=True)

        dev_block = game_details.find("div", class_="c-gameDetails_Developer")
        if dev_block:
            dev_names = []

            for li in dev_block.select("ul li"):
                # <a> then <span>, fallback to text
                if li.find("a"):
                    dev_names.append(li.find("a").get_text(strip=True))
                elif li.find("span"):
                    dev_names.append(li.find("span").get_text(strip=True))
                else:
                    dev_names.append(li.get_text(strip=True))

            details["developers"] = list({name for name in dev_names if name})

        pub_block = game_details.find(
            "div", class_="c-gameDetails_Distributor")
        if pub_block:
            pub_names = []

            links = pub_block.find_all("a")
            if links:
                for a in links:
                    pub_names.append(a.get_text(strip=True))
            else:
                text = pub_block.get_text(separator=" ", strip=True)
                text = text.replace("Publisher:", "").strip()
                if text:
                    pub_names.append(text)

            details["publishers"] = list({name for name in pub_names if name})

        genre_spans = game_details.select(
            "ul.c-genreList span.c-globalButton_label"
        )
        details["genres"] = [
            span.get_text(strip=True) for span in genre_spans
        ]

        return details

    def _extractGamePlatforms(self, soup):
        results = []

        # Find all <a> inside the platform modal
        links = soup.find_all("a", class_="c-gamePlatformTileLink")

        for a in links:
            href = a.get("href", "")
            if "platform=" not in href:
                continue

            # Extract slug
            slug = href.split("platform=")[-1].strip()

            # Extract display name
            name_div = a.find("div", class_="g-text-medium")
            display = name_div.get_text(strip=True) if name_div else slug

            results.append((slug, display))

        return results

    # def _extractSeasons(self, soup):
    #    seasons = []
    #    container = soup.find("div", class_="c-seasonSelector")
    #    if not container:
    #        return seasons
    #
    #    for a in container.find_all("a"):
    #        href = a.get("href", "")
    #        if "season=" in href:
    #            slug = href.split("season=")[-1].strip()
    #            seasons.append(slug)
    #
    # return seasons

    def _extractTvSeriesDetails(self, soup):
        media_details = {
            "summary": self._extract_summary_from_nuxt(soup),
            "production_companies": [],
            "initial_release_date": None,
            "number_of_seasons": None,
            "rating": None,
            "genres": [],
            "seasons": [],
            "awards": self._extractAwards(soup)
        }

        # DETAILS ROOT
        root = soup.find("div", class_="c-productionDetailsTv")
        if not root:
            return media_details

        # Production companies
        prod_label = root.find(
            "span", string=lambda s: s and "Production Company" in s
        )
        if prod_label:
            ul = prod_label.find_next("ul")
            if ul:
                media_details["production_companies"] = [
                    li.get_text(strip=True)
                    for li in ul.find_all("li")
                ]

        # Initial release date
        release_label = root.find(
            "span", string=lambda s: s and "Initial Release Date" in s
        )
        if release_label:
            value = release_label.find_next("span", class_="g-color-gray70")
            if value:
                media_details["initial_release_date"] = value.get_text(
                    strip=True)

        # Number of seasons
        seasons_label = root.find(
            "span", string=lambda s: s and "Number of seasons" in s
        )
        if seasons_label:
            value = seasons_label.find_next("span", class_="g-color-gray70")
            if value:
                try:
                    media_details["number_of_seasons"] = int(
                        value.get_text(strip=True).split()[0]
                    )
                except Exception:
                    pass

        # Rating
        rating_label = root.find(
            "span", string=lambda s: s and s.strip() == "Rating:"
        )
        if rating_label:
            value = rating_label.find_next("span", class_="g-color-gray70")
            if value:
                media_details["rating"] = value.get_text(strip=True)

        # Genres
        media_details["genres"] = [
            g.get_text(strip=True)
            for g in root.select("ul.c-genreList span.c-globalButton_label")
        ]

        # SEASONS
        for card in soup.select("div[data-testid='seasons-modal-card']"):
            link = card.find("a", href=True)
            if not link:
                continue

            season_slug = link["href"].rstrip("/").split("/")[-1]

            # Season name
            title = card.select_one("div.g-text-bold")
            season_name = title.get_text(strip=True) if title else None

            # Episodes & year
            info_spans = card.select("div.g-text-normal span")
            episodes = None
            year = None

            if len(info_spans) >= 2:
                ep_match = re.search(r"(\d+)", info_spans[0].get_text())
                if ep_match:
                    episodes = int(ep_match.group(1))

                year_match = re.search(r"(\d{4})", info_spans[1].get_text())
                if year_match:
                    year = int(year_match.group(1))

            # Metascore
            metascore = None
            score_span = card.select_one("div.c-siteReviewScore span")
            if score_span and score_span.get_text(strip=True).isdigit():
                metascore = int(score_span.get_text(strip=True))

            media_details["seasons"].append({
                "season": season_name,
                "season_slug": season_slug,
                "episodes": episodes,
                "year": year,
                "metascore": metascore
            })

        return media_details

    def _extractMediaInfoFromCurrent(self) -> MediaInfoPages:
        main_soup = None
        media_details = None

        current_element_title = self.browse_element_list[self.current_elmt_num - 1]
        self.current_elmt_num += 1  # This element already taken

        review_p_inf = self.pagination_info["reviews"]

        main_page_link = f"https://www.metacritic.com/{self.pagination_info["browse"]}/{current_element_title}/"
        critics_reviews_base_link = f"critic/{review_p_inf[0]}/{current_element_title}"
        user_reviews_base_link = f"user/{review_p_inf[0]}/{current_element_title}"

        critic_reviews = {}
        user_reviews = {}

        # criticsReviewHandler = MetacriticReviewAPIHandler(
        #  critics_reviews_base_link, user_agent=self.USER_AGENT, isCritics=True)
        # userReviewHandler = MetacriticReviewAPIHandler(
        #    user_reviews_base_link, user_agent=self.USER_AGENT, isCritics=False)

        # rev = criticsReviewHandler.getReviews()

        # return rev  # for testing

        # VIDEO GAMES
        if review_p_inf[1] == "platform":
            main_soup = self._loadPageFromUrl(main_page_link)

            platforms = self._extractGamePlatforms(main_soup)

            media_details = self._extractGameDetails(
                main_soup,
                platforms=[p[1] for p in platforms]
            )

            for platform_slug, platform_display in platforms:

                criticsReviewHandler = MetacriticReviewAPIHandler(
                    critics_reviews_base_link + f"/platform/{platform_slug}",
                    user_agent=self.USER_AGENT,
                    isCritics=True
                )

                userReviewHandler = MetacriticReviewAPIHandler(
                    user_reviews_base_link + f"/platform/{platform_slug}",
                    user_agent=self.USER_AGENT,
                    isCritics=False
                )

                critic_reviews[platform_display] = criticsReviewHandler.getReviews()
                user_reviews[platform_display] = userReviewHandler.getReviews()
        elif review_p_inf[1] == "season":
            # TV SHOWS
            main_soup = self._loadPageFromUrl(main_page_link)
            media_details = self._extractTvSeriesDetails(main_soup)

            for season in media_details.get("seasons", []):
                season_slug = season.get("season_slug")
                if not season_slug:
                    continue

                criticsReviewHandler = MetacriticReviewAPIHandler(
                    critics_reviews_base_link + f"/season/{season_slug}",
                    user_agent=self.USER_AGENT,
                    isCritics=True
                )
                userReviewHandler = MetacriticReviewAPIHandler(
                    user_reviews_base_link + f"/season/{season_slug}",
                    user_agent=self.USER_AGENT,
                    isCritics=False
                )

                critic_reviews[season_slug] = criticsReviewHandler.getReviews()
                user_reviews[season_slug] = userReviewHandler.getReviews()
                media_details["cast"] = self._extractCastFromCredits(
                    slug=current_element_title,
                    browse_type=self.pagination_info["browse"]
                )
        else:
            # MOVIES
            criticsReviewHandler = MetacriticReviewAPIHandler(
                critics_reviews_base_link, user_agent=self.USER_AGENT, isCritics=True)
            userReviewHandler = MetacriticReviewAPIHandler(
                user_reviews_base_link, user_agent=self.USER_AGENT, isCritics=False)

            critic_reviews["_default"] = criticsReviewHandler.getReviews()
            user_reviews["_default"] = userReviewHandler.getReviews()

            main_soup = self._loadPageFromUrl(main_page_link)
            media_details = self._extractMovieDetails(main_soup)
            media_details["cast"] = self._extractCastFromCredits(
                slug=current_element_title,
                browse_type=self.pagination_info["browse"]
            )

        return MediaInfoPages(
            element_pagination_title=current_element_title,
            main_page=main_soup,
            critic_reviews=critic_reviews,
            user_reviews=user_reviews,
            media_details=media_details
        )

        # We need to call api links for critics cause of lazy load :(  , either critic or user

        # Games per paltform
        # https://backend.metacritic.com/reviews/metacritic/critic/games/the-outer-worlds-2/platform/xbox-series-x/web?offset=0
        # https://backend.metacritic.com/reviews/metacritic/critic/games/the-outer-worlds-2/platform/pc/web?offset=0

        # Movies are easy
        # https://backend.metacritic.com/reviews/metacritic/critic/movies/nouvelle-vague/web?offset=0
        # https://backend.metacritic.com/reviews/metacritic/user/movies/nouvelle-vague/web?offset=0

        # For Tv shows its by season !
        # https://backend.metacritic.com/reviews/metacritic/critic/seasons/the-witcher/season/season-1/web
