from models import MetacriticReview
import requests

class MetacriticReviewAPIHandler:
        def __init__(self, base_link: str, user_agent: dict[str:str], isCritics: bool = True, response_limit: int = 20):
            if "User-agent" not in user_agent:
                raise f"User-agent not defined: {user_agent}"
            
            self.base_link = base_link
            self.totalReviews = int(self._requestReviewAPI(0)["totalResults"])
            self.isCritics = isCritics
            self.response_limit = response_limit

        def _requestReviewAPI(self, offset: int) -> dict:
            api_link = f"https://backend.metacritic.com/reviews/metacritic/{self.base_link}/web?offset={offset}"
            response = requests.get(api_link, headers = self.USER_AGENT)
            return response.json()["data"]
        
        def getTotalReviews(self) -> int:
            return self.totalReviews
        
        def getReviewBatch(self, offset: int) -> list[MetacriticReview]:
            data = self._requestReviewAPI(offset)
            return [MetacriticReview(item, self.isCritics) for item in data["items"]]
        
        def getReviews(self) -> list[MetacriticReview]:
            reviews = []
            for offset in range(0, min(self.totalReviews, self.response_limit), 10):
                reviews.extend(self.getReviewBatch(offset))
            return reviews