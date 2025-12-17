from models import MetacriticReview
import requests
import time
import random


class MetacriticReviewAPIHandler:
    def __init__(
        self,
        base_link: str,
        user_agent: dict[str:str],
        isCritics: bool = True,
        response_limit: int = 20,
        max_retries: int = 3,
    ):
        if "User-agent" not in user_agent:
            raise ValueError(f"User-agent not defined: {user_agent}")

        self.USER_AGENT = user_agent
        self.base_link = base_link
        self.isCritics = isCritics
        self.response_limit = response_limit
        self.max_retries = max_retries

        # Safe initialization
        data = self._requestReviewAPI(0)
        self.totalReviews = int(data.get("totalResults", 0))
        self._init_error = data.get("_error")

    # RETRY + GRACEFUL FAILURE

    def _requestReviewAPI(self, offset: int) -> dict:
        api_link = (
            f"https://backend.metacritic.com/reviews/metacritic/"
            f"{self.base_link}/web?offset={offset}"
        )

        last_error = None

        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.get(api_link, headers=self.USER_AGENT, timeout=10)
            except Exception as e:
                last_error = {
                    "type": "request_exception",
                    "message": str(e),
                    "attempt": attempt,
                    "url": api_link,
                }
            else:
                # Retry only on server-side issues
                if response.status_code >= 500:
                    last_error = {
                        "type": "server_error",
                        "status_code": response.status_code,
                        "attempt": attempt,
                        "url": api_link,
                    }
                else:
                    try:
                        payload = response.json()
                    except Exception:
                        last_error = {
                            "type": "non_json_response",
                            "status_code": response.status_code,
                            "attempt": attempt,
                            "url": api_link,
                        }
                    else:
                        if "data" in payload:
                            return payload["data"]

                        last_error = {
                            "type": "missing_data_key",
                            "status_code": response.status_code,
                            "payload_keys": list(payload.keys()),
                            "attempt": attempt,
                            "url": api_link,
                        }

            if attempt < self.max_retries:
                delay = (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                time.sleep(delay)

        return {
            "items": [],
            "totalResults": 0,
            "_error": {**last_error, "retries_exhausted": True},
        }

    def getTotalReviews(self) -> int:
        return self.totalReviews

    def getReviewBatch(self, offset: int) -> list[MetacriticReview]:
        data = self._requestReviewAPI(offset)

        if data.get("_error"):
            return []

        return [
            MetacriticReview(item, self.isCritics) for item in data.get("items", [])
        ]

    def getReviews(self) -> list[MetacriticReview]:
        if self.totalReviews == 0:
            return []

        reviews = []
        for offset in range(0, min(self.totalReviews, self.response_limit), 10):
            reviews.extend(self.getReviewBatch(offset))

        return reviews
