import textwrap


class MetacriticReview:
    author: str
    company: str | None
    quote: str
    rating: int
    # !!! to change to date type !!! We're going to need to get date from review_src_url for old critics :/
    post_date: str | None
    # If is a critic, full review was posted on their page
    full_review_src_url: str | None
    spoiler: bool
    isCritic: bool

    def to_dict(self):
        return {
            "author": self.author,
            "company": self.company,
            "quote": self.quote,
            "rating": self.rating,
            "post_date": self.post_date,
            "full_review_src_url": self.full_review_src_url,
            "spoiler": self.spoiler,
            "isCritic": self.isCritic,
        }

    def __init__(self, data: dict, isCritic: bool):
        self.author = data.get("author", "")
        self.company = data.get("publicationName", None)
        if isCritic and self.author == "":
            # !!! We're going to need to get author from review_src_url for games :/
            self.author = data.get("publicationSlug", "")
        self.quote = data.get("quote", "")
        self.rating = data.get("score", None)
        self.post_date = data.get("date", "")
        self.full_review_src_url = data.get("url", None)
        # if not present assume no spoiler, specially for critic reviews
        self.spoiler = data.get("spoiler", False)
        self.isCritic = isCritic

    def __repr__(self):
        wrapped_quote = textwrap.fill(
            self.quote, width=80, subsequent_indent=" " * 10  # matches 4-space indent
        )
        return (
            "MetacriticReview(\n"
            + f"    author={self.author}\n"
            + f"    company={self.company}\n"
            + f"    isCritic={self.isCritic}\n"
            + f"    full_review_src_url={self.full_review_src_url}\n"
            + f"    spoiler={self.spoiler}\n"
            + f"    post_date={self.post_date}\n"
            + f"    rating={self.rating}\n"
            + f'    quote="{wrapped_quote}"\n'
            + ")"
        )

    def __str__(self):
        wrapped_quote = textwrap.fill(self.quote, width=80)
        return (
            f"\n"
            + f'"{wrapped_quote}"\n'
            + f" - by {self.author}"
            + (f" | {self.company}" if self.company else "")
            + f" [{self.post_date}]"
        )
