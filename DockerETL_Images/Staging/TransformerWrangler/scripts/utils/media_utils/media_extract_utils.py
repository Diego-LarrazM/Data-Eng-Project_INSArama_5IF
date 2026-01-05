class MediaExtractUtils:

    def extract_int_val(str_val):
        val = None
        if str_val is not None:
            val = int(str_val)
        return val

    def extract_title(data: dict) -> str | None:
        return data.get("title") or data.get("tv_title") or data.get("game_title")

    def extract_runtime_minutes(runtime_str: str | None) -> int | None:
        if not runtime_str or runtime_str == "\\N":
            return None
        try:  # "duration": "X h YZ m",
            mins = 0
            h_split = runtime_str.split("h")
            minsplit = h_split[1].split("m")
            mins += int(h_split[0].strip()) * 60
            mins += int(minsplit[0].strip())
            return mins
        except ValueError:
            return None

    def extract_year_from_release_date(release_date: str | None) -> int | None:
        if not isinstance(release_date, str):
            return None
        try:
            return int(release_date.strip()[-4:])
        except ValueError:
            return None

    def extract_all_reviews(data):
        reviews = []

        for section, section_reviews in data.get("critic_reviews", {}).items():
            reviews.append((section, section_reviews))
        for section, section_reviews in data.get("user_reviews", {}).items():
            reviews.append((section, section_reviews))
        return reviews
