from urllib.parse import quote_plus
import os

from mongo_loader import MongoLoader

USER_AGENT = {"User-agent": "Mozilla/5.0"}

if __name__ == "__main__":

    print("Starting Metacritic Scrapper...")
