import sys
import os

# Chemin vers le dossier qui contient DockerETL_Images
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from DockerETL_Images.Ingestion.Scrapper.scripts.metacritic_scrapper import *

scr = MetacriticScrapper(MetacriticCategory.MOVIES)
print("Max pages:", scr.MAX_PAGES)

# for i in scr:   # for testing end
#   print(i)
# for i in range(26):
#   print(scr.__next__())
reviews = scr.__next__()
print(len(reviews))
print(reviews)

for rev in scr.__next__():
  print(rev)
  print()


# for media in MetacriticScrapper(MetacriticScrapper.Category.GAMES):
#     media.user_reviews_page
#     media.critic_reviews_page
#     media.main_page
    