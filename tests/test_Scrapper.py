import sys
import os

# Chemin vers le dossier qui contient DockerETL_Images
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from DockerETL_Images.Ingestion.Scrapper.scripts.metacritic_scrapper import *


scr = MetacriticScrapper(MetacriticScrapper.Category.GAMES)
print("test")
print(scr.MAX_PAGES)

#scr.__next__()