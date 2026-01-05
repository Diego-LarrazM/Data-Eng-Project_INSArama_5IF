from . import ModelsBase
from sqlalchemy import Column, String


class GenreORM(ModelsBase):
    __tablename__ = "GENRES"

    # Primary Keys
    id = Column(String, primary_key=True, nullable=False)

    # Fields
    genre_title = Column(String, nullable=False)

    def __repr__(self):
        return f"<GenreORM(id={self.id}, genre_title='{self.genre_title}')>"
