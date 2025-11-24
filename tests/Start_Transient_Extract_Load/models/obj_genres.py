from . import ModelsBase
from sqlalchemy import Column, Integer, String

class GenreORM(ModelsBase):
  __tablename__ = "GENRES"
  
  # Primary Keys
  Genre_ID = Column(Integer, primary_key=True, nullable=False)

  # Fields
  GenreTitle = Column(String, nullable=False)

  def __repr__(self):
    return f"<GenreORM(Genre_ID={self.Genre_ID}, GenreTitle='{self.GenreTitle}')>"