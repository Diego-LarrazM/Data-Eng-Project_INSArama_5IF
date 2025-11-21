from . import ModelsBase
from sqlalchemy import Column, Integer, Float, ForeignKey
from sqlalchemy.orm import relationship

class MediaGenreBridgeORM(ModelsBase):
  __tablename__ = "BRIDGE_MEDIA_GENRE"
  
  # Primary Keys
  MediaInfo_ID = Column(Integer, ForeignKey("DIM_MEDIA_INFO.MediaInfo_ID"), primary_key=True, nullable=False)
  Genre_ID = Column(Integer, ForeignKey("GENRES.Genre_ID"), primary_key=True, nullable=False)

  # Fields
  Weight = Column(Float, nullable=False)

  # ORM relationships
  media = relationship("MediaInfoDimORM")
  genre = relationship("GenreORM")

  def __repr__(self):
    return f"<MediaGenreBridgeORM(MediaInfo_ID={self.MediaInfo_ID}, Genre_ID={self.Genre_ID}, Weight={self.Weight})>"