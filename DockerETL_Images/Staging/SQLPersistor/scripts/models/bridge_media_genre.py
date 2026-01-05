from . import ModelsBase
from sqlalchemy import Column, String, Float, ForeignKey
from sqlalchemy.orm import relationship


class MediaGenreBridgeORM(ModelsBase):
    __tablename__ = "BRIDGE_MEDIA_GENRE"

    # Primary Keys
    media_id = Column(
        String,
        ForeignKey("DIM_MEDIA_INFO.id"),
        primary_key=True,
        nullable=False,
    )
    genre_id = Column(String, ForeignKey("GENRES.id"), primary_key=True, nullable=False)

    # Fields
    weight = Column(Float, nullable=False)

    # ORM relationships
    media = relationship("MediaInfoDimORM")
    genre = relationship("GenreORM")

    def __repr__(self):
        return f"<MediaGenreBridgeORM(media_id={self.media_id}, genre_id={self.genre_id}, weight={self.weight})>"
