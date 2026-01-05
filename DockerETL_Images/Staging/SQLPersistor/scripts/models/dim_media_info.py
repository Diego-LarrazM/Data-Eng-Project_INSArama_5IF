from . import ModelsBase
from sqlalchemy import Column, Integer, String, Float, DateTime


class MediaInfoDimORM(ModelsBase):
    __tablename__ = "DIM_MEDIA_INFO"

    # Primary Keys
    id = Column(String, primary_key=True, nullable=False)

    # Fields
    media_type = Column(String, nullable=False)
    franchise = Column(String, nullable=True)
    primary_title = Column(String, nullable=False)
    release_date = Column(DateTime, nullable=False)
    duration = Column(Float, nullable=True)
    pegi_mpa_rating = Column(Integer, nullable=True)
    description = Column(String, nullable=True)

    def __repr__(self):
        return (
            f"<MediaInfoDimORM(id={self.id}, media_type='{self.media_type}', "
            f"franchise='{self.franchise}', primary_title='{self.primary_title}', "
            f"release_date='{self.release_date}', "
            f"duration={self.duration}min, pegi_mpa_rating='{self.pegi_mpa_rating}', "
            f"description='{self.description}')>"
        )
