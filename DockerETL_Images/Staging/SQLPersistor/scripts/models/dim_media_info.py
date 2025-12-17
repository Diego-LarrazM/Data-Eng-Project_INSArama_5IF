from . import ModelsBase
from sqlalchemy import Column, Integer, String, Float, DateTime


class MediaInfoDimORM(ModelsBase):
    __tablename__ = "DIM_MEDIA_INFO"

    # Primary Keys
    MediaInfo_ID = Column(Integer, primary_key=True, nullable=False)

    # Fields
    PrimaryTitle = Column(String, nullable=False)
    Titlelanguage = Column(String, nullable=False)
    OriginalTitle = Column(String, nullable=False)
    MediaType = Column(String, nullable=False)
    ReleaseDate = Column(DateTime, nullable=False)
    # Sales = Column(Integer, nullable=True)
    Duration = Column(Float, nullable=True)
    Description = Column(String, nullable=True)
    PEGI_MPA_Rating = Column(Integer, nullable=True)

    def __repr__(self):
        return (
            f"<MediaInfoDimORM(MediaInfo_ID={self.MediaInfo_ID}, PrimaryTitle='{self.PrimaryTitle}', "
            f"Titlelanguage='{self.Titlelanguage}', OriginalTitle='{self.OriginalTitle}', "
            f"MediaType='{self.MediaType}', ReleaseDate='{self.ReleaseDate}', "
            f"Duration={self.Duration}, PEGI_MPA_Rating={self.PEGI_MPA_Rating})>"
        )
