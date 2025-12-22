from . import ModelsBase
from sqlalchemy import Column, Integer, String


class PlatformDimORM(ModelsBase):
    __tablename__ = "DIM_PLATFORM"

    # Primary Keys
    Platform_ID = Column(Integer, primary_key=True, nullable=False)

    # Fields
    PlatformName = Column(String, nullable=False)
    PlatformType = Column(String, nullable=False)

    def __repr__(self):
        return (
            f"<PlatformDimORM(Platform_ID={self.Platform_ID}, PlatformName='{self.PlatformName}', "
            f"PlatformType='{self.PlatformType}')>"
        )
