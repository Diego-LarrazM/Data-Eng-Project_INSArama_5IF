from . import ModelsBase
from sqlalchemy import Column, Integer, Float, ForeignKey
from sqlalchemy.orm import relationship


class MediaCompanyBridgeORM(ModelsBase):
    __tablename__ = "BRIDGE_MEDIA_COMPANY"

    # Primary Keys
    media_id = Column(
        Integer,
        ForeignKey("DIM_MEDIA_INFO.id"),
        primary_key=True,
        nullable=False,
    )
    company_id = Column(
        Integer, ForeignKey("COMPANIES.id"), primary_key=True, nullable=False
    )

    # Fields
    weight = Column(Float, nullable=False)

    # ORM relationships
    media = relationship("MediaInfoDimORM")
    company = relationship("CompanyORM")

    def __repr__(self):
        return f"<MediaCompanyBridgeORM(media_id={self.media_id}, company_id={self.company_id}, weight={self.weight})>"
