from . import ModelsBase
from sqlalchemy import Column, Integer, Float, ForeignKey
from sqlalchemy.orm import relationship

class MediaCompanyBridgeORM(ModelsBase):
  __tablename__ = "BRIDGE_MEDIA_COMPANY"
  
  # Primary Keys
  MediaInfo_ID = Column(Integer, ForeignKey("dim_media_info.MediaInfo_ID"), primary_key=True, nullable=False)
  Company_ID = Column(Integer, ForeignKey("companies.Company_ID"), primary_key=True, nullable=False)

  # Fields
  Weight = Column(Float, nullable=False)

  # ORM relationships
  media = relationship("MediaInfoDimORM")
  company = relationship("CompanyORM")

  def __repr__(self):
    return f"<MediaCompanyBridgeORM(MediaInfo_ID={self.MediaInfo_ID}, Company_ID={self.Company_ID}, Weight={self.Weight})>"