from . import ModelsBase
from sqlalchemy import Column, Integer, Float, ForeignKey
from sqlalchemy.orm import relationship

class MediaRoleBridgeORM(ModelsBase):
  __tablename__ = "BRIDGE_MEDIA_ROLE"
  
  # Primary Keys
  MediaInfo_ID = Column(Integer, ForeignKey("DIM_MEDIA_INFO.MediaInfo_ID"), primary_key=True, nullable=False)
  Role_ID = Column(Integer, ForeignKey("ROLES.Role_ID"), primary_key=True, nullable=False)

  # Fields
  Weight = Column(Float, nullable=False)

  # ORM relationships
  media = relationship("MediaInfoDimORM")
  role = relationship("RoleORM")

  def __repr__(self):
    return f"<MediaRoleBridgeORM(MediaInfo_ID={self.MediaInfo_ID}, Role_ID={self.Role_ID}, Weight={self.Weight})>"