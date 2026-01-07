from . import ModelsBase
from sqlalchemy import Column, String, Float, ForeignKey
from sqlalchemy.orm import relationship


class MediaRoleBridgeORM(ModelsBase):
    __tablename__ = "BRIDGE_MEDIA_ROLE"

    # Primary Keys
    media_id = Column(
        String,
        ForeignKey("DIM_MEDIA_INFO.id"),
        primary_key=True,
        nullable=False,
    )
    role_id = Column(String, ForeignKey("ROLES.id"), primary_key=True, nullable=False)

    # Fields
    weight = Column(Float, nullable=False)

    # ORM relationships
    media = relationship("MediaInfoDimORM")
    role = relationship("RoleORM")

    def __repr__(self):
        return f"<MediaRoleBridgeORM(media_id={self.media_id}, role_id={self.role_id}, weight={self.weight})>"
