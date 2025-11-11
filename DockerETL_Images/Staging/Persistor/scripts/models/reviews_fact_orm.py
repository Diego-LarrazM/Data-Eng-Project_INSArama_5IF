from . import ModelsBase
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import relationship

class ReviewsFactORM(ModelsBase):
  __tablename__ = "reviews_fact"
  
  # Primary Keys
  Reviewer_ID = Column(Integer, ForeignKey("reviewer_dim.id"), nullable=False)
  Time_ID = Column(Integer, ForeignKey("time_dim.id"), nullable=False)
  Platform_ID = Column(Integer, ForeignKey("platform_dim.id"), nullable=False)
  MediaInfo_ID = Column(Integer, ForeignKey("mediainfo_dim.id"), nullable=False)
  Franchise_ID = Column(Integer, ForeignKey("franchise_dim.id"), nullable=False)

  # Facts
  RatingScore = Column(Integer, nullable=False)

  # ORM relationships
  reviewer = relationship("ReviewerDimORM")
  time = relationship("TimeDimORM")
  platform = relationship("PlatformDimORM")
  mediainfo = relationship("MediaInfoDimORM")
  franchise = relationship("FranchiseDimORM")

  def __repr__(self):
    return (f"<ReviewsFactORM(Reviewer_ID={self.Reviewer_ID}, Time_ID={self.Time_ID}, "
            f"Platform_ID={self.Platform_ID}, MediaInfo_ID={self.MediaInfo_ID}, "
            f"Franchise_ID={self.Franchise_ID}, RatingScore={self.RatingScore})>")