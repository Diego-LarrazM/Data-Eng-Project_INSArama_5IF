from . import ModelsBase
from sqlalchemy import Column, Integer, ForeignKey, String
from sqlalchemy.orm import relationship

class ReviewsFactORM(ModelsBase):
  __tablename__ = "FACT_REVIEWS"
  
  # Primary Keys
  Reviewer_ID = Column(Integer, ForeignKey("dim_reviewer.Reviewer_ID"), primary_key=True, nullable=False)
  Time_ID = Column(Integer, ForeignKey("dim_time.Time_ID"), primary_key=True, nullable=False)
  Platform_ID = Column(Integer, ForeignKey("dim_platform.Platform_ID"), primary_key=True, nullable=False)
  MediaInfo_ID = Column(Integer, ForeignKey("dim_media_info.MediaInfo_ID"), primary_key=True, nullable=False)
  #Franchise_ID = Column(Integer, ForeignKey("dim_franchise.Franchise_ID"), primary_key=True, nullable=False)
  FranchiseTitle = Column(String, primary_key=True, nullable=False) # Degenerate dimension to reduce overhead

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