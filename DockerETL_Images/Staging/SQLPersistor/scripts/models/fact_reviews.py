from . import ModelsBase
from sqlalchemy import Column, Integer, ForeignKey, String
from sqlalchemy.orm import relationship


class ReviewsFactORM(ModelsBase):
    __tablename__ = "FACT_REVIEWS"

    # Primary Keys
    reviewer_id = Column(
        String,
        ForeignKey("DIM_REVIEWER.id"),
        primary_key=True,
        nullable=False,
    )
    time_id = Column(
        String, ForeignKey("DIM_TIME.id"), primary_key=True, nullable=False
    )
    section_id = Column(
        String,
        ForeignKey("DIM_SECTION.id"),
        primary_key=True,
        nullable=False,
    )
    media_info_id = Column(
        String,
        ForeignKey("DIM_MEDIA_INFO.id"),
        primary_key=True,
        nullable=False,
    )

    # Facts
    rating = Column(Integer, nullable=False)

    # ORM relationships
    reviewer = relationship("ReviewerDimORM")
    time = relationship("TimeDimORM")
    section = relationship("SectionDimORM")
    mediainfo = relationship("MediaInfoDimORM")

    def __repr__(self):
        return (
            f"<ReviewsFactORM(reviewer_id={self.reviewer_id}, time_id={self.time_id}, "
            f"section_id={self.section_id}, media_info_id={self.media_info_id}, "
            f"rating={self.rating})>"
        )
