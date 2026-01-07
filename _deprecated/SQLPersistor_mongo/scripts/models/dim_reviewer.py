from . import ModelsBase
from sqlalchemy import Column, Integer, String, Boolean


class ReviewerDimORM(ModelsBase):
    __tablename__ = "DIM_REVIEWER"

    # Primary Keys
    Reviewer_ID = Column(Integer, primary_key=True, nullable=False)

    # Fields
    ReviewerUsername = Column(
        String, nullable=True
    )  # not all reviewers post their name
    IsCritic = Column(Boolean, nullable=False)
    Association = Column(String, nullable=True)  # might be a user, non-critic

    def __repr__(self):
        return (
            f"<ReviewerDimORM(Reviewer_ID={self.Reviewer_ID}, ReviewerUsername='{self.ReviewerUsername}', "
            f"IsCritic={self.IsCritic}, Association='{self.Association}')>"
        )
