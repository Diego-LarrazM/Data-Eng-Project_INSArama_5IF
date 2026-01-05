from . import ModelsBase
from sqlalchemy import Column, Integer, String, Boolean


class ReviewerDimORM(ModelsBase):
    __tablename__ = "DIM_REVIEWER"

    # Primary Keys
    id = Column(String, primary_key=True, nullable=False)

    # Fields
    association = Column(String, nullable=True)  # might be a user, non-critic
    is_critic = Column(Boolean, nullable=False)
    reviewer_username = Column(
        String, nullable=True
    )  # not all reviewers post their name

    def __repr__(self):
        return (
            f"<ReviewerDimORM(id={self.id}, association='{self.association}', "
            f"is_critic={self.is_critic} , reviewer_username='{self.reviewer_username}')>"
        )
