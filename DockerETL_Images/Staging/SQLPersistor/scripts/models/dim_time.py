from . import ModelsBase
from sqlalchemy import Column, Integer


class TimeDimORM(ModelsBase):
    __tablename__ = "DIM_TIME"

    # Primary Keys
    id = Column(Integer, primary_key=True, nullable=False)

    # Fields
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    day = Column(Integer, nullable=False)

    def __repr__(self):
        return (
            f"<TimeDimORM(id={self.id}, year={self.year}, "
            f"month={self.month}, day={self.day})>"
        )
