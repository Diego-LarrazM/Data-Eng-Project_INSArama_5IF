from . import ModelsBase
from sqlalchemy import Column, Integer, String


class TimeDimORM(ModelsBase):
    __tablename__ = "DIM_TIME"

    # Primary Keys
    id = Column(String, primary_key=True, nullable=False)

    # Fields
    year = Column(Integer, nullable=True)
    month = Column(Integer, nullable=True)
    day = Column(Integer, nullable=True)

    def __repr__(self):
        return (
            f"<TimeDimORM(id={self.id}, year={self.year}, "
            f"month={self.month}, day={self.day})>"
        )
