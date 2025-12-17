from . import ModelsBase
from sqlalchemy import Column, Integer


class TimeDimORM(ModelsBase):
    __tablename__ = "DIM_TIME"

    # Primary Keys
    Time_ID = Column(Integer, primary_key=True, nullable=False)

    # Fields
    Year = Column(Integer, nullable=False)
    Month = Column(Integer, nullable=False)
    Day = Column(Integer, nullable=False)

    def __repr__(self):
        return (
            f"<TimeDimORM(Time_ID={self.Time_ID}, Year={self.Year}, "
            f"Month={self.Month}, Day={self.Day})>"
        )
