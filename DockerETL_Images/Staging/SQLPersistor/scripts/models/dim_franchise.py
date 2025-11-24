from . import ModelsBase
from sqlalchemy import Column, Integer, String

class FranchiseDimORM(ModelsBase):
  __tablename__ = "DIM_FRANCHISE"
  
  # Primary Keys
  Franchise_ID = Column(Integer, primary_key=True, nullable=False)

  # Fields
  FranchiseTitle = Column(String, nullable=False)

  def __repr__(self):
    return f"<FranchiseDimORM(Franchise_ID={self.Franchise_ID}, FranchiseTitle='{self.FranchiseTitle}')>"