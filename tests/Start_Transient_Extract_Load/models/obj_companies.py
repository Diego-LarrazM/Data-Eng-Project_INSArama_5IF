from . import ModelsBase
from sqlalchemy import Column, Integer, Float, String

class CompanyORM(ModelsBase):
  __tablename__ = "COMPANIES"
  
  # Primary Keys
  Company_ID = Column(Integer, primary_key=True, nullable=False)

  # Fields
  CompanyName = Column(String, nullable=False)
  #Networth = Column(Float, nullable=True)

  def __repr__(self):
    return f"<CompanyORM(Company_ID={self.Company_ID}, CompanyName='{self.CompanyName}')>"