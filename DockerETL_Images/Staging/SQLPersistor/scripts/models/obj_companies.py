from . import ModelsBase
from sqlalchemy import Column, String, String


class CompanyORM(ModelsBase):
    __tablename__ = "COMPANIES"

    # Primary Keys
    id = Column(String, primary_key=True, nullable=False)

    # Fields
    company_role = Column(String, nullable=False)
    company_name = Column(String, nullable=False)

    def __repr__(self):
        return f"<CompanyORM(id={self.id}, company_role='{self.company_role}' company_name='{self.company_name}')>"
