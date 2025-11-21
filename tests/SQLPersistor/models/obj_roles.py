from . import ModelsBase
from sqlalchemy import Column, Integer, String

class RoleORM(ModelsBase):
  __tablename__ = "ROLES"
  
  # Primary Keys
  Role_ID = Column(Integer, primary_key=True, nullable=False)

  # Fields
  PersonName = Column(String, nullable=False)
  RoleTitle = Column(String, nullable=False)
  PlayMethod = Column(String, nullable=False)

  def __repr__(self):
    return (f"<RoleORM(Role_ID={self.Role_ID}, PersonName='{self.PersonName}', "
            f"RoleTitle='{self.RoleTitle}', PlayMethod='{self.PlayMethod}')>")