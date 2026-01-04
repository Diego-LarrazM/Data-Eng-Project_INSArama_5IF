from . import ModelsBase
from sqlalchemy import Column, Integer, String


class RoleORM(ModelsBase):
    __tablename__ = "ROLES"

    # Primary Keys
    id = Column(Integer, primary_key=True, nullable=False)

    # Fields
    person_name = Column(String, nullable=False)
    play_method = Column(String, nullable=True)
    role = Column(String, nullable=False)

    def __repr__(self):
        return (
            f"<RoleORM(id={self.id}, person_name='{self.person_name}', "
            f"role='{self.role}', play_method='{self.play_method}')>"
        )
