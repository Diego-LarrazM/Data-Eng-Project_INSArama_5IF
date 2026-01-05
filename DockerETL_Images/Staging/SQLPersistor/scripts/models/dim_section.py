from . import ModelsBase
from sqlalchemy import Column, Integer, String


class SectionDimORM(ModelsBase):
    __tablename__ = "DIM_SECTION"

    # Primary Keys
    id = Column(String, primary_key=True, nullable=False)

    # Fields
    section_type = Column(String, nullable=False)
    section_group = Column(String, nullable=True)
    section_name = Column(String, nullable=False)

    def __repr__(self):
        return (
            f"<PlatformDimORM(id={self.id}, section_type='{self.section_type}', "
            f"section_group='{self.section_group}', section_name='{self.section_name}')>"
        )
