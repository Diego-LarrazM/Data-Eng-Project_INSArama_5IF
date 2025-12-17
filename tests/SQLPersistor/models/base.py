from sqlalchemy.orm import declarative_base, DeclarativeMeta
from typing import TypeVar

ModelsBase = declarative_base()
ModelType = TypeVar("ModelType", bound=DeclarativeMeta)
