from dataclasses import dataclass
from typing import List, Optional


@dataclass()
class Job:
    name: str
    data: Optional[List[dict]] = None
