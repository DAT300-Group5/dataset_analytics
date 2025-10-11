from dataclasses import dataclass, field
from typing import List, Tuple


@dataclass
class PlotParams:
    values: List[float]
    labels: List[str]
    colors: List[str]
    ylabel: str
    title: str
    output_path: str
    figsize: Tuple[float, float] = field(default_factory=tuple)
    rotation : int = 0
    annotate : bool = True
