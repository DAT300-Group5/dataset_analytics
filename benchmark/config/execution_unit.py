from dataclasses import dataclass

from consts.EngineType import EngineType


@dataclass
class ExecutionUnit:

    group_id: str
    engine : EngineType

    def __iter__(self):
        yield self.group_id
        yield self.engine