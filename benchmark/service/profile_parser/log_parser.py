from pathlib import Path

from service.profile_parser.query_metric import QueryMetrics


class LogParser:
    def __init__(self, log_path: Path):
        self.log_path = log_path

    def parse_log(self) -> QueryMetrics:
        raise NotImplementedError("Subclasses should implement this method.")
