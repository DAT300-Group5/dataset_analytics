from pathlib import Path


class DuckdbLogParser:
    def __init__(self, log_path):
        self.log_path = Path(log_path)

    def parse_log(self):

