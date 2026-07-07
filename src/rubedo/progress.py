from typing import Dict
from rich.live import Live
from rich.table import Table

class TerminalProgress:
    def __init__(self):
        self.stats: Dict[str, Dict[str, int]] = {}
        self.live = Live(self._build_table(), refresh_per_second=4)

    def _build_table(self) -> Table:
        table = Table(title="Pipeline Progress")
        table.add_column("Step")
        table.add_column("Created", style="green")
        table.add_column("Reused", style="blue")
        table.add_column("Filtered", style="dim")
        table.add_column("Failed", style="red")
        table.add_column("Blocked", style="yellow")

        for step_name, counts in self.stats.items():
            table.add_row(
                step_name,
                str(counts["created"]) if counts["created"] else "-",
                str(counts["reused"]) if counts["reused"] else "-",
                str(counts["filtered"]) if counts["filtered"] else "-",
                str(counts["failed"]) if counts["failed"] else "-",
                str(counts["blocked"]) if counts["blocked"] else "-",
            )
        return table

    def update(self, step_name: str, coordinate: str, status: str) -> None:
        if step_name not in self.stats:
            self.stats[step_name] = {"created": 0, "reused": 0, "filtered": 0, "failed": 0, "blocked": 0}
        
        if status in self.stats[step_name]:
            self.stats[step_name][status] += 1
            
        self.live.update(self._build_table())

    def __enter__(self):
        self.live.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.live.__exit__(exc_type, exc_val, exc_tb)
