from dataclasses import dataclass, field


@dataclass
class LineageContext:
    run_id: str
    steps: list[str] = field(default_factory=list)

    def add_step(self, step: str) -> None:
        self.steps.append(step)

    def as_mermaid(self) -> str:
        lines = ["flowchart TD"]
        for idx, step in enumerate(self.steps):
            lines.append(f"S{idx}[\"{step}\"]")
            if idx > 0:
                lines.append(f"S{idx-1} --> S{idx}")
        return "\n".join(lines)
