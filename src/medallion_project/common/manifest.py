import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True)
class TableSpec:
    name: str
    file: str
    fmt: str = "csv"
    options: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class BronzeManifest:
    name: str
    raw_dir: str
    bronze_dir: str
    tables: tuple[TableSpec, ...]

    def source_path(self, spec: TableSpec) -> str:
        return f"{self.raw_dir}/{spec.file}"

    def target_path(self, spec: TableSpec) -> str:
        return f"{self.bronze_dir}/{spec.name}"


def _normalize_options(options: Mapping[str, object]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in options.items():
        if isinstance(value, bool):
            normalized[key] = str(value).lower()
        else:
            normalized[key] = str(value)
    return normalized


def load_manifest(path: str | Path) -> BronzeManifest:
    with open(path, "rb") as handle:
        raw = tomllib.load(handle)

    dataset = raw["dataset"]
    default_format = dataset.get("format", "csv")
    default_options = _normalize_options(dataset.get("default_options", {}))

    tables: list[TableSpec] = []
    for entry in raw.get("tables", []):
        merged = dict(default_options)
        merged.update(_normalize_options(entry.get("options", {})))
        tables.append(
            TableSpec(
                name=entry["name"],
                file=entry["file"],
                fmt=entry.get("format", default_format),
                options=merged,
            )
        )

    return BronzeManifest(
        name=dataset["name"],
        raw_dir=dataset["raw_dir"],
        bronze_dir=dataset["bronze_dir"],
        tables=tuple(tables),
    )
