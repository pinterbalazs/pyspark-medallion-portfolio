from pathlib import Path

from medallion_project.common.manifest import load_manifest

MANIFEST_TOML = """
[dataset]
name = "demo"
raw_dir = "data/raw/demo"
bronze_dir = "data/bronze/demo"
format = "csv"

[dataset.default_options]
header = true
encoding = "UTF-8"

[[tables]]
name = "alpha"
file = "alpha.csv"

[[tables]]
name = "beta"
file = "beta.csv"
[tables.options]
multiLine = true
escape = '"'
"""


def write_manifest(path: Path) -> None:
    path.write_text(MANIFEST_TOML, encoding="utf-8")


def test_load_manifest_parses_dataset_and_tables(tmp_path: Path) -> None:
    manifest_path = tmp_path / "demo.toml"
    write_manifest(manifest_path)

    manifest = load_manifest(manifest_path)

    assert manifest.name == "demo"
    assert manifest.raw_dir == "data/raw/demo"
    assert manifest.bronze_dir == "data/bronze/demo"
    assert len(manifest.tables) == 2
    assert [t.name for t in manifest.tables] == ["alpha", "beta"]


def test_default_options_merge_and_per_table_override(tmp_path: Path) -> None:
    manifest_path = tmp_path / "demo.toml"
    write_manifest(manifest_path)

    manifest = load_manifest(manifest_path)
    by_name = {t.name: t for t in manifest.tables}

    # Defaults apply to every table.
    assert by_name["alpha"].options["header"] == "true"
    assert by_name["alpha"].options["encoding"] == "UTF-8"

    # beta inherits the defaults and adds its own options.
    assert by_name["beta"].options["header"] == "true"
    assert by_name["beta"].options["multiLine"] == "true"
    assert by_name["beta"].options["escape"] == '"'


def test_path_helpers_use_forward_slashes(tmp_path: Path) -> None:
    manifest_path = tmp_path / "demo.toml"
    write_manifest(manifest_path)

    manifest = load_manifest(manifest_path)
    alpha = manifest.tables[0]

    assert manifest.source_path(alpha) == "data/raw/demo/alpha.csv"
    assert manifest.target_path(alpha) == "data/bronze/demo/alpha"
