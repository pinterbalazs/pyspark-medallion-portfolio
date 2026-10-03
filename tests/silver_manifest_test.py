from pathlib import Path

from medallion_project.common.manifest import load_silver_manifest

MANIFEST_TOML = """
[dataset]
name = "olist"
bronze_dir = "data/bronze/olist"
silver_dir = "data/silver/olist"

[[tables]]
name = "products"
source = "products"
strategy = "scd2"
business_key = ["product_id"]
tracked_attributes = ["product_category_name", "product_weight_g"]
[tables.renames]
product_name_lenght = "product_name_length"
[tables.casts]
product_weight_g = "int"

[[tables]]
name = "order_items"
source = "order_items"
strategy = "append_insert_only"
business_key = ["order_id", "order_item_id"]
[tables.casts]
order_item_id = "int"

[[tables]]
name = "geolocation"
source = "geolocation"
strategy = "snapshot_overwrite"
business_key = ["geolocation_zip_code_prefix"]
transform = "geolocation_collapse"
"""


def write_manifest(path: Path) -> None:
    path.write_text(MANIFEST_TOML, encoding="utf-8")


def test_load_silver_manifest_parses_dataset_and_tables(tmp_path: Path) -> None:
    manifest_path = tmp_path / "olist.toml"
    write_manifest(manifest_path)

    manifest = load_silver_manifest(manifest_path)

    assert manifest.name == "olist"
    assert manifest.bronze_dir == "data/bronze/olist"
    assert manifest.silver_dir == "data/silver/olist"
    assert [t.name for t in manifest.tables] == ["products", "order_items", "geolocation"]


def test_specs_carry_keys_strategy_and_typing(tmp_path: Path) -> None:
    manifest_path = tmp_path / "olist.toml"
    write_manifest(manifest_path)

    by_name = {t.name: t for t in load_silver_manifest(manifest_path).tables}

    products = by_name["products"]
    assert products.strategy == "scd2"
    assert products.business_key == ("product_id",)
    assert products.tracked_attributes == ("product_category_name", "product_weight_g")
    assert products.renames == {"product_name_lenght": "product_name_length"}
    assert products.casts == {"product_weight_g": "int"}
    assert products.transform is None

    order_items = by_name["order_items"]
    assert order_items.business_key == ("order_id", "order_item_id")
    assert order_items.casts == {"order_item_id": "int"}

    geolocation = by_name["geolocation"]
    assert geolocation.strategy == "snapshot_overwrite"
    assert geolocation.transform == "geolocation_collapse"


def test_path_helpers(tmp_path: Path) -> None:
    manifest_path = tmp_path / "olist.toml"
    write_manifest(manifest_path)

    manifest = load_silver_manifest(manifest_path)
    products = manifest.tables[0]

    assert manifest.source_path(products) == "data/bronze/olist/products"
    assert manifest.target_path(products) == "data/silver/olist/products"
