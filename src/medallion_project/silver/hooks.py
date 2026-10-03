from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def geolocation_collapse(df: DataFrame) -> DataFrame:
    """Collapse the raw geolocation point cloud to one representative row per zip prefix.

    ~1M raw points (many per zip, with exact-duplicate rows) become 19k rows keyed by
    geolocation_zip_code_prefix, so downstream zip joins do not fan out. Representative
    lat/lng via median; city/state via the most frequent (modal) value.
    """
    return (
        df.groupBy("geolocation_zip_code_prefix")
        .agg(
            F.percentile_approx("geolocation_lat", 0.5).alias("geolocation_lat"),
            F.percentile_approx("geolocation_lng", 0.5).alias("geolocation_lng"),
            F.mode("geolocation_city").alias("geolocation_city"),
            F.mode("geolocation_state").alias("geolocation_state"),
        )
    )


# Named, dataset-agnostic registry of bespoke transforms referenced by manifest `transform`.
TRANSFORMS = {
    "geolocation_collapse": geolocation_collapse,
}
