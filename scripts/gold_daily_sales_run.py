from medallion_project.pipelines.gold_daily_sales import GoldDailySalesPipeline


def main() -> None:
    pipeline = GoldDailySalesPipeline()
    pipeline.run()


if __name__ == "__main__":
    main()
