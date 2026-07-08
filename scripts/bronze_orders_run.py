from medallion_project.pipelines.bronze_orders import BronzeOrdersPipeline


def main() -> None:
    pipeline = BronzeOrdersPipeline()
    pipeline.run()


if __name__ == "__main__":
    main()