from medallion_project.pipelines.silver_orders import SilverOrdersPipeline


def main() -> None:
    pipeline = SilverOrdersPipeline()
    pipeline.run()


if __name__ == "__main__":
    main()