# run_etl.py
from etl.config import ETLConfig
from etl.pipeline import run_etl_pipeline


def main() -> None:
    config = ETLConfig.from_cli()
    run_etl_pipeline(config)


if __name__ == "__main__":
    main()
