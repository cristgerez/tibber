import argparse
from src.jobs.currencies import CurrencyUpdate
from src.jobs.item_price_batch_processing import BatchItemProcessing
from src.jobs.prices_in_nok_view import NokView

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run specific data pipeline jobs.")
    parser.add_argument(
        "--job", type=str, choices=["currencies", "batch", "view", "all"], default="all",
        help="Specify which job to run"
    )
    parser.add_argument(
        "--files", nargs="*", default=None,
        help="Specify a list of file names to process in batch mode (only applies to --job batch)"
    )

    args = parser.parse_args()

    if args.job == "currencies":
        CurrencyUpdate().currency_task()

    elif args.job == "batch":
        BatchItemProcessing(file_name_list=args.files).batch_task()

    elif args.job == "view":
        NokView().create_view()

    elif args.job == "all":
        CurrencyUpdate().currency_task()
        BatchItemProcessing().batch_task()
        NokView().create_view()
