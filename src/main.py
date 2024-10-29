import argparse
import asyncio

from hiscore.hs_manager import main as run_hiscore_worker


def main():
    parser = argparse.ArgumentParser(description="Highscore Worker Script")
    parser.add_argument(
        "--run-hs-worker",
        action="store_true",
        help="Start the highscore worker.",
    )

    args = parser.parse_args()

    if args.run_hs_worker:
        asyncio.run(run_hiscore_worker())
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
