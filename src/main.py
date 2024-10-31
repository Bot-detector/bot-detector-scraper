import argparse
import asyncio

from hiscore.main import main as run_hiscore_worker
from runemetrics.main import main as run_runemetrics_worker


def main():
    parser = argparse.ArgumentParser(description="Highscore Worker Script")
    parser.add_argument(
        "--run-hs-worker",
        action="store_true",
        help="Start the highscore worker.",
    )
    parser.add_argument(
        "--run-rm-worker",
        action="store_true",
        help="Start the highscore worker.",
    )

    args = parser.parse_args()

    if args.run_hs_worker:
        asyncio.run(run_hiscore_worker())
    elif args.run_rm_worker:
        asyncio.run(run_runemetrics_worker())
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
