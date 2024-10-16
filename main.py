import argparse

from db_manager import DbManager
from window import run_gui


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-g', '--gui', action='store_true', help="Run GUI")
    args = parser.parse_args()

    if args.gui:
        run_gui()
    else:
        dbm = DbManager()
        dbm.load()
