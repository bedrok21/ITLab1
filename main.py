import argparse

from window import run_gui


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--desktop', action='store_true', help="Run Descktop Version")
    args = parser.parse_args()

    if args.desktop:
        run_gui('d')
    else:
        run_gui('c')
