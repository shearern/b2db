import os, sys
import yaml
import argparse


def parse_args():

    parser = argparse.ArgumentParser(
        description = "Build model helper classes"
    )

    parser.add_argument(
        'model',
        type=str,
        default = 'model.yml',
        nargs=1,
        help = "Path to model file")

    parser.add_argument(
        'target',
        type=str,
        default = 'model.py',
        nargs=1,
        help = "Path to python file to write to")

    return parser.parse_args(sys.argv[1:])



if __name__ == '__main__':

    args = parse_args()
