"""
    Modes:
        Scan:
            Must be run first
            Generates positional mappings
            expanded categorical variables

        Output CSV
            dynamics
            static - maps

        Output data to HDF5
            zero padding for ending
            maximum number of steps

"""


import argparse
from utilities import JsonLineReader
import json


def scan_file(input_file_json_txt, directory, base_name):

    line_reader_obj = JsonLineReader(input_file_json_txt)
    values_dict = {}

    for data_dict in line_reader_obj:
        print(data_dict)


def generate_csv_files(input_file_json_txt, directory, base_name):
    pass


def generate_hdf5_files(input_file_json_txt, directory, base_name):
    pass


def main(input_file_json_txt, command, directory, base_name):

    if command == "scan":
        scan_file(input_file_json_txt, directory, base_name)
    elif command == "csv":
        generate_csv_files(input_file_json_txt, directory, base_name)
    elif command == "hdf5":
        generate_hdf5_files(input_file_json_txt, directory, base_name)


if __name__ == "__main__":

    arg_parse_obj = argparse.ArgumentParser(description="Package sequences for analysis in either CSV or HDF5")
    arg_parse_obj.add_argument("-f", "--input-file-json-txt", dest="input_file_json_txt", required=True)
    arg_parse_obj.add_argument("-c", "--command", dest="command", required=True, help="Modes: scan, csv, hdf5")
    arg_parse_obj.add_argument("-b", "--base-name", dest="base_name", default="sequence", help="base name for files")
    arg_parse_obj.add_argument("-d", "--directory", dest="directory", default="./", help="Directory to write files to")

    arg_obj = arg_parse_obj.parse_args()