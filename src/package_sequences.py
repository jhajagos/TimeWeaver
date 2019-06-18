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
import pprint
import os


def scan_file(input_file_json_txt, directory, base_name, numeric_compress_functions=["mean", "min"]):

    keys_to_ignore = ["id", "meta"]

    line_reader_obj = JsonLineReader(input_file_json_txt)
    values_dict = {}
    count_dict = {}
    histogram_sequence_dict = {}

    # Collect distribution of sequence

    for data_dict in line_reader_obj:

        subject_keys = [sk for sk in data_dict if sk not in keys_to_ignore]

        for subject_key in subject_keys:
            subjects = data_dict[subject_key]
            if subjects.__class__ != [].__class__:
                subjects = [subjects]
            else:
                if subject_key not in histogram_sequence_dict:
                    histogram_sequence_dict[subject_key] = {}
                number_of_items = len(subjects)
                if number_of_items in histogram_sequence_dict[subject_key]:
                    histogram_sequence_dict[subject_key][number_of_items] += 1
                else:
                    histogram_sequence_dict[subject_key][number_of_items] = 1

            if subject_key not in values_dict:
                values_dict[subject_key] = {}
                count_dict[subject_key] = {}

            for subject in subjects:

                item_keys = [ik for ik in subject.keys() if ik not in keys_to_ignore]

                for item_key in item_keys:
                    subject_item_key = subject[item_key]

                    if subject_item_key.__class__ == [].__class__: # Deal with lists
                        if len(subject_item_key):
                            if subject_item_key[0].__class__ in (int, float):
                                if item_key not in values_dict[subject_key]:
                                    values_dict[subject_key][item_key] = numeric_compress_functions
                                    count_dict[subject_key][item_key] = 1
                                else:
                                    count_dict[subject_key][item_key] += 1
                            else:
                                if item_key not in values_dict[subject_key]:
                                    values_dict[subject_key][item_key] = list(set(subject_item_key))
                                    count_dict[subject_key][item_key] = 1
                                else:
                                    for sub_item in subject_item_key:
                                        value_subject_item_key = values_dict[subject_key][item_key]
                                        if sub_item not in value_subject_item_key:
                                            values_dict[subject_key][item_key] += [sub_item]
                                    count_dict[subject_key][item_key] += 1

                    else:
                        if item_key not in values_dict[subject_key]:
                            if subject_item_key.__class__ == "".__class__:
                                values_dict[subject_key][item_key] = [subject_item_key]
                            else:

                                if subject_item_key.__class__ == float:
                                    values_dict[subject_key][item_key] = "float"
                                elif subject_item_key.__class__ == int:
                                    values_dict[subject_key][item_key] = "int"
                            count_dict[subject_key][item_key] = 1
                        else:
                            value_subject_item_key = values_dict[subject_key][item_key]
                            if value_subject_item_key.__class__ == [].__class__:
                                if subject_item_key not in value_subject_item_key:
                                    values_dict[subject_key][item_key] += [subject_item_key]
                            count_dict[subject_key][item_key] += 1

    export_dict = {}
    export_dict["histogram"] = histogram_sequence_dict
    export_dict["values"] = values_dict
    export_dict["value_counts"] = count_dict

    pprint.pprint(export_dict)



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