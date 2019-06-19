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
import os
import csv


class CSVWriter(object):

    def __init__(self, subject, file_name, scan_obj, separator="|"):

        self.subject = subject
        self.file_name = file_name
        self.scan_obj = scan_obj

        self.fw = open(self.file_name, "w")
        self.csv_writer = csv.writer(self.fw)

        self.separator = separator
        self._define_subject()
        self._define_data_columns()

    def _define_subject(self):
        self.subject_dict = self.scan_obj.get_subject(self.subject)

    def _define_data_columns(self):
        self._define_mapping_dicts()

    def _define_mapping_dicts(self):
        keys = self.scan_obj.get_subject_keys(self.subject)

        self.categorical_dict = {}
        self.numeric_list_dict = {}
        self.numeric_dict = {}

        for key in keys:
            subject_value_key = self.subject_dict[key]
            if subject_value_key.__class__ == [].__class__:
                if len(subject_value_key):
                    if subject_value_key[0].__class__ in (int, float):
                        i = 0
                        self.numeric_list_dict[key] = {}
                        for sub_key in subject_value_key:
                            self.numeric_list_dict[key][sub_key] = i
                            i += 1
                    else:
                        i = 0
                        self.categorical_dict[key] = {}
                        for sub_key in subject_value_key:
                            self.categorical_dict[key][sub_key] = i
                            i += 1
            else:
                self.numeric_dict[key] = subject_value_key

        print(self.categorical_dict)
        print(self.numeric_list_dict)
        print(self.numeric_dict)

    def _def_define_numeric_list_dict(self):
        pass

    def _write(self, id_value, object_dict, subject_type):
        pass

    def close(self):
        self.fw.close()


class StaticCSVWriter(CSVWriter):

    def write(self, id_value, object_dict):
        self._write(id_value, object_dict, "static")


class DynamicCSVWriter(CSVWriter):

    def write(self, object_dict):
        self._write(object_dict, "dynamic")


def mean(numeric_list):
    return sum(numeric_list) / len(numeric_list)


class ClassScan(object):

    def __init__(self, directory):
        self.file_name = os.path.join(directory, "class_scan.json")
        with open(self.file_name, "r") as f:
            self.scan = json.load(f)

        self.dynamic_subjects = ["carry_forward", "changes"]

    def get_topics(self):
        return list(self.scan.keys())

    def get_subjects(self):
        return list(self.scan["value_counts"])

    def get_dynamic_subjects(self):
        return [g for g in self.get_subjects() if g in self.dynamic_subjects]

    def get_static_subjects(self):
        return [g for g in self.get_subjects() if g not in self.dynamic_subjects]

    def get_subject(self, subject_name):
        return self.scan["values"][subject_name]

    def get_subject_keys(self, subject_name):
        return list(self.get_subject(subject_name).keys())


def scan_file(input_file_json_txt, directory, numeric_compress_functions=["mean", "min"]):

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

    export_file_name = os.path.join(directory, "class_scan.json")

    with open(export_file_name, "w") as fw:
        json.dump(export_dict, fw, sort_keys=True, indent=4, separators=(',', ': '))


def generate_csv_files(input_file_json_txt, directory, base_name):

    scan_obj = ClassScan(directory)

    static_subjects = scan_obj.get_static_subjects()
    # print(static_subjects)
    dynamic_subjects = scan_obj.get_dynamic_subjects()
    # print(dynamic_subjects)

    static_csv_file_names_dict = {ss: os.path.join(directory, base_name + "_" + ss + ".csv") for ss in static_subjects}
    dynamic_csv_file_names_dict = {ds: os.path.join(directory, base_name + "_" + ds + ".csv") for ds in dynamic_subjects}

    static_csv_obj_dict = {ss: StaticCSVWriter(ss, static_csv_file_names_dict[ss], scan_obj) for ss in static_subjects}
    dynamic_csv_obj_dict = {ds: DynamicCSVWriter(ds, dynamic_csv_file_names_dict[ds], scan_obj) for ds in dynamic_subjects}




def generate_hdf5_files(input_file_json_txt, directory, base_name):
    pass


def main(input_file_json_txt, command, directory, base_name):

    if command == "scan":
        scan_file(input_file_json_txt, directory)
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