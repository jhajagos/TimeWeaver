"""
    Modes:
        scan:
            Must be run first
            Generates positional mappings
            expanded categorical variables

        csv:
            Outputs sequences into CSV files
            dynamic
            static

        hdf5:
            Output data to structured HDF5 format
            Must be run after csv
            zero padding for ending
            maximum number of steps

"""


import argparse
from utilities import JsonLineReader
import json
import os
import csv


class CSVWriter(object):

    def __init__(self, subject, file_name, scan_obj, separator="|", numeric_functions=["_mean", "_min", "_max"]):

        self.subject = subject
        self.file_name = file_name
        self.scan_obj = scan_obj

        self.fw = open(self.file_name, mode="w", newline="")
        self.csv_writer = csv.writer(self.fw)

        self.numeric_functions = numeric_functions
        self.separator = separator
        self._define_subject()
        self._define_data_columns()

        self.csv_writer.writerow(self.header)

    def _add_meta_data_columns(self):
        pass

    def _add_meta_data_header(self):
        pass

    def _define_subject(self):
        self.subject_dict = self.scan_obj.get_subject(self.subject)

    def _define_data_columns(self):
        self._define_mapping_dicts()

        self.column_positions = {"id": [0], "_i_id": [1]}  # ID is always column 0

        # Order of columns
        self.data_columns = []

        i = 2

        for key in sorted(self.numeric_dict):
            self.column_positions[key] = [i]
            i += 1

        j = i
        for key in sorted(self.numeric_list_dict):
            data_keys = self.numeric_list_dict[key]
            number_of_data_keys = len(data_keys)
            self.column_positions[key] = [0] * number_of_data_keys
            for data_key in data_keys:
                position = data_keys[data_key]
                self.column_positions[key][position] = j + position

            j += number_of_data_keys


        k = j
        data_keys = []
        for key in sorted(self.categorical_dict):

            data_keys = self.categorical_dict[key]
            number_of_data_keys = len(data_keys)
            self.column_positions[key] = [0] * number_of_data_keys
            for data_key in data_keys:
                position = data_keys[data_key]
                self.column_positions[key][position] = k + position
            k += number_of_data_keys

        self.number_of_data_columns = k
        self.number_of_columns = k

        self._add_meta_data_columns()
        self.header = [''] * self.number_of_columns
        self.header[0] = "id"
        self.header[1] = "_i_id"

        i = 2
        for key in sorted(self.numeric_dict.keys()):
            self.header[i] = key
            i += 1

        for key in sorted(self.numeric_list_dict):
            data_keys = self.numeric_list_dict[key]
            z = 0
            key_list = []
            for data_key in data_keys:
                key_list += [(z, data_key)]
                z += 1

            key_list.sort(key=lambda x: x[0])

            starting_position = self.column_positions[key][0]

            for item in key_list:
                self.header[starting_position + item[0]] = key + self.separator + str(item[1][1:])

        for key in sorted(self.categorical_dict):
            data_keys = self.categorical_dict[key]
            z = 0
            key_list = []
            for data_key in data_keys:
                key_list += [(z, data_key)]
                z += 1
            key_list.sort(key=lambda x: x[0])
            starting_position = self.column_positions[key][0]

            for item in key_list:
                self.header[starting_position + item[0]] = key + self.separator + str(item[1])

        self._add_meta_data_header()

        if "_sequence_i" in self.header:
            self.data_columns = self.header[2:-4]
            self.meta_data_columns = self.header[-4:]
        else:
            self.data_columns = self.header[2:]
            self.meta_data_columns = []

        self.id_columns = self.header[0:2]

    def _define_mapping_dicts(self):
        keys = self.scan_obj.get_subject_keys(self.subject)

        self.categorical_dict = {}
        self.numeric_list_dict = {}
        self.numeric_dict = {}

        for key in keys:
            subject_value_key = self.subject_dict[key]
            if subject_value_key.__class__ == [].__class__:
                if len(subject_value_key):
                    if subject_value_key[0] in self.numeric_functions:
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

    def _write(self, id_value, row_id, objects_dict, subject_type):

        keys_to_ignore = ["id", "meta"]

        row_template = [''] * self.number_of_columns
        row_template[0] = id_value
        row_template[1] = row_id

        if objects_dict.__class__ != [].__class__:
            objects_dict = [objects_dict]

        for object_dict in objects_dict:
            keys = [k for k in object_dict.keys() if k not in keys_to_ignore]
            row_to_write = list(row_template)
            for key in keys:
                data_value = object_dict[key]

                if key in self.column_positions:
                    data_positions = self.column_positions[key]
                    if key in self.numeric_dict:
                        row_to_write[data_positions[0]] = str(data_value)
                    elif key in self.numeric_list_dict:
                        operations = self.numeric_list_dict[key]

                        for operation_key in operations:
                            if operation_key == "_mean":
                                row_to_write[data_positions[0] + operations[operation_key]] = mean(data_value)
                            elif operation_key == "_min":
                                row_to_write[data_positions[0] + operations[operation_key]] = min(data_value)
                            elif operation_key == "_max":
                                row_to_write[data_positions[0] + operations[operation_key]] = max(data_value)

                    elif key in self.categorical_dict:

                        for data_position in data_positions:
                            row_to_write[data_position] = 0

                        if data_value.__class__ != [].__class__:
                            data_value = [data_value]

                        for datum in data_value:
                            row_to_write[data_positions[0] + self.categorical_dict[key][datum]] = 1

            if subject_type == "dynamic":
                row_to_write[-4] = str(object_dict["meta"]["sequence_time_delta"])
                row_to_write[-3] = str(object_dict["meta"]["sequence_i"])
                row_to_write[-2] = str(object_dict["meta"]["start_time"])
                row_to_write[-1] = str(object_dict["meta"]["time_span"])

            self.csv_writer.writerow(row_to_write)

    def close(self):
        self.fw.close()


class StaticCSVWriter(CSVWriter):

    def write(self, id_value, row_id, object_dict):
        self._write(id_value, row_id, object_dict, "static")


class DynamicCSVWriter(CSVWriter):

    def write(self, id_value, row_id, object_dict):
        self._write(id_value, row_id, object_dict, "dynamic")

    def _add_meta_data_columns(self):
        i = self.number_of_data_columns
        self.column_positions["_sequence_time_delta"] = [i]
        i += 1
        self.column_positions["_sequence_i"] = [i]
        i += 1
        self.column_positions["_start_time"] = [i]
        i += 1
        self.column_positions["_time_span"] = [i]

        self.number_of_columns = i + 1

    def _add_meta_data_header(self):
        self.header[-1] = "_time_span"
        self.header[-2] = "_start_time"
        self.header[-3] = "_sequence_i"
        self.header[-4] = "_sequence_time_delta"


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


def scan_file(input_file_json_txt, directory, numeric_compress_functions=["_mean", "_min", "_max"]):

    keys_to_ignore = ["id", "meta"]

    line_reader_obj = JsonLineReader(input_file_json_txt)
    values_dict = {}
    count_dict = {}
    count_id_dict = {}

    histogram_sequence_dict = {}

    # Collect distribution of sequence

    z = 0
    for data_dict in line_reader_obj:

        subject_keys = [sk for sk in data_dict if sk not in keys_to_ignore]
        count_id_data_dict = {}

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

            if subject_key not in count_id_data_dict:
                count_id_data_dict[subject_key] = {}

            if subject_key not in count_id_dict:
                count_id_dict[subject_key] = {}

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

                                if item_key not in count_id_data_dict[subject_key]:
                                    count_id_data_dict[subject_key][item_key] = 1

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

                                if item_key not in count_id_data_dict[subject_key]:
                                    count_id_data_dict[subject_key][item_key] = 1
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

                        if item_key not in count_id_data_dict[subject_key]:
                            count_id_data_dict[subject_key][item_key] = 1

            if subject_key in count_id_data_dict:
                for sub_key in count_id_data_dict[subject_key]: # keep track how many times a variable appears
                    if sub_key in count_id_dict[subject_key]:
                        count_id_dict[subject_key][sub_key] += 1
                    else:
                        count_id_dict[subject_key][sub_key] = 1
        z += 1

    export_dict = {}
    export_dict["histogram"] = histogram_sequence_dict
    export_dict["values"] = values_dict
    export_dict["value_counts"] = count_dict
    export_dict["value_counts_id"] = count_id_dict

    export_file_name = os.path.join(directory, "class_scan.json")

    with open(export_file_name, "w") as fw:
        json.dump(export_dict, fw, sort_keys=True, indent=4, separators=(',', ': '))


def generate_csv_files(input_file_json_txt, directory, base_name):
    """Output CSV files of sequences"""

    scan_obj = ClassScan(directory)

    static_subjects = scan_obj.get_static_subjects()
    dynamic_subjects = scan_obj.get_dynamic_subjects()

    static_csv_file_names_dict = {ss: os.path.join(directory, base_name + "_" + ss + ".csv") for ss in static_subjects}
    dynamic_csv_file_names_dict = {ds: os.path.join(directory, base_name + "_" + ds + ".csv") for ds in dynamic_subjects}

    static_csv_obj_dict = {ss: StaticCSVWriter(ss, static_csv_file_names_dict[ss], scan_obj) for ss in static_subjects}
    dynamic_csv_obj_dict = {ds: DynamicCSVWriter(ds, dynamic_csv_file_names_dict[ds], scan_obj) for ds in dynamic_subjects}

    data_reader = JsonLineReader(input_file_json_txt)

    z = 0 # keep track of how many data items were processed
    for data_dict in data_reader:
        subjects = [k for k in list(data_dict.keys()) if k != "id"]
        id_value = data_dict["id"]

        for subject in subjects:

            if subject in static_csv_obj_dict:
                static_csv_obj_dict[subject].write(id_value, z, data_dict[subject])
            elif subject in dynamic_csv_obj_dict:
                dynamic_csv_obj_dict[subject].write(id_value, z, data_dict[subject])

        z += 1

    csv_json_meta_file_name = os.path.join(directory, "csv_input_data.json")

    with open(csv_json_meta_file_name, "w") as fw:
        csv_meta_data_dict = {"dynamic": {}, "static": {}}
        for key in dynamic_csv_obj_dict:
            csv_meta_data_dict["dynamic"][key] = {"file_name": dynamic_csv_obj_dict[key].file_name}
            csv_meta_data_dict["dynamic"][key]["header"] = dynamic_csv_obj_dict[key].header
            csv_meta_data_dict["dynamic"][key]["data_columns"] = dynamic_csv_obj_dict[key].header
            csv_meta_data_dict["dynamic"][key]["_i_id"] = z
            csv_meta_data_dict["dynamic"][key]["data_columns"] = dynamic_csv_obj_dict[key].data_columns
            csv_meta_data_dict["dynamic"][key]["meta_data_columns"] = dynamic_csv_obj_dict[key].meta_data_columns
            csv_meta_data_dict["dynamic"][key]["id_columns"] = dynamic_csv_obj_dict[key].id_columns

        for key in static_csv_obj_dict:
            csv_meta_data_dict["static"][key] = {"file_name": static_csv_obj_dict[key].file_name}
            csv_meta_data_dict["static"][key]["header"] = static_csv_obj_dict[key].header
            csv_meta_data_dict["static"][key]["_i_id"] = z
            csv_meta_data_dict["static"][key]["data_columns"] = static_csv_obj_dict[key].data_columns
            csv_meta_data_dict["static"][key]["meta_data_columns"] = static_csv_obj_dict[key].meta_data_columns
            csv_meta_data_dict["static"][key]["id_columns"] = static_csv_obj_dict[key].id_columns

        json.dump(csv_meta_data_dict, fw, sort_keys=True, indent=4, separators=(',', ': '))


def generate_hdf5_files(input_file_json_txt, directory, base_name):
    print("Feature is currently not implemented")


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

    main(arg_obj.input_file_json_txt, arg_obj.command, arg_obj.directory, arg_obj.base_name)