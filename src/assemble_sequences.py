import csv
import json
import argparse
import datetime


class AssembleMappingConfig(object):
    """Holds the JSON configuration for how files are mapped"""

    def __init__(self, json_assemble_mapping_file_name):

        with open(json_assemble_mapping_file_name) as f:
            self.config = json.load(f)

        self._build_class_position_mapping()

    def _build_class_position_mapping(self):

        self._static_positions = {}
        self._dynamic_positions = {}

        i = 0
        for mapping in self.config["static"]:
            self._static_positions[mapping["class"]] = i
            i += 1

        i = 0
        for mapping in self.config["dynamic"]:
            self._dynamic_positions[mapping["class"]] = i

    def get_static_class(self, class_name):
        return self.config["static"][self._static_positions[class_name]]

    def get_dynamic_class(self, class_name):
        return self.config["dynamic"][self._dynamic_positions[class_name]]


class CSVBlockFile(object):

    """Class for reading a CSV file into memory. The assumption is that the file is stored """

    def __init__(self, file_name, block_field_name):
        self.file_name = file_name

        self.fp = open(file_name, newline="", mode="r")

        self.csv_dict = csv.DictReader(self.fp)

        self.block_field_name = block_field_name

        self.row_i = 0  # Index
        self.current_block_field_value = None
        self.past_block_field_value = None

        self.rows_buffer = []

    def __iter__(self):
        return self

    def __next__(self):
        for row_dict in self.csv_dict:
            self.row_i += 1
            self.current_block_field_value = row_dict[self.block_field_name]

            if self.past_block_field_value is None or self.past_block_field_value == self.current_block_field_value:
                self.rows_buffer += [row_dict]
                self.past_block_field_value = self.current_block_field_value
            else:
                self.past_block_field_value = self.current_block_field_value
                rows_dict = list(self.rows_buffer)
                self.rows_buffer = [rows_dict]
                return rows_dict

        if len(self.rows_buffer):
            rows_dict = list(self.rows_buffer)
            self.rows_buffer = []
            return rows_dict
        else:
            raise StopIteration


class Block(object):

    def __init__(self, block, class_config):
        self.class_config = class_config
        self.block = block

    def _config_date_time(self):
        date_time_config = self.class_config["date_time"]
        self.date_time_field_name = date_time_config["field_name"]
        self.date_time_format = date_time_config["format"]

    def _process_date_time(self, row_dict):

        date_time_string_value = row_dict[self.date_time_field_name]
        date_time_value = datetime.datetime.strptime(date_time_string_value, self.date_time_format)

        datetime_dict = {
            "unix_epoch_time": date_time_value.timestamp(),
            "original_date_time_value": date_time_string_value
        }

        return datetime_dict


class StaticBlockPrimaryProcess(Block):
    """The primary object must have a datetime field"""

    def process(self):
        self._config_date_time()

        list_to_process = []
        for item in self.block:
            item_dict = {}
            row_result = self._process_date_time(item)
            item_dict.update(row_result)
            list_to_process += [item_dict]

        return list_to_process


class StaticBlockAdditionalProcess(Block):

    def process(self):
        return None


class DynamicBlockProcess(Block):

    def process(self):
        self._config_date_time()


def main(json_file_assembly_mapping, input_directory, output_file_name):
    with open(json_file_assembly_mapping, "r") as f:
        assembly_mapping = json.load(f)




if __name__ == "__main__":

    arg_parse_obj = argparse.ArgumentParser(description="Generate a text JSON file which intermediate")

    arg_parse_obj.add_argument("-j", "--json-file-assembly-mapping", dest="json_file_assembly_mapping", required=True)
    arg_parse_obj.add_argument("-i", "--input-directory", dest="input_directory", default="./")
    arg_parse_obj.add_argument("-o", "--output-file-name", dest="output_file_name", default="./assembly_mapping.json.txt")

    arg_obj = arg_parse_obj.parse()

    main(arg_obj.json_assembly_mapping, arg_obj.input_directory, arg_obj.output_file_name)