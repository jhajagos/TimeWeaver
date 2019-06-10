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

    def _config_primary_id(self):
        self.id_field_name = self.class_config["id_field_name"]

    def _config_fields(self):
        self.field_names = self.class_config["field_names"]
        self.field_mappings = self.class_config["field_mappings"]

    def _process_id(self, row_dict):
        return row_dict[self.id_field_name]

    def _process_fields(self, item_dict):
        field_value_dict = {}
        for field_name in self.field_names:

            field_value = item_dict[field_name]

            if field_name in self.field_mappings:
                field_map = self.field_mappings[field_name]
                if field_map == "int":
                    field_value = int(field_value)

            field_value_dict[field_name] = field_value

        return field_value_dict


class StaticBlockPrimaryProcess(Block):
    """The primary object must have a datetime field"""

    def process(self):
        self._config_date_time()
        self._config_primary_id()
        self._config_fields()

        list_to_process = []
        for item in self.block:
            item_dict = {}
            row_result = self._process_date_time(item)
            item_dict.update(row_result)
            item_dict["id"] = self._process_id(item)
            item_dict["field_values"] = self._process_fields(item)
            list_to_process += [item_dict]

        return list_to_process


class StaticBlockAdditionalProcess(Block):

    def process(self):
        self._config_primary_id()
        self._config_fields()

        list_to_process = []
        for item in self.block:
            item_dict = {}
            item_dict["id"] = self._process_id(item)
            item_dict["field_values"] = self._process_fields(item)
            list_to_process += [item_dict]

        return list_to_process


class DynamicBlockProcess(Block):

    def _config_additional_fields(self):
        self.field_names = self.class_config["additional_field_names"]

    def _config_join_id(self):
        self.id_field_name = self.class_config["joining_id_field_name"]

    def process(self):
        self._config_date_time()


def main(json_file_assembly_mapping, input_directory, output_file_name):
    config_obj = AssembleMappingConfig(json_file_assembly_mapping)



if __name__ == "__main__":

    arg_parse_obj = argparse.ArgumentParser(description="Generate a text JSON file which intermediate")

    arg_parse_obj.add_argument("-j", "--json-file-assembly-mapping", dest="json_file_assembly_mapping", required=True)
    arg_parse_obj.add_argument("-i", "--input-directory", dest="input_directory", default="./")
    arg_parse_obj.add_argument("-o", "--output-file-name", dest="output_file_name", default="./assembly_mapping.json.txt")

    arg_obj = arg_parse_obj.parse_args()

    main(arg_obj.json_assembly_mapping, arg_obj.input_directory, arg_obj.output_file_name)