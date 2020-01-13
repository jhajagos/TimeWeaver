import csv
import json
import argparse
import datetime
import os
from utilities import JsonLineWriter


class Assembler(object):

    """
     The main assumption is that all files are sorted by a single key or id.
    """

    def __init__(self, assemble_mapping_config, directory, output_file_name, exclude_empty_series=True):
        self.assemble_mapping_config = assemble_mapping_config
        self.directory = directory
        self.output_file_name = output_file_name

        self.static_primary_config = {}
        self.static_additional_config = {}
        self.dynamic_config = {}

        self.exclude_empty_series = exclude_empty_series

        self._initialize()

        self.writer = JsonLineWriter(output_file_name)

    def _initialize(self):

        self.static_class_names = self.assemble_mapping_config.get_static_classes()
        self.dynamic_class_names = self.assemble_mapping_config.get_dynamic_classes()

        # All files must exist

        # Open all CSV files

        dynamic_dict_file_names = {dc: self.assemble_mapping_config.get_dynamic_class(dc)["source"]["file_name"] for dc in
                              self.dynamic_class_names}

        static_dict_file_names = {sc: self.assemble_mapping_config.get_static_class(sc)["source"]["file_name"] for sc in
                              self.static_class_names}

        self.dynamic_dict_block = {dc: CSVBlockFile(os.path.join(self.directory, dynamic_dict_file_names[dc]),
                                               self.assemble_mapping_config.get_dynamic_class(dc)["joining_id_field_name"]) for dc in
                                                dynamic_dict_file_names}

        self.static_dict_block = {sc: CSVBlockFile(os.path.join(self.directory, static_dict_file_names[sc]),
                                                    self.assemble_mapping_config.get_static_class(sc)[
                                                        "id_field_name"]) for sc in
                                   static_dict_file_names}

        self.static_name_primary = [sc for sc in self.static_class_names
                                if self.assemble_mapping_config.get_static_class(sc)["type"] == "primary"][0]

        self.static_name_additional = [sc for sc in self.static_class_names
                                     if self.assemble_mapping_config.get_static_class(sc)["type"] == "additional"]

        self.primary_config = self.assemble_mapping_config.get_static_class(self.static_name_primary)

    def process(self):

        class_names = self.static_class_names + self.dynamic_class_names

        class_names_dict_id = {c: None for c in class_names}

        static_result = {}
        static_finished = []
        dynamic_result = {}
        dynamic_finished = []
        static_id = None
        dynamic_id = None

        # We start with the primary block
        for primary_block in self.static_dict_block[self.static_name_primary]:
            result_dict = {}
            primary_obj = StaticBlockPrimaryProcess(primary_block, self.primary_config)

            primary_result = primary_obj.process()[0]
            result_dict["primary"] = primary_result
            result_dict["static"] = {}
            result_dict["dynamic"] = []

            primary_id = primary_result["id"]

            class_names_dict_id[self.static_name_primary] = primary_id

            for static_name in self.static_name_additional:

                if static_name not in static_finished:  # Check if we have finished reading the file

                    if class_names_dict_id[static_name] is None:  # First time through
                        static_block = self.static_dict_block[static_name].__next__()
                        static_objs = StaticBlockAdditionalProcess(static_block,
                                                                   self.assemble_mapping_config.get_static_class(static_name))

                        static_obj_list = []
                        for static_obj in static_objs.process():
                            static_obj_list += [static_obj]

                        static_id = static_obj["id"]
                        class_names_dict_id[static_name] = static_id

                        if len(static_obj_list) > 1:
                            static_result[static_name] = static_obj_list
                        else:
                            static_result[static_name] = static_obj_list[0]

                    if class_names_dict_id[static_name] == primary_id:  # We have a match

                        result_dict["static"][static_name] = static_result[static_name]
                        try:
                            static_block = self.static_dict_block[static_name].__next__()
                            static_objs = StaticBlockAdditionalProcess(static_block,
                                                                       self.assemble_mapping_config.get_static_class(
                                                                           static_name))

                            static_obj_list = []
                            for static_obj in static_objs.process():
                                static_obj_list += [static_obj]
                            static_id = static_obj["id"]

                            if len(static_obj_list) > 1:
                                static_result[static_name] = static_obj_list
                            else:
                                static_result[static_name] = static_obj_list[0]

                            class_names_dict_id[static_name] = static_id

                        except StopIteration:
                            static_finished += [static_name]

                    else:
                        result_dict["static"][static_name] = {}
                else:
                    result_dict["static"][static_name] = {}

            for dynamic_name in self.dynamic_class_names:
                if dynamic_name not in dynamic_finished:
                    if class_names_dict_id[dynamic_name] is None:
                        dynamic_block = self.dynamic_dict_block[dynamic_name].__next__()

                        dynamic_objs = DynamicBlockProcess(dynamic_block, self.assemble_mapping_config.get_dynamic_class(dynamic_name))

                        dynamic_list_result = []
                        dynamic_objs_to_process = dynamic_objs.process()

                        for dynamic_obj in dynamic_objs_to_process:
                            if dynamic_obj["match"]:
                                del dynamic_obj["match"]
                                dynamic_list_result += [dynamic_obj]
                        dynamic_id = dynamic_obj["id"]

                        class_names_dict_id[dynamic_name] = dynamic_id
                        dynamic_result[dynamic_name] = dynamic_list_result

                if class_names_dict_id[dynamic_name] == primary_id:
                    result_dict["dynamic"] += dynamic_result[dynamic_name]
                    try:
                        dynamic_block = self.dynamic_dict_block[dynamic_name].__next__()
                        dynamic_objs = DynamicBlockProcess(dynamic_block,
                                                           self.assemble_mapping_config.get_dynamic_class(dynamic_name))

                        dynamic_list_result = []
                        dynamic_objs_to_process = dynamic_objs.process()
                        for dynamic_obj in dynamic_objs_to_process:
                            if dynamic_obj["match"]:
                                del dynamic_obj["match"]
                                dynamic_list_result += [dynamic_obj]
                            #dynamic_list_result += [dynamic_obj]

                        dynamic_id = dynamic_obj["id"]
                        class_names_dict_id[dynamic_name] = dynamic_id

                        class_names_dict_id[dynamic_name] = dynamic_id
                        dynamic_result[dynamic_name] = dynamic_list_result

                    except StopIteration:
                        dynamic_finished += [dynamic_name]

            if len(result_dict["dynamic"]):  # Sort every thing into time order
                result_dict["dynamic"].sort(key=lambda x: x["unix_time"])

            if len(result_dict["dynamic"]) > 0:
                self.writer.write(result_dict)
            elif not self.exclude_empty_series:
                self.writer.write(result_dict)

        self.writer.close()
        # For other blocks:

        # If id is not present  -- left join do not advance

        # If id is present - advance and process block

        # Read and process the static_additional

        # Read and process dynamic block

        # Sort the dynamic blocks

        # Write to single line JSON txt file

        pass


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
            i += 1

    def get_static_class(self, class_name):
        return self.config["static"][self._static_positions[class_name]]

    def get_dynamic_class(self, class_name):
        return self.config["dynamic"][self._dynamic_positions[class_name]]

    def get_static_classes(self):
        static_classes = [sc for sc in self.config["static"]]
        return [sc["class"] for sc in static_classes]

    def get_dynamic_classes(self):
        dynamic_classes = [sc for sc in self.config["dynamic"]]
        return [dc["class"] for dc in dynamic_classes]


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
                self.rows_buffer = [row_dict]
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

    def _config_class(self):
        self.block_class = self.class_config["class"]

    def _config_date_time(self):
        date_time_config = self.class_config["date_time"]
        self.date_time_field_name = date_time_config["field_name"]
        self.date_time_format = date_time_config["format"]

    def _process_date_time(self, row_dict):

        date_time_string_value = row_dict[self.date_time_field_name]
        date_time_value = datetime.datetime.strptime(date_time_string_value, self.date_time_format)

        datetime_dict = {
            "unix_time": date_time_value.timestamp(),
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
                elif field_map == "float":
                    field_value = float(field_value)

            field_value_dict[field_name] = field_value

        return field_value_dict


class StaticBlockPrimaryProcess(Block):
    """The primary object must have a datetime field"""

    def process(self):
        self._config_date_time()
        self._config_primary_id()
        self._config_fields()
        self._config_class()

        list_to_return = []
        for item in self.block:
            item_dict = {}
            row_result = self._process_date_time(item)
            item_dict.update(row_result)
            item_dict["id"] = self._process_id(item)
            item_dict["field_values"] = self._process_fields(item)
            list_to_return += [item_dict]

        return list_to_return


class StaticBlockAdditionalProcess(Block):

    def process(self):
        self._config_primary_id()
        self._config_fields()
        self._config_class()

        list_to_return = []
        for item in self.block:
            item_dict = {}
            item_dict["id"] = self._process_id(item)
            item_dict["field_values"] = self._process_fields(item)
            list_to_return += [item_dict]

        return list_to_return


class DynamicBlockProcess(Block):

    def _config_additional_fields(self):
        self.field_names = self.class_config["additional_field_names"]
        self.field_mappings = self.class_config["additional_field_name_mappings"]

    def _config_join_id(self):
        self.id_field_name = self.class_config["joining_id_field_name"]

    def _config_filters(self):
        self.filter = self.class_config["filter"]
        if self.filter is not None or len(self.filter):
            self.filter_criteria = self.filter["criteria"]
            self.filter_field_names = self.filter["field_names"]
            self.filter_values = self.filter["values"]
        else:
            self.filter_criteria = None

    def _config_label(self):
        config_label = self.class_config["label"]

        self.label_field_names = config_label["field_names"]
        self.label_join_character = config_label["join_character"]

    def _config_value(self):

        config_value = self.class_config["value"]
        self.value_field_name = config_value["field_name"]
        self.value_type = config_value["type"]

    def _process_value(self, item_dict):

        value_process = item_dict[self.value_field_name]

        if self.value_type == "float":
            try:
                value_process = float(value_process)
            except ValueError:
                value_process = None
        if self.value_type == "int":
            try:
                value_process = int(value_process)
            except ValueError:
                value_process = None

        return {"value": value_process, "value_type": self.value_type}

    def _check_if_matches(self, item_dict):

        if self.filter_criteria is None:  # if no filter return
            return True
        else:
            item_values = []
            for field_name in self.filter_field_names:
                item_values += [item_dict[field_name]]

            if self.filter_criteria == "or":
                for item_value in item_values:
                    if item_value in self.filter_values:
                        return True
                return False

            elif self.filter_criteria == "not_equal":
                for item_value in item_values:
                    if item_value in self.filter_values:
                        return False
                return True

            else:
                return False  # Other criteria

    def _process_label(self, row_dict):
        label_list = []

        for label_field_name in self.label_field_names:
            label_list += [row_dict[label_field_name]]
            label_string = self.label_join_character.join(label_list)

        return {"label_list": label_list, "label_string": label_string}

    def _process_key(self, label_string, separator="||"):
        return {"key": self.block_class + separator + label_string}

    def process(self):
        self._config_date_time()
        self._config_join_id()
        self._config_additional_fields()
        self._config_filters()
        self._config_label()
        self._config_class()
        self._config_value()

        list_to_return = []

        for item in self.block:
            row_dict = self._process_date_time(item)
            row_dict["match"] = self._check_if_matches(item)
            row_dict["id"] = self._process_id(item)
            row_dict.update(self._process_value(item))
            row_dict["class"] = self.block_class
            row_dict["additional_data"] = self._process_fields(item)
            row_dict.update(self._process_label(item))
            row_dict.update(self._process_key(row_dict["label_string"]))
            list_to_return += [row_dict]

        list_to_return.sort(key=lambda x: x["unix_time"])
        return list_to_return


def main(json_file_assembly_mapping, input_directory, output_file_name):
    config_obj = AssembleMappingConfig(json_file_assembly_mapping)
    assembler_obj = Assembler(config_obj, input_directory, output_file_name)
    assembler_obj.process()


if __name__ == "__main__":

    arg_parse_obj = argparse.ArgumentParser(description="Generate a text JSON file which intermediate")

    arg_parse_obj.add_argument("-j", "--json-file-assembly-mapping", dest="json_file_assembly_mapping", required=True)
    arg_parse_obj.add_argument("-i", "--input-directory", dest="input_directory", default="./")
    arg_parse_obj.add_argument("-o", "--output-file-name", dest="output_file_name", default="./assembly_mapping.json.txt")

    arg_obj = arg_parse_obj.parse_args()

    main(arg_obj.json_file_assembly_mapping, arg_obj.input_directory, arg_obj.output_file_name)