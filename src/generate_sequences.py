import json
import argparse
from utilities import JsonLineWriter, JsonLineReader


class GenerateSequenceConfig(object):

    def __init__(self, json_file_name):
        with open(json_file_name, "r") as f:
            self.config = json.load(f)

    def get_time_window_size(self):
        return self.config["time_window"]

    def get_compress_keys(self):
        return list(self.config["compress"].keys())

    def get_categorical_keys(self):
        return list(self.config["categorical"].keys())

    def get_cumulative_keys(self):

        categorical_keys = self.get_categorical_keys()
        cumulative_key_list = []
        for key in categorical_keys:
            if "cumulative" == self.config["categorical"][key]:
                cumulative_key_list += [key]

        return cumulative_key_list

    def get_static_mappings(self):
        return self.config["static"]


def carry_forward(processed_sequence_list, cumulative_list):
    """Carry forward until a change is observed"""
    carry_sequence_list = []

    i = 0
    for item_dict in processed_sequence_list:

        copy_item_dict = item_dict.copy()
        if i > 0:
            past_copy_item_dict = carry_sequence_list[-1]

            for key in past_copy_item_dict:
                if key not in copy_item_dict:
                    copy_item_dict[key] = past_copy_item_dict[key]
                elif key in cumulative_list: # We are generating cumulative counts
                    copy_item_dict[key] += past_copy_item_dict[key]
                else:
                    pass # Item exists but not cumulative

        carry_sequence_list += [copy_item_dict]
        i += 1

    return carry_sequence_list


def sequence_generator(sequence_list, config_obj, sequence_start_time):

    state = "Start" # scan | start | process | end

    sequence_i = 0
    compress_keys = config_obj.get_compress_keys()
    processed_sequence = []

    categorical_keys = config_obj.get_categorical_keys()
    cumulative_keys_raw = config_obj.get_cumulative_keys()
    cumulative_keys = []

    sequence_dict = {}
    new_sequence_dict = {}
    end_window = None
    past_time = None
    start_time = None

    i = 0

    for item in sequence_list:

        current_time = item["unix_time"]
        current_key = item["key"]
        current_class = item["class"]

        if current_class in categorical_keys or current_key in categorical_keys:
            current_value = 1
            if current_class in cumulative_keys_raw:
                if current_key not in cumulative_keys:
                    cumulative_keys += [current_key]
                else:
                    if current_key in cumulative_keys_raw:
                        if current_key not in cumulative_keys:
                            cumulative_keys += [current_key]
        else:
            current_value = item["value"]

        if state == "Start":  # Starting a new sequence
            start_time = current_time
            end_window = start_time + config_obj.get_time_window_size()
            state = "Process"

        if current_time > end_window:
            if current_key in compress_keys or current_class in compress_keys:
                new_sequence_dict = {current_key: [current_value]}
            else:
                new_sequence_dict = {current_key: current_value}

            state = "End"

        if state == "Process":

            if current_key in compress_keys or current_class in compress_keys:

                if current_key in sequence_dict:
                    sequence_dict[current_key] += [current_value]
                else:
                    sequence_dict[current_key] = [current_value]

            else:

                if current_key not in sequence_dict:
                    sequence_dict[current_key] = current_value

                else: # Key exists so we are going to generate another sequence
                    new_sequence_dict = {current_key: current_value}
                    # start_time = current_time
                    state = "End"

        if state == "End":

            sequence_dict["meta"] = {}
            sequence_dict["meta"]["start_time"] = start_time
            sequence_dict["meta"]["sequence_time_delta"] = start_time - sequence_start_time
            sequence_dict["meta"]["end_time"] = past_time
            sequence_dict["meta"]["sequence_i"] = sequence_i
            sequence_dict["meta"]["i"] = i

            processed_sequence += [sequence_dict.copy()]
            sequence_i += 1

            sequence_dict = new_sequence_dict.copy()
            new_sequence_dict = {}
            state = "Process"

            start_time = current_time
            end_window = start_time + config_obj.get_time_window_size()

        past_time = current_time
        i += 1

    if state == "Process": # We hit the end
        sequence_dict["meta"] = {}
        sequence_dict["meta"]["start_time"] = start_time
        sequence_dict["meta"]["end_time"] = current_time
        sequence_dict["meta"]["sequence_i"] = sequence_i
        sequence_dict["meta"]["i"] = i

        processed_sequence += [sequence_dict.copy()]
        state = "End"

    for sequence_dict in processed_sequence:
        sequence_dict["meta"]["time_span"] = sequence_dict["meta"]["end_time"] - sequence_dict["meta"]["start_time"]

    processed_sequence_dict = {"changes": processed_sequence, "carry_forward": carry_forward(processed_sequence, cumulative_keys)}

    return processed_sequence_dict


def get_dict_value(data_dict, key_list):

    key_data_dict = data_dict
    last_key = key_list[-1]

    try:
        for key in key_list:
            if key_data_dict.__class__ == {}.__class__:
                if "field_values" in key_data_dict:
                    key_data_dict = key_data_dict["field_values"]
                else:
                    key_data_dict = key_data_dict[key]

                if last_key == key:
                    return key_data_dict[key]

            else:
                return [it["field_values"][key] for it in key_data_dict]
    except KeyError:
        return None


def static_generator(data_dict, config_obj, separator="_"):

    static_dict = {}
    static_dict["id"] = data_dict["primary"]["id"]

    static_mappings = config_obj.get_static_mappings()
    key_mappings = [k for k in static_mappings]

    for key in key_mappings:
        static_dict[key] = {}

    for key in key_mappings:
        keys_lists = static_mappings[key]
        for key_list in keys_lists:
            joined_key_str = separator.join(key_list)
            static_dict[key][joined_key_str] = get_dict_value(data_dict, key_list)
    # print(static_dict)
    return static_dict


def sequence_record_generator(record_dict, config_obj):

    dict_record = {}
    static_record_dict = static_generator(record_dict, config_obj)
    dict_record.update(static_record_dict)
    dynamic_record_dict = sequence_generator(record_dict["dynamic"], config_obj, record_dict["primary"]["unix_time"])
    dict_record.update(dynamic_record_dict)

    return dict_record


def main(config, input_json_txt_file, output_file_name):

    config_obj = GenerateSequenceConfig(config)

    line_writer = JsonLineWriter(output_file_name)

    # Static
    for sequence_dict in JsonLineReader(input_json_txt_file):
        line_writer.write(sequence_record_generator(sequence_dict, config_obj))

    line_writer.close()


if __name__ == "__main__":

    arg_obj = argparse.ArgumentParser(description="Builds JSON txt file for sequence model training")
    arg_obj.add_argument("-j", "--json-config-file", dest="json_config_file", required=True)
    arg_obj.add_argument("-i", "--input-file-name", dest="input_file_name", required=True)
    arg_obj.add_argument("-o", "--output-file-name", dest="output_file_name", required=True)
    arg_parse_obj = arg_obj.parse_args()

    main(arg_parse_obj.json_config_file, arg_parse_obj.input_file_name, arg_parse_obj.output_file_name)

