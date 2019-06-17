import json
import argparse
from utilities import JsonLineWriter, JsonLineReader
import pprint


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


def process_static_variables(sequence_dict, config_dict):
    return {}


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


def sequence_generator(sequence_list, config_obj):

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

    i = 0
    print("******************")

    pprint.pprint(sequence_list)

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
                    start_time = current_time
                    state = "End"

        if state == "End":

            sequence_dict["meta"] = {}
            sequence_dict["meta"]["start_time"] = start_time
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

    # pprint.pprint(processed_sequence_dict)
    # print("*********************************")

    return processed_sequence_dict


def static_generator(data_dict, config_obj):
    return {}


def main(config, input_json_txt_file, output_file_name):

    config_obj = GenerateSequenceConfig(config)

    line_writer = JsonLineWriter(output_file_name)

    # Static

    for sequence_dict in JsonLineReader(input_json_txt_file):
        line_writer.write(sequence_generator(sequence_dict["dynamic"], config_obj))

    line_writer.close()


if __name__ == "__main__":

    arg_obj = argparse.ArgumentParser(description="Builds JSON txt file for sequence model training")
    arg_obj.add_argument("-j", "--json-config-file", dest="json_config_file", required=True)
    arg_obj.add_argument("-i", "--input-file-name", dest="input_file_name", required=True)
    arg_obj.add_argument("-o", "--output-file-name", dest="output_file_name", required=True)
    arg_parse_obj = arg_obj.parse_args()

    main(arg_parse_obj.json_config_file, arg_parse_obj.input_file_name, arg_parse_obj.output_file_name)

"""

Sequence generation for specific encounter:
{
"time_window": 3600, # specified in seconds
"categorical": {"medication": {"methods": ["cumulative"]}},
"compress": {"vital": {"methods": ["mean", "min", "max"]}}
"static": [["primary","id"], ["primary", "gender"]] 
}

state transition

start
process
end

first event -> start

set start time

process

keep accumulating events into list

if we hit a non-compress event of the same class and label
then create a new sequence

if hit end

once list 

sequence_id,
start_time,
end_time,
label

pass
"""