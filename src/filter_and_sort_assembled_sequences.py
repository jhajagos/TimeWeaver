import argparse
import json


def main(input_file_name, output_file_name, query_json_file_name):

    with open(query_json_file_name) as f:
        query_dict = json.load(f)

    # first pass

    with open(input_file_name, "r") as f:

        i = 0
        for line in f:
            sequence_struct = json.load(f)

            i += 1

        print(f"First pass through file: read {i} items")

    # sorting

    # write out file

    with open(input_file_name, "r") as f:
        with open(output_file_name, "w") as fw:
            for line in f:
                pass


if __name__ == "__main__":

    arg_obj = argparse.ArgumentParser(description="Filters and sorts a JSON file name")
    arg_obj.add_argument("-i", "--input-file-name", dest="input_file_name", required=True)
    arg_obj.add_argument("-o", "--output-file-name", dest="output_file_name", default=None)
    arg_obj.add_argument("-q", "--query-json-file-name", dest="query_json_file_name", required=True)
    arg_parse_obj = arg_obj.parse_args()

    if arg_parse_obj.output_file_name is None:
        output_file_name = arg_parse_obj.input_file_name + ".filtered"
    else:
        output_file_name = arg_parse_obj.output_file_name


    main(arg_parse_obj.input_file_name, output_file_name, arg_parse_obj.query_json_file_name)