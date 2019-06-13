import json


class JSONLineReader(object):

    def __init__(self, json_txt_file_name):
        self.json_txt_file_name = json_txt_file_name
        self.f = open(self.json_txt_file_name, "r")

    def __iter__(self):
        return self

    def __next__(self):
        return json.loads(self.f.__next__())

    def close(self):
        self.f.close()


def main(config, input_json_txt_file, output_file_name):
    pass

if __name__ == "__main__":
    pass