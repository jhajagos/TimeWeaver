import json


class JsonLineWriter(object):
    def __init__(self, file_name):
        self.file_name = file_name
        self.fw = open(self.file_name, "w")

    def close(self):
        self.fw.close()

    def write(self, obj_to_serialize):
        self.fw.write(json.dumps(obj_to_serialize) + "\n")  # Assumption here is that your strings do not contain any newlines


class JsonLineReader(object):

    def __init__(self, json_txt_file_name):
        self.json_txt_file_name = json_txt_file_name
        self.f = open(self.json_txt_file_name, "r")

    def __iter__(self):
        return self

    def __next__(self):
        return json.loads(self.f.__next__())

    def close(self):
        self.f.close()