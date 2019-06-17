import unittest
import json
from generate_sequences import GenerateSequenceConfig, sequence_generator

class TestGenerateSequences(unittest.TestCase):

    def setUp(self):
        pass

    def test_generate_sequences(self):

        with open("./data/testing_output.json.txt") as f:
            sequence_dict = json.loads(f.__next__())


        config_obj = GenerateSequenceConfig("./mappings/config_generate_sequences.json")

        result_1 = sequence_generator(sequence_dict["dynamic"], config_obj)

        self.assertIsNotNone(result_1)


if __name__ == '__main__':
    unittest.main()
