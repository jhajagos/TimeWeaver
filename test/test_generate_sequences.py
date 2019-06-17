import unittest
import json
from generate_sequences import GenerateSequenceConfig, sequence_generator, static_generator, main


class TestGenerateSequences(unittest.TestCase):

    def setUp(self):
        with open("./data/testing_output.json.txt") as f:
            self.sequence_dict = json.loads(f.__next__())

        self.config_obj = GenerateSequenceConfig("./mappings/config_generate_sequences.json")

    def test_generate_sequences(self):

        result_1 = sequence_generator(self.sequence_dict["dynamic"], self.config_obj)
        self.assertIsNotNone(result_1)

    def test_generate_static(self):

        result_1 = static_generator(self.sequence_dict, self.config_obj)
        self.assertIsNotNone(result_1)


    def test_main(self):
        main("./mappings/config_generate_sequences.json", "./data/testing_output.json.txt", "./output/result.1.json.txt")
        self.assertTrue(1)



if __name__ == '__main__':
    unittest.main()
