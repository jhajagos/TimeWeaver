import unittest
import json
import pprint

from assemble_sequences import main, CSVBlockFile, StaticBlockProcess, DynamicBlockProcess, AssembleMappingConfig


class TestStaticBlock(unittest.TestCase):

    def setUp(self):
        self.block_file_obj = CSVBlockFile("./data/encounter_details.csv", "encounter_id")

        self.assemble_mapping = AssembleMappingConfig("./mappings/config_assemble_mapping.json")

    def test_static_read(self):

        block_process_obj = []
        for block in self.block_file_obj:
            block_process_obj = StaticBlockProcess(block, self.assemble_mapping.get_static_class("encounter_details"))






class CSVBlockFileTestCase(unittest.TestCase):
    def test_read(self):

        block_obj = CSVBlockFile("./data/measurements.csv", "encounter_id")

        list_block_obj = list(block_obj)

        self.assertEqual(3, len(list_block_obj))

        self.assertEqual(6, len(list_block_obj[0]))

        self.assertEqual(2, len(list_block_obj[1]))

        self.assertEqual(1, len(list_block_obj[2]))

class AssembleTestCase(unittest.TestCase):
    def test_assemble(self):

        main("./mappings/config_assemble_mapping.json", "./data/", "./output")

        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()
