import unittest
import pprint

from assemble_sequences import main, CSVBlockFile


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
