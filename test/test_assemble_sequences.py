import unittest

from assemble_sequences import main

class AssembleTestCase(unittest.TestCase):
    def test_assemble(self):

        main("./mappings/config_assemble_mapping.json", "./data/", "./output")

        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()
