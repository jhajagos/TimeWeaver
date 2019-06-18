import unittest
from package_sequences import scan_file

class PackageSequenceTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def test_scan_sequence(self):

        scan_file("./data/result.1.json.txt", "./output/", "test_base_name")

        self.assertTrue(True)


if __name__ == '__main__':
    unittest.main()
