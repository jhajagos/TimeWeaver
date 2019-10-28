import unittest
from package_sequences import scan_file, ClassScan, generate_csv_files, generate_hdf5_file

class PackageSequenceTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def test_scan_sequence(self):

        scan_file("./data/result.1.json.txt", "./output/")

        scan_obj = ClassScan("./output/")

        keys = scan_obj.get_subjects()

        self.assertIn("dependent", keys)

    def test_generate_csv_files(self):

        scan_file("./data/result.1.json.txt", "./output/")
        generate_csv_files("./data/result.1.json.txt", "./output/", "test")

        self.assertTrue(1)

    def test_generate_hdf5_files(self):

        scan_file("./data/result.1.json.txt", "./output/")
        generate_csv_files("./data/result.1.json.txt", "./output/", "test")
        generate_hdf5_file("./data/result.1.json.txt", "./output/", "test")


    def test_generate_buffered_hdf5_files(self):

        scan_file("./data/result.1.json.txt", "./output/")
        generate_csv_files("./data/result.1.json.txt", "./output/", "test")
        generate_hdf5_file("./data/result.1.json.txt", "./output/", "test", buffer_size=2)

if __name__ == '__main__':
    unittest.main()
