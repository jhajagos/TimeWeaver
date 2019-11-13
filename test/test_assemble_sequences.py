import unittest
import pprint
import json

from assemble_sequences import main, CSVBlockFile, StaticBlockPrimaryProcess, StaticBlockAdditionalProcess,\
    DynamicBlockProcess, AssembleMappingConfig, Assembler


class TestAssembler(unittest.TestCase):

    def setUp(self):
        self.assemble_mapping = AssembleMappingConfig("./mappings/config_assemble_mapping.json")
        self.directory = "./data/"
        self.output_file_name = "./output/test_output.json.txt"

    def test_assembler(self):

        assembler_obj = Assembler(self.assemble_mapping, self.directory, self.output_file_name)
        assembler_obj.process()


class TestStaticBlock(unittest.TestCase):

    def setUp(self):
        self.additional_block_file_obj = CSVBlockFile("./data/encounter_details.csv", "encounter_id")
        self.additional_block_multi_file_obj = CSVBlockFile("./data/encounter_dx.csv", "encounter_id")
        self.primary_block_file_obj = CSVBlockFile("./data/encounters.csv", "encounter_id")

        self.assemble_mapping = AssembleMappingConfig("./mappings/config_assemble_mapping.json")

    def test_static_read(self):

        block_process_list_obj = []
        for block in self.additional_block_file_obj:
            block_process_list_obj += [StaticBlockAdditionalProcess(block,
                                                                    self.assemble_mapping.get_static_class("encounter_detail"))]

        process_result_1 = block_process_list_obj[0].process()

        self.assertIsNotNone(process_result_1)

    def test_static_multi_read(self):
        block_process_list_obj = []

        for block in self.additional_block_multi_file_obj:
            block_process_list_obj += [StaticBlockAdditionalProcess(block,
                                                                    self.assemble_mapping.get_static_class(
                                                                        "diagnosis"))]

        process_result_1 = block_process_list_obj[0].process()
        process_result_2 = block_process_list_obj[1].process()

        self.assertEqual(len(process_result_1), 2)
        self.assertEqual(len(process_result_2), 3)

    def test_primary(self):

        block_process_list_obj = []
        for block in self.primary_block_file_obj:
            block_process_list_obj += [StaticBlockPrimaryProcess(block,
                                                                 self.assemble_mapping.get_static_class("encounter"))]

        prim_result = block_process_list_obj[0].process()
        self.assertIsNotNone(prim_result)


class TestDynamicBlock(unittest.TestCase):

    def setUp(self):
        self.dynamic_block_file_obj_1 = CSVBlockFile("./data/measurements.csv", "encounter_id")
        self.dynamic_block_file_obj_2 = CSVBlockFile("./data/medications.csv", "encounter_id")
        self.assemble_mapping = AssembleMappingConfig("./mappings/config_assemble_mapping.json")

    def test_dynamic_process(self):
        dyn_block_process_list_obj_1 = []
        for dyn_block in self.dynamic_block_file_obj_1:
            dyn_block_process_list_obj_1 += [DynamicBlockProcess(dyn_block, self.assemble_mapping.get_dynamic_class("measurement"))]

        processed_1 = [b.process() for b in dyn_block_process_list_obj_1]

        self.assertIsNotNone(processed_1[0])

        dyn_block_process_list_obj_2 = []
        for dyn_block in self.dynamic_block_file_obj_2:
            dyn_block_process_list_obj_2 += [
                DynamicBlockProcess(dyn_block, self.assemble_mapping.get_dynamic_class("medication"))]

        processed_2 = [b.process() for b in dyn_block_process_list_obj_2]

        self.assertIsNotNone(processed_2[0])


class CSVBlockFileTestCase(unittest.TestCase):
    def test_read(self):

        block_obj_1 = CSVBlockFile("./data/measurements.csv", "encounter_id")

        list_block_obj_1 = list(block_obj_1)

        self.assertEqual(4, len(list_block_obj_1))

        self.assertEqual(6, len(list_block_obj_1[0]))

        self.assertEqual(2, len(list_block_obj_1[1]))

        self.assertEqual(1, len(list_block_obj_1[2]))

        block_obj_2 = CSVBlockFile("./data/encounter_dx.csv", "encounter_id")

        list_block_obj_2 = list(block_obj_2)

        self.assertEqual(2, len(list_block_obj_2[0]))

        self.assertEqual(3, len(list_block_obj_2[1]))


class AssembleTestCase(unittest.TestCase):
    def test_assemble(self):

        main("./mappings/config_assemble_mapping.json", "./data/", "./output/test_output_result.json.txt")

        with open("./output/test_output_result.json.txt") as f:

            result_1 = json.loads(f.__next__())
            result_2 = json.loads(f.__next__())
            result_3 = json.loads(f.__next__())

        self.assertIsNotNone(result_1)


if __name__ == '__main__':
    unittest.main()
