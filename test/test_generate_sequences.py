import unittest
import json
from generate_sequences import GenerateSequenceConfig, sequence_generator

class TestGenerateSequences(unittest.TestCase):

    def setUp(self):
        pass

    def test_generate_sequences(self):
        json_txt = '{"primary": {"unix_time": 1547995800.0, "original_date_time_value": "2019-01-20 09:50", "id": "1", "field_values": {"discharge_date_time": "2019-01-25 14:50", "person_id": 1, "gender": "Male", "age_at_encounter_start": 67}}, "static": {"encounter_detail": {"id": "1", "field_values": {"encounter_type_code": "I", "encounter_type_name": "Inpatient"}}}, "dynamic": [{"unix_time": 1547998573.0, "original_date_time_value": "2019-01-20 10:36:13", "id": "1", "value": 76, "value_type": "int", "class": "vital", "additional_data": {}, "label_list": ["56565-3", "pulse"], "label_string": "56565-3|pulse", "key": "vital||56565-3|pulse"}, {"unix_time": 1548003602.0, "original_date_time_value": "2019-01-20 12:00:02.0", "id": "1", "value": 250.0, "value_type": "float", "class": "measurement", "additional_data": {"measurement_value_concept": "high"}, "label_list": ["5555-6", "blood glucose"], "label_string": "5555-6|blood glucose", "key": "measurement||5555-6|blood glucose"}, {"unix_time": 1548003906.0, "original_date_time_value": "2019-01-20 12:05:06.0", "id": "1", "value": 0.7, "value_type": "float", "class": "measurement", "additional_data": {"measurement_value_concept": "normal"}, "label_list": ["1111-6", "creatine"], "label_string": "1111-6|creatine", "key": "measurement||1111-6|creatine"}, {"unix_time": 1548007510.2, "original_date_time_value": "2019-01-20 13:05:10.2", "id": "1", "value": 243.0, "value_type": "float", "class": "measurement", "additional_data": {"measurement_value_concept": "high"}, "label_list": ["5555-6", "blood glucose"], "label_string": "5555-6|blood glucose", "key": "measurement||5555-6|blood glucose"}, {"unix_time": 1548011943.0, "original_date_time_value": "2019-01-20 14:19:03", "id": "1", "value": 69, "value_type": "int", "class": "vital", "additional_data": {}, "label_list": ["56565-3", "pulse"], "label_string": "56565-3|pulse", "key": "vital||56565-3|pulse"}, {"unix_time": 1548014770.6, "original_date_time_value": "2019-01-20 15:06:10.6", "id": "1", "value": 90.0, "value_type": "float", "class": "measurement", "additional_data": {"measurement_value_concept": "normal"}, "label_list": ["5555-6", "blood glucose"], "label_string": "5555-6|blood glucose", "key": "measurement||5555-6|blood glucose"}, {"unix_time": 1548028693.0, "original_date_time_value": "2019-01-20 18:58:13.0", "id": "1", "value": "5555", "value_type": "categorical", "class": "medication", "additional_data": {}, "label_list": ["5555", "insulin"], "label_string": "5555|insulin", "key": "medication||5555|insulin"}, {"unix_time": 1548032136.6, "original_date_time_value": "2019-01-20 19:55:36.6", "id": "1", "value": 60.0, "value_type": "float", "class": "measurement", "additional_data": {"measurement_value_concept": "low"}, "label_list": ["5555-6", "blood glucose"], "label_string": "5555-6|blood glucose", "key": "measurement||5555-6|blood glucose"}, {"unix_time": 1548046225.0, "original_date_time_value": "2019-01-20 23:50:25.0", "id": "1", "value": "5555", "value_type": "categorical", "class": "medication", "additional_data": {}, "label_list": ["5555", "insulin"], "label_string": "5555|insulin", "key": "medication||5555|insulin"}, {"unix_time": 1548057245.0, "original_date_time_value": "2019-01-21 02:54:05", "id": "1", "value": 81, "value_type": "int", "class": "vital", "additional_data": {}, "label_list": ["56565-3", "pulse"], "label_string": "56565-3|pulse", "key": "vital||56565-3|pulse"}, {"unix_time": 1548123789.0, "original_date_time_value": "2019-01-21 21:23:09", "id": "1", "value": 72, "value_type": "int", "class": "vital", "additional_data": {}, "label_list": ["56565-3", "pulse"], "label_string": "56565-3|pulse", "key": "vital||56565-3|pulse"}, {"unix_time": 1548130881.0, "original_date_time_value": "2019-01-21 23:21:21", "id": "1", "value": 72, "value_type": "int", "class": "vital", "additional_data": {}, "label_list": ["56565-3", "pulse"], "label_string": "56565-3|pulse", "key": "vital||56565-3|pulse"}, {"unix_time": 1548134265.0, "original_date_time_value": "2019-01-22 00:17:45", "id": "1", "value": 77, "value_type": "int", "class": "vital", "additional_data": {}, "label_list": ["56565-3", "pulse"], "label_string": "56565-3|pulse", "key": "vital||56565-3|pulse"}, {"unix_time": 1548141450.0, "original_date_time_value": "2019-01-22 02:17:30", "id": "1", "value": 80, "value_type": "int", "class": "vital", "additional_data": {}, "label_list": ["56565-3", "pulse"], "label_string": "56565-3|pulse", "key": "vital||56565-3|pulse"}, {"unix_time": 1548159414.0, "original_date_time_value": "2019-01-22 07:16:54", "id": "1", "value": 77, "value_type": "int", "class": "vital", "additional_data": {}, "label_list": ["56565-3", "pulse"], "label_string": "56565-3|pulse", "key": "vital||56565-3|pulse"}, {"unix_time": 1548173439.0, "original_date_time_value": "2019-01-22 11:10:39", "id": "1", "value": 80, "value_type": "int", "class": "vital", "additional_data": {}, "label_list": ["56565-3", "pulse"], "label_string": "56565-3|pulse", "key": "vital||56565-3|pulse"}, {"unix_time": 1548177003.0, "original_date_time_value": "2019-01-22 12:10:03", "id": "1", "value": 88, "value_type": "int", "class": "vital", "additional_data": {}, "label_list": ["56565-3", "pulse"], "label_string": "56565-3|pulse", "key": "vital||56565-3|pulse"}, {"unix_time": 1548202421.0, "original_date_time_value": "2019-01-22 19:13:41", "id": "1", "value": 83, "value_type": "int", "class": "vital", "additional_data": {}, "label_list": ["56565-3", "pulse"], "label_string": "56565-3|pulse", "key": "vital||56565-3|pulse"}, {"unix_time": 1548205084.0, "original_date_time_value": "2019-01-22 19:58:04", "id": "1", "value": 86, "value_type": "int", "class": "vital", "additional_data": {}, "label_list": ["56565-3", "pulse"], "label_string": "56565-3|pulse", "key": "vital||56565-3|pulse"}, {"unix_time": 1548205690.0, "original_date_time_value": "2019-01-22 20:08:10", "id": "1", "value": 75, "value_type": "int", "class": "vital", "additional_data": {}, "label_list": ["56565-3", "pulse"], "label_string": "56565-3|pulse", "key": "vital||56565-3|pulse"}, {"unix_time": 1548335380.0, "original_date_time_value": "2019-01-24 08:09:40", "id": "1", "value": 74, "value_type": "int", "class": "vital", "additional_data": {}, "label_list": ["56565-3", "pulse"], "label_string": "56565-3|pulse", "key": "vital||56565-3|pulse"}, {"unix_time": 1548352975.0, "original_date_time_value": "2019-01-24 13:02:55", "id": "1", "value": 71, "value_type": "int", "class": "vital", "additional_data": {}, "label_list": ["56565-3", "pulse"], "label_string": "56565-3|pulse", "key": "vital||56565-3|pulse"}, {"unix_time": 1548357990.0, "original_date_time_value": "2019-01-24 14:26:30", "id": "1", "value": 76, "value_type": "int", "class": "vital", "additional_data": {}, "label_list": ["56565-3", "pulse"], "label_string": "56565-3|pulse", "key": "vital||56565-3|pulse"}, {"unix_time": 1548360096.0, "original_date_time_value": "2019-01-24 15:01:36", "id": "1", "value": 78, "value_type": "int", "class": "vital", "additional_data": {}, "label_list": ["56565-3", "pulse"], "label_string": "56565-3|pulse", "key": "vital||56565-3|pulse"}, {"unix_time": 1548415871.0, "original_date_time_value": "2019-01-25 06:31:11", "id": "1", "value": 91, "value_type": "int", "class": "vital", "additional_data": {}, "label_list": ["56565-3", "pulse"], "label_string": "56565-3|pulse", "key": "vital||56565-3|pulse"}, {"unix_time": 1548431230.0, "original_date_time_value": "2019-01-25 10:47:10", "id": "1", "value": 88, "value_type": "int", "class": "vital", "additional_data": {}, "label_list": ["56565-3", "pulse"], "label_string": "56565-3|pulse", "key": "vital||56565-3|pulse"}, {"unix_time": 1548431920.0, "original_date_time_value": "2019-01-25 10:58:40", "id": "1", "value": 74, "value_type": "int", "class": "vital", "additional_data": {}, "label_list": ["56565-3", "pulse"], "label_string": "56565-3|pulse", "key": "vital||56565-3|pulse"}]}'

        sequence_dict = json.loads(json_txt)

        config_obj = GenerateSequenceConfig("./mappings/config_generate_sequences.json")

        result_1 = sequence_generator(sequence_dict["dynamic"], config_obj)

        self.assertIsNotNone(result_1)


if __name__ == '__main__':
    unittest.main()
