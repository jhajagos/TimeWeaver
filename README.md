# TimeWeaver

Is an assembler of multi-sourced sequential data for analytic and machine learning 
use cases. It is designed to read from data stored in CSV files. The tool can 
combine multiple CSV files into a single JSON record. From the JSON file data
can be assembled into separate data sequences which can be more directly utilized by 
RNN (Recurrent Neural Networks) sequence type models.

The use case TimeWeaver is being tested is on sequence data from health records 
where there are multiple sequential sequences, such as, medications and 
measurements.

## Pipeline steps

### Sequence assembly step

This step generates a `json.txt` which includes static and temporal data. A 
`json.txt` file is text file where each line is JSON object. This structure helps 
with memory usage as file can interated through.

```bash
python3 assemble_sequences.py -j ..\ohdsi\config\config_assemble_mapping.json -i Y:\ds\ts\ -o Y:\ds\ts\tseries.json.txt
```

The program is primarily driven by a mapping file. The mapping file has two parts.
```json
{
    "static": {},
    "dynamic": {}
}
```

#### Static ####
```json

```
#### Dynamic ####

The dynamic section is used to configure which fields get mapped into the sequence.

The class name must be unique:
```json
{"class": "drug_exposure"}
```

The `"date_time"` key specifies the primary trigger time event and the format of the
date time field format. See https://docs.python.org/3.6/library/datetime.html#strftime-and-strptime-behavior
for the formatting.
```json
{"date_time": {"field_name": "drug_exposure_start_datetime",
               "format": "%Y-%m-%d %H:%M:%S"}}
```

The `"source"` key specifies the file that it will read from. Currently, TimeWeaver
only supports CSV files.
```json
{"source": {"type": "csv",
            "file_name": "map2_measurement.csv"}}
```

The `"joining_id_field_name"` key specifies the field name that links to the 
main identifier.
```json
{"joining_id_field_name": "visit_occurrence_id"}
```

The `""value"` key is used to determine which values are used as
values in the format.

For numeric data, such as measurements:
```json
{"value": {"field_name": "value_as_number",
           "type": "float"}}
```
The `"type"` key can also have the values `"int"` for integers, `"float"` for
floating point numbers for continuous measured values, and `"categorial"`

For categorical data, such as drugs, where we just concerned when the drug
was first given:
```json
{"value": {"field_name": "drug_concept_id",
           "type": "categorical"}}
```

The key `"additional_field_names"` allows supporting fields with values to be 
included in the generated `JSON.txt` file.
```json
{"additional_field_names": ["stop_reason", "route_source_value", "quantity","dose_unit_source_value"]}
```
The field names must be in a list.

The key `"additional_field_name_mappings"` key allows a field specified 
in `""additional_field_names"` to have a different name in the generated 
`JSON.txt` file.
```json
{"additional_field_name_mappings": {"stop_reason":  "stop_reason_text"}}
```

The key `"label"` is used to generate human readable labels which make
interpreting and trouble shooting easier.
```json
{"label": {"field_names": ["drug_concept_id", "drug_concept_name"],
    "join_character": "|"}}
```
Using the above mappings, as an example `drug_concept_id = 6387` and `drug_concept_name = Ibuprofen`
would generate the following human readable label `"6387|Ibuprofen"`.

The key `"filter` is used to include or exclude certain values in the sequence from being 
included. To exclude certain values:
```json
{"filter": {
            "field_names": ["drug_concept_id"],
             "values": ["0"],
             "criteria": "not_equal"}}
```
To include certain values:
```json
{"filter": {
            "field_names": ["drug_concept_id"],
             "values": ["5640","6387"],
             "criteria": "or"}}
```


### Sequence generation step

```bash
python3 generate_sequences.py -j ..\ohdsi\config\config_sequences.json -i Y:\ds\ts\tseries.json.txt -o Y:\ds\ts\result.tseries.json.txt
```

### Sequence packaging

#### Scan

```bash
python .\package_sequences.py -f Y:\healthfacts\ts\result.tseries.json.txt -c scan -b ohdsi -d Y:\ds\ts\output\
```

#### Export to CSV

```bash
python .\package_sequences.py -f Y:\healthfacts\ts\result.tseries.json.txt -c csv -b ohdsi -d Y:\ds\ts\output\
```

#### Package as HDF5 file

```bash
python .\package_sequences.py -f Y:\healthfacts\ts\result.tseries.json.txt -c csv -b ohdsi -d Y:\ds\ts\output\
```

