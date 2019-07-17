# TimeWeaver

Is a generator of multi-sourced sequential data for analytic and machine learning use cases. It is designed to read from
data stored in CSV files. The tool can combine multiple data sources into a single JSON record and export this
data into separate data sequences which can be more directly utilized by RNN (Recurrent Neural Networks)
sequence type models.

The use case this is being tested is on sequence data from health records where there are multiple sources.

## Pipeline steps

### Sequence assembly step

This step generates a `json.txt` which includes static and temporal data.

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

### Sequence generation step

```bash
python3 generate_sequences.py -j ..\ohdsi\config\config_sequences.json -i Y:\ds\ts\tseries.json.txt -o Y:\ds\ts\result.tseries.json.txt
```

### Sequence packaging

#### Scan

```bash
python.exe .\package_sequences.py -f Y:\healthfacts\ts\result.tseries.json.txt -c scan -b ohdsi -d Y:\ds\ts\output\
```

#### Export to CSV

```bash
python.exe .\package_sequences.py -f Y:\healthfacts\ts\result.tseries.json.txt -c csv -b ohdsi -d Y:\ds\ts\output\
```

#### Package as HDF5 file

```bash
python.exe .\package_sequences.py -f Y:\healthfacts\ts\result.tseries.json.txt -c csv -b ohdsi -d Y:\ds\ts\output\
```

