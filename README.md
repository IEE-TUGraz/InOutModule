# InOutModule

Data I/O package for LEGO-Pyomo. Handles reading Excel case study files, writing results to Excel and SQLite, and utility data transformations.

## Key Files

| File                   | Purpose                                                                 |
|------------------------|-------------------------------------------------------------------------|
| `CaseStudy.py`         | Loads all Excel input files into a single object; data manipulation     |
| `ExcelReader.py`       | Low-level Excel parsing (per-file reader functions, version checking)   |
| `ExcelWriter.py`       | Writes formatted Excel output files, driven by `TableDefinitions.xml`  |
| `SQLiteWriter.py`      | Exports Pyomo model results to SQLite; stores solver stats & run params |
| `Utilities.py`         | Data transformations (inflows ↔ capacity factors, Printer helpers)      |
| `printer.py`           | Singleton console/logfile printer with severity levels                  |
| `Caller.py`            | Parallel job runner for batch experiments                               |
| `TableDefinitions.xml` | Declarative column/style definitions used by `ExcelWriter`              |
| `PypsaReader.py`       | Imports case studies from PyPSA network objects                         |
| `nrel118-reader.py`    | Converts NREL 118-bus data into LEGO Excel format                       |

## CaseStudy

### Constructor

```python
CaseStudy(
    data_folder: str | Path,
    do_not_scale_units: bool = False,
    do_not_merge_single_node_buses: bool = False,
    parallel_read: bool = True,
    n_jobs: int = 4,
    # Per-file overrides: pass a DataFrame to skip reading from disk
    dPower_ThermalGen: pd.DataFrame = None,
    ...
)
```

All Excel files in `data_folder` are read automatically. Any `d*` parameter can be passed directly as a DataFrame to bypass file reading (useful for programmatic construction or testing).

### DataFrame Attributes

| Attribute              | Time dependency | Source file                  |
|------------------------|-----------------|------------------------------|
| `dGlobal_Parameters`   | none            | `Global_Parameters.xlsx`     |
| `dGlobal_Scenarios`    | none            | `Global_Scenarios.xlsx`      |
| `dPower_Parameters`    | none            | `Power_Parameters.xlsx`      |
| `dPower_BusInfo`       | none            | `Power_BusInfo.xlsx`         |
| `dPower_Network`       | none            | `Power_Network.xlsx`         |
| `dPower_ThermalGen`    | none            | `Power_ThermalGen.xlsx`      |
| `dPower_VRES`          | none            | `Power_VRES.xlsx`            |
| `dPower_Storage`       | none            | `Power_Storage.xlsx`         |
| `dPower_Demand`        | rp + k          | `Power_Demand.xlsx`          |
| `dPower_VRESProfiles`  | rp + k          | `Power_VRESProfiles.xlsx`    |
| `dPower_Inflows`       | rp + k          | `Power_Inflows.xlsx`         |
| `dPower_ImportExport`  | rp + k          | `Power_ImportExport.xlsx`    |
| `dPower_Hindex`        | rp + k          | `Power_Hindex.xlsx`          |
| `dPower_WeightsRP`     | rp only         | `Power_WeightsRP.xlsx`       |
| `dPower_WeightsK`      | k only          | `Power_WeightsK.xlsx`        |

### Key Methods

| Method                         | Description                                              |
|--------------------------------|----------------------------------------------------------|
| `copy()`                       | Deep copy — safe to modify independently                 |
| `equal_to(cs)`                 | Compare all DataFrames with another CaseStudy            |
| `merge_single_node_buses()`    | Collapse single-bus zones; preserves `z` as union string |
| `scale_CaseStudy()`            | Applies power and cost scaling factors from parameters   |
| `get_rpTransitionMatrices()`   | Returns absolute and relative transition matrices        |

## SQLiteWriter

```python
from InOutModule.SQLiteWriter import model_to_sqlite, add_run_parameters_to_sqlite, add_solver_statistics_to_sqlite

model_to_sqlite(model, "output/results.sqlite")
add_solver_statistics_to_sqlite("output/results.sqlite", lego)
add_run_parameters_to_sqlite("output/results.sqlite", zoi="R1", dc_buffer=2)
```

- `model_to_sqlite()` exports all Pyomo variables, parameters, and sets; automatically appends objective decomposition and dual values.
- `add_run_parameters_to_sqlite()` creates a `run_parameters` table — evaluation scripts should read from this table rather than parsing filenames.

## Caller (Parallel Job Runner)

```bash
python InOutModule/Caller.py jobs.txt
python InOutModule/Caller.py jobs.txt --spawn 4   # open 4 parallel terminal windows
```

`jobs.txt` is a plain-text file with one shell command per line. Lines containing only `---` act as barriers: all workers wait until every job above the barrier is finished before continuing.

## Excel File Format

All input Excel files follow a versioned multi-sheet format:
- Each sheet corresponds to one scenario (or `ScenarioA` for deterministic runs).
- Sheets whose name starts with `~` are skipped.
- Cell `C2` on each sheet contains a version specifier (e.g., `v0.1.0`); mismatches produce a warning.

See `changelog-LEGOExcels.md` for version history of the Excel format.
