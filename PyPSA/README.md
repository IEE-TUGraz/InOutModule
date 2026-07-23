# How to translate a PyPSA-Eur dataset to LEGO

This guide explains how to convert an existing PyPSA-Eur network file (`.nc`) to LEGO Excel files.

To build the PyPSA-Eur dataset first, follow
[How to create a PyPSA-Eur dataset](https://github.com/IEE-TUGraz/pypsa-eur-multivoltage/blob/fix/corine-bool-and-offshore-keyerror/README_pypsa_dataset.md)
in the `pypsa-eur-multivoltage` repository.

Since the conversion can require a lot of memory and may also take some time, it is recommended to use a powerful
machine.

## 1. Set the mapping metadata

In `pypsa_lego_mapping_config.yaml`, update the dataset fields that are marked with the placeholder value
`to be filled out`.

Example for a dataset with the target year 2025 and weather year 2013:

```yaml
Metadata:
  dataSource: "PyPSA-Eur_2025_CY2013"

dData_Packages:
  comments: "PyPSA-Eur dataset for target year 2025 and weather year 2013. The (fuel) costs for thermal generators defined in the Metadata section are default assumptions."

dData_Sources:
  personResponsible: "Jane Doe"
  lastEdited: "21.07.2026"
  comments: "PyPSA-Eur dataset for target year 2025 and weather year 2013. The (fuel) costs for thermal generators defined in the Metadata section are default assumptions."

dGlobal_Scenarios:
  comments: "PyPSA-Eur 2025 CY2013"
```

What these fields mean:

- `dataSource`: Name of the source dataset.
- `dData_Packages.comments`: Comment written to `Data_Packages.xlsx`.
- `dData_Sources.personResponsible`: The person responsible for the source entry (your name).
- `dData_Sources.lastEdited`: Date when the source entry was last edited.
- `dData_Sources.comments`: Comment written to `Data_Sources.xlsx`.
- `dGlobal_Scenarios.comments`: Comment written to `Global_Scenarios.xlsx`.

> **Note:** The fuel costs of thermal technologies defined in the `pypsa_lego_mapping_config.yaml` are only dummy values and should get adapted for your specific case study!

## 2. Translate to LEGO

From the `PyPSA/` folder, run the converter with:

```
python PypsaReader.py <input directory> <input file> <output directory> <output folder name>
```

Example:

```
python PypsaReader.py "../../pypsa-eur-multivoltage/resources/PyPSA-Eur_2025_CY2013/networks" "base_s_all_elec.nc" "../../" "LEGO_PyPSA-Eur_2025_CY2013"
```

This will write the LEGO Excel files to:

```
../../LEGO_PyPSA-Eur_2025_CY2013
```

## About the generated files

The converter generates the following LEGO files:

- `Data_Packages.xlsx`
- `Data_Sources.xlsx`
- `Global_Scenarios.xlsx`
- `Power_BusInfo.xlsx`
- `Power_Demand.xlsx`
- `Power_Hindex.xlsx`
- `Power_Inflows.xlsx`
- `Power_Network.xlsx`
- `Power_Storage.xlsx`
- `Power_ThermalGen.xlsx`
- `Power_VRES.xlsx`
- `Power_VRESProfiles.xlsx`
- `Power_WeightsK.xlsx`
- `Power_WeightsRP.xlsx`

The following files are not generated and have to be added and adapted manually:

- `Global_Parameters.xlsx`
- `Power_Parameters.xlsx`

# Reader Documentation

The `PypsaReader` module provides a specialized pipeline for converting PyPSA (Python for Power System Analysis)
networks into the Excel-based data
format required by the LEGO model.

## Purpose & limitations

- This reader only converts PyPSA networks to LEGO input files (EXCEL). It does not return a LEGO CaseStudy object.
- For running the converted case study in LEGO, the files `Global_Parameters.xlsx` and `Power_Parameters.xlsx` still
  have to be provided manually.
- Currently, only the power sector is supported by the converter.
- The generated Excel files include all the required information for importing into the LEGO database.

## General Workflow

The conversion process follows a structured workflow:

1. **Network Loading**: A PyPSA network is loaded from a NetCDF (`.nc`) file.
2. **Configuration Parsing**: The `NetworkDataExtractor` reads `pypsa_lego_mapping_config.yaml` to determine how PyPSA
   components (buses, lines,
   generators,
   etc.) map to LEGO tables.
3. **Data Extraction & Transformation**:
    - **Source Retrieval**: Data is pulled either directly from PyPSA network attributes (e.g., `net.buses`) or via
      complex processing in
      `pypsa_helper.py`.
    - **Filtering**: Technologies are filtered based on their `carrier` attribute using definitions in the `Metadata`
      section of the config.
    - **Column Mapping**: PyPSA attributes are mapped to LEGO column names.
    - **Unit Conversion**: The `Conversions` class applies transformations (e.g., MW to kW, EUR to MEUR, or calculating
      decommissioning years).
4. **Normalization**: DataFrames are augmented with mandatory LEGO columns (`scenario`, `id`, `dataPackage`,
   `dataSource`) and aligned with
   definitions in `TableDefinitions.xml`.
5. **Excel Export**: The `ExcelWriter` saves each processed DataFrame into individual `.xlsx` files. <br> **Warning:**
   This can take up to several
   hours for very
   large networks!

## Configuration File (`pypsa_lego_mapping_config.yaml`)

The YAML configuration acts as the "translation map" between the two models.

### Metadata Section

* **Technology Filters**: Defines list of `carrier` strings that identify specific technologies (e.g.,
  `solar: filter: ['solar', 'Solar']`).
* **Fuel Costs**: Specifies fuel prices in EUR/MWh for thermal technologies.
* **Categories**: Groups individual technologies into broader LEGO categories like `ThermalGen` or `VRES` for batch
  processing.
* **Global Settings**: Defines `dataPackage`, `dataSource`, and thresholds like `LDES_threshold`.

### Table Mapping Section

Each entry (e.g., `dPower_ThermalGen`) defines:

* **source**: The data origin (`type: attribute` for direct PyPSA access or `type: helper` for function calls).
* **category**: (Optional) References a metadata category to automatically filter the source data.
* **index**: Defines the LEGO index columns (e.g., `i` for bus, `g` for generator).
* **mapping**: A dictionary where keys are LEGO columns and values are either:
    - A direct PyPSA attribute name.
    - A static value (int/float).
    - A dictionary specifying an `attr`, a `conversion` function, or a `factor`.
* **metadata_arg**: (Optional) Passes a single value from the `Metadata` section into the helper
  function.

## Helper Functions (`pypsa_helper.py`)

When simple attribute mapping is insufficient, helper functions handle complex data aggregation and profile generation:

* **Network Topology**:
    - `prepare_ac_lines_and_dc_links`: Combines PyPSA `lines`, `links` (filtered for DC), and `transformers` into a
      single LEGO `Power_Network` table.
    - `prepare_ac_lines` / `prepare_transformers`: Calculate missing electrical parameters ($r, x, b$) based on standard
      type definitions if they are
      not explicitly set.
* **Time-Series Profiles**:
    - `prepare_renewable_profiles`: Extracts `p_max_pu` for VRES generators and reshapes it into a long-form LEGO
      format.
    - `prepare_inflow_profiles`: Aggregates inflow data from both `storage_units` (hydro) and `generators` (
      Run-of-River) into a unified time-series.
    - `prepare_demand_profiles`: Sums PyPSA load data per bus and formats it for the LEGO demand table.
* **Package, Scenario and Time Data**:
    - `prepare_data_packages`: Creates the data package information.
    - `prepare_data_sources`: Creates the data source information.
    - `prepare_global_scenarios`: Creates the scenario information.
    - `prepare_power_hindex`: Creates the relation between periods and representative periods.
    - `prepare_power_weights_k`: Creates the representative time step weights.
    - `prepare_power_weights_rp`: Creates the representative period weights.

## Conversions & Logic

The `Conversions` class in `PypsaReader.py` contains static methods for specialized logic:

* **Unit Scaling**: `EUR_to_MEUR`, `MW_to_kW`, `V_to_kV`.
* **Calculated Values**: `year_and_lifetime_to_year_decom` (derives decommissioning date) and
  `total_capacity_to_number_of_units`.
* **LEGO Specifics**: `is_ldes` (Long Duration Energy Storage detection) and `line_carrier_to_tec_repr` (mapping
  carriers to LEGO technical
  representations like `DC-OPF` or `TP`).
