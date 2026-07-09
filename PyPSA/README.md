# How to translate a PyPSA-Eur dataset to LEGO

This guide explains how to convert an existing PyPSA-Eur network file (`.nc`) to LEGO Excel files.

To build the PyPSA-Eur dataset first, follow
[How to create a PyPSA-Eur dataset](https://github.com/IEE-TUGraz/pypsa-eur-multivoltage/blob/fix/corine-bool-and-offshore-keyerror/README_pypsa_dataset.md)
in the `pypsa-eur-multivoltage` repository.

Since the conversion can require a lot of memory and may also take some time, it is recommended to use a powerful
machine.

## 1. Set the mapping metadata

In `pypsa_lego_mapping_config.yaml`, update the dataset metadata fields `dataPackage` and `dataSource`.

Use the name of the PyPSA-Eur dataset for `dataSource`.

Example:

```yaml
Metadata:
    dataPackage: "PyPSA-Eur"
    dataSource: "PyPSA-Eur_2025_CY2013"
```

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

- `Power_BusInfo.xlsx`
- `Power_Demand.xlsx`
- `Power_Inflows.xlsx`
- `Power_Network.xlsx`
- `Power_Storage.xlsx`
- `Power_ThermalGen.xlsx`
- `Power_VRES.xlsx`
- `Power_VRESProfiles.xlsx`

The following files are not generated and have to be added and adapted manually:

- `Data_Packages.xlsx`
- `Data_Sources.xlsx`
- `Global_Parameters.xlsx`
- `Global_Scenarios.xlsx`
- `Power_Hindex.xlsx`
- `Power_Parameters.xlsx`
- `Power_WeightsK.xlsx`
- `Power_WeightsRP.xlsx`

# Reader Documentation

The `PypsaReader` module provides a specialized pipeline for converting PyPSA (Python for Power System Analysis)
networks into the Excel-based data
format required by the LEGO model.

## Purpose & limitations

- This reader only converts PyPSA networks to LEGO input files (EXCEL). It does not return a LEGO CaseStudy object.
- For running the converted case study in LEGO, at least the files `Global_Parameters.xlsx`, `Power_Parameters.xlsx`,
  `Power_Hindex.xlsx`,
  `Power_WeightsK.xlsx` and `Power_WeightsRP.xlsx` must be defined.
- Currently, only the power sector is supported by the converter.
- For importing the generated Excel files into the LEGO database, the files `Global_Scenarios.xlsx`,
  `Data_Packages.xlsx` and `Data_Sources.xlsx` must
  be defined.

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

## Conversions & Logic

The `Conversions` class in `PypsaReader.py` contains static methods for specialized logic:

* **Unit Scaling**: `EUR_to_MEUR`, `MW_to_kW`, `V_to_kV`.
* **Calculated Values**: `year_and_lifetime_to_year_decom` (derives decommissioning date) and
  `total_capacity_to_number_of_units`.
* **LEGO Specifics**: `is_ldes` (Long Duration Energy Storage detection) and `line_carrier_to_tec_repr` (mapping
  carriers to LEGO technical
  representations like `DC-OPF` or `TP`).
