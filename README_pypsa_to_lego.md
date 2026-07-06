# How to translate a PyPSA-Eur dataset to LEGO

This guide explains how to convert an existing PyPSA-Eur network file (`.nc`) to LEGO Excel files.

To build the PyPSA-Eur dataset first, follow
[How to create a PyPSA-Eur dataset](https://github.com/IEE-TUGraz/pypsa-eur-multivoltage/blob/fix/corine-bool-and-offshore-keyerror/README_pypsa_dataset.md)
in the `pypsa-eur-multivoltage` repository.

It is recommended to connect to an IEE workstation via
[Remote Desktop](https://gitlab.tugraz.at/iee/iee4u/-/wikis/IT-Support/Remote-Desktop),
since the conversion can require a lot of memory and may also take some time.

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

Run the converter with:

```
python PypsaReader.py <input directory> <input file> <output directory> <output folder name>
```

Example:

```
python PypsaReader.py "../pypsa-eur-multivoltage/resources/PyPSA-Eur_2025_CY2013/networks" "base_s_all_elec.nc" "../" "LEGO_PyPSA-Eur_2025_CY2013"
```

This will write the LEGO Excel files to:

```
../LEGO_PyPSA-Eur_2025_CY2013
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
