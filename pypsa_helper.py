import pandas as pd
import numpy as np


def prepare_ac_lines(net, config: dict):
    """
    Prepares AC line data by calculating missing parameters (r, x, b) from line types 
    and ensuring consistent naming and carrier definitions.
    """
    lines = net.lines.copy()
    types = net.line_types

    # Define the line parameters r, x, b if only line type is specified
    r_per_len = lines["type"].map(types["r_per_length"])
    x_per_len = lines["type"].map(types["x_per_length"])

    # Vectorized calculation: replace 0 values with type-based defaults
    lines["r"] = lines["r"].where(lines["r"] != 0, r_per_len * lines["length"])
    lines["x"] = lines["x"].where(lines["x"] != 0, x_per_len * lines["length"])
    lines["b"] = lines["b"].where(lines["b"] != 0, x_per_len * lines["length"])

    # Add tap ratios and phase shifts with default values (if not already present)
    lines['tap_ratio'] = 1
    lines['phase_shift'] = 0

    if "name" not in lines.columns or lines["name"].isnull().all():
        lines["name"] = "c1"

    # if carrier is nan or empty string (''), set to AC for all lines to get defined as DC-OPF
    lines.carrier = lines.carrier.fillna('AC').where(lines.carrier != '', 'AC')

    return lines


def prepare_dc_links(net, config: dict):
    """
    Extracts and prepares DC link data, initializing parameters for DC-OPF compatibility 
    and generating unique names.
    """
    links = net.links[net.links["carrier"] == "DC"].copy()

    # Define line parameters as nan for DC links, as they are not relevant for DC-OPF
    links["r"] = np.nan
    links["x"] = np.nan
    links["b"] = np.nan

    # Define s_nom and s_nom_extendable to be consistent with lines and transformers
    links["s_nom"] = links["p_nom"]
    links["s_nom_extendable"] = links["p_nom_extendable"]
    links["id"] = links.index
    # Vectorized name generation
    links["name"] = "DC_Link_" + pd.Series(range(len(links)), index=links.index).astype(str)

    # if carrier is nan or empty string (''), set to DC for all links to get defined as transport problem (TP)
    links.carrier = links.carrier.fillna('DC').where(links.carrier != '', 'DC')

    # Add tap ratios and phase shifts with default values (if not already present)
    links['tap_ratio'] = 1
    links['phase_shift'] = 0

    return links


def prepare_transformers(net, config: dict) -> pd.DataFrame:
    """
    Calculates transformer electrical parameters (r, x, b) based on their types 
    and sets default values for LEGO compatibility.
    """
    transformers = net.transformers.copy()
    types = net.transformer_types

    # Calculate r, x, b, based on transformer type if specified
    vsc = transformers["type"].map(types["vsc"])
    nlc = transformers["type"].map(types["i0"])
    pfe = transformers["type"].map(types["pfe"])
    g = pfe / (1000 * transformers.s_nom)

    transformers["r"] = transformers["r"].where(transformers["r"] != 0, vsc / 100)
    transformers["x"] = transformers["x"].where(transformers["x"] != 0, np.sqrt((vsc/100) ** 2 - transformers.r ** 2))
    transformers["b"] = transformers["b"].where(transformers["b"] != 0, - np.sqrt((nlc / 100) ** 2 - g ** 2))

    # Set carrier to AC for all transformers to get defined as DC-OPF
    transformers["carrier"] = "AC"

    if "name" not in transformers.columns or transformers["name"].isnull().all():
        transformers["name"] = "c1"

    return transformers


def prepare_ac_lines_and_dc_links(net, config: dict):
    """
    Combines AC lines, DC links, and transformers into a single DataFrame 
    for comprehensive network mapping.
    """
    ac_lines = prepare_ac_lines(net, config)
    dc_links = prepare_dc_links(net, config)
    transformers = prepare_transformers(net, config)
    return pd.concat([ac_lines, dc_links, transformers], ignore_index=True)


def prepare_renewable_profiles(net, config: dict):
    """
    Extracts renewable generation profiles (p_max_pu) for specified carriers 
    and formats them for LEGO input.
    """
    # renewable_types = ['Solar', 'Wind Onshore', 'Wind Offshore']
    gens = net.generators.copy()
    vres_gens = gens.query(config["source"]["filter"])
    vres_ids = vres_gens.index.to_list()

    profiles = net.generators_t.p_max_pu[vres_ids].copy()
    # Ensure the index (snapshots) has a known name before resetting
    profiles = profiles.rename_axis("k").reset_index().melt(
        id_vars="k", var_name="generator_id", value_name="Capacity"
    )
    profiles["rp"] = "rp01"  # add a dummy column for compatibility

    return profiles  # flat, column-based, no index set yet


def prepare_inflow_profiles(net, config: dict):
    """
    Aggregates inflow data from hydro storage units and Run-of-River generators 
    into a unified profile format.
    """
    # Get hydro storage inflows
    hydro_ids = net.storage_units.query(config["source"]["filter"]).index.to_list()
    set_hydro_ids = set(hydro_ids)
    set_inflow_columns = set(net.storage_units_t.inflow.columns.to_list())

    # check if inflows are specified for hydro storage units
    existing_inflows = list(set_hydro_ids & set_inflow_columns)
    missing_inflows = list(set_hydro_ids - set_inflow_columns)

    if len(existing_inflows) == 0:
        print("Warning: No hydro storage units have inflow data. Storage inflow profiles will be empty.")
        inflow_storage = net.storage_units_t.inflow.copy()
    else:
        inflow_storage = net.storage_units_t.inflow[existing_inflows].copy()
        if len(missing_inflows) > 0:
            print(f"Warning: The following hydro storage units are missing inflow data and will not be defined: {missing_inflows}")

    # Get RoR generator inflows
    ror_ids = net.generators.query(config["source"]["filter"]).index.to_list()
    set_ror_ids = set(ror_ids)
    set_ror_inflow_columns = set(net.generators_t.p_max_pu.columns.to_list())

    # check if inflows are specified for RoR generators
    existing_ror_inflows = list(set_ror_ids & set_ror_inflow_columns)
    missing_ror_inflows = list(set_ror_ids - set_ror_inflow_columns)

    if len(existing_ror_inflows) == 0:
        print("Warning: No RoR generators have inflow data. RoR inflow profiles will be empty.")
        inflow_ror = pd.DataFrame()
    else:
        ror = net.generators.loc[existing_ror_inflows].copy()
        inflow_ror = net.generators_t.p_max_pu[existing_ror_inflows].copy()
        inflow_ror = inflow_ror.mul(ror["p_nom"], axis=1)
        if len(missing_ror_inflows) > 0:
            print(f"Warning: The following RoR generators are missing inflow data and will not be defined: {missing_ror_inflows}")

    # Concatenate both: hydro + RoR inflows → [time, generator]
    combined = pd.concat([inflow_storage, inflow_ror], axis=1)
    combined = combined.T  # index: generator_id, columns: time

    # Convert to long format by renaming index to 'g' before resetting
    inflow_long = combined.rename_axis('g').reset_index().melt(
        id_vars="g", var_name="k", value_name="Inflow"
    )
    inflow_long["rp"] = "rp01"  # add a dummy column for compatibility

    return inflow_long[["rp", "g", "k", "Inflow"]]


def prepare_demand_profiles(net, config: dict):
    """
    Extracts load demand profiles and formats them into a long-form DataFrame 
    for LEGO representation.
    """
    df = net.loads_t.p_set.copy()  # shape: [time, load_id]
    df = df.rename_axis("k").reset_index()  # 'k' = time

    demand_long = df.melt(id_vars="k", var_name="n", value_name="Demand")
    demand_long["rp"] = "rp01"
    return demand_long[["rp", "n", "k", "Demand"]]
