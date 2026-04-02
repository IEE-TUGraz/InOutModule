import pandas as pd


def prepare_ac_lines(net, config: dict):
    lines = net.lines.copy()
    types = net.line_types

    # Map type attributes to lines for vectorized calculation
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

    return lines


def prepare_dc_links(net, config: dict):
    links = net.links[net.links["carrier"] == "DC"].copy()
    links["r"] = 0.0
    links["x"] = 0.0
    links["b"] = 0.0
    links["s_nom"] = links["p_nom"]
    links["s_nom_extendable"] = links["p_nom_extendable"]
    links["id"] = links.index
    # Vectorized name generation
    links["name"] = "DC_Link_" + pd.Series(range(len(links)), index=links.index).astype(str)

    # Add tap ratios and phase shifts with default values (if not already present)
    links['tap_ratio'] = 1
    links['phase_shift'] = 0

    return links


def prepare_transformers(net, config: dict) -> pd.DataFrame:
    transformers = net.transformers.copy()
    if "name" not in transformers.columns or transformers["name"].isnull().all():
        transformers["name"] = "c1"

    return transformers


def prepare_ac_lines_and_dc_links(net, config: dict):
    ac_lines = prepare_ac_lines(net, config)
    dc_links = prepare_dc_links(net, config)
    transformers = prepare_transformers(net, config)
    return pd.concat([ac_lines, dc_links, transformers], ignore_index=True)


def prepare_renewable_profiles(net, config: dict):
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
    df = net.loads_t.p_set.copy()  # shape: [time, load_id]
    df = df.rename_axis("k").reset_index()  # 'k' = time

    demand_long = df.melt(id_vars="k", var_name="g", value_name="Demand")
    demand_long["rp"] = "rp01"
    return demand_long[["rp", "g", "k", "Demand"]]
