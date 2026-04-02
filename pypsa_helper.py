import pandas as pd


def prepare_ac_lines(net, config: dict):
    lines = net.lines.copy()
    types = net.line_types

    lines["r"] = lines.apply(
        lambda row: row["r"] if row["r"] != 0 else types.loc[row["type"]].r_per_length * row["length"], axis=1)
    lines["x"] = lines.apply(
        lambda row: row["x"] if row["x"] != 0 else types.loc[row["type"]].x_per_length * row["length"], axis=1)
    lines["b"] = lines.apply(
        lambda row: row["b"] if row["b"] != 0 else types.loc[row["type"]].x_per_length * row["length"], axis=1)

    if "name" not in lines.columns or lines["name"].isnull().all():
        lines["name"] = "c1"

    return lines


def prepare_dc_links(net, config: dict):
    links = net.links[net.links["carrier"] == "DC"].copy()
    links["r"] = 0.0
    links["x"] = 0.0
    links["b"] = 0.0
    links["pmax"] = links["p_nom"]
    links["id"] = links.index
    links["name"] = [f"DC_Link_{i}" for i in range(len(links))]

    return links[["bus0", "bus1", "r", "x", "b", "pmax", "id", "name"]]


def prepare_ac_lines_and_dc_links(net, config: dict):
    ac_lines = prepare_ac_lines(net, config)
    dc_links = prepare_dc_links(net, config)
    return pd.concat([ac_lines, dc_links], ignore_index=True)


def prepare_thermal_generators(net, config: dict):
    thermal_types = ['OCGT', 'biomass', 'CCGT', 'nuclear', 'oil', 'coal', 'lignite']
    gens = net.generators.copy()
    gens = gens[gens.carrier.isin(thermal_types)]

    gens["max_prod"] = gens["p_max_pu"] * gens["p_nom"]
    gens["min_prod"] = gens["p_min_pu"] * gens["p_nom"]
    gens["ramp_up"] = gens["ramp_limit_up"] * gens["p_nom"]
    gens["ramp_down"] = gens["ramp_limit_down"] * gens["p_nom"]
    gens["enable_invest"] = gens["p_nom_extendable"].astype(int)

    gens["id"] = gens.index
    return gens[[
        "id", "carrier", "bus", "max_prod", "min_prod",
        "ramp_up", "ramp_down", "start_up_cost",
        "enable_invest", "capital_cost", "marginal_cost"
    ]]


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


def prepare_renewable_generators(net, config: dict):
    renewable_types = ['solar-hsat', 'onwind', 'solar']
    gens = net.generators.copy()
    vres = gens[gens.carrier.isin(renewable_types)].copy()

    vres["max_prod"] = vres["p_max_pu"] * vres["p_nom"]
    vres["enable_invest"] = vres["p_nom_extendable"].astype(int)

    vres["id"] = vres.index.values
    return vres[[
        "id", "carrier", "bus", "max_prod",
        "enable_invest", "p_nom_max", "capital_cost", "marginal_cost"
    ]]


def prepare_ror_generators(net, config: dict):
    ror = net.generators[net.generators.carrier == "ror"].copy()

    ror["id"] = ror.index
    ror["max_prod"] = ror["p_max_pu"] * ror["p_nom"]
    ror["min_prod"] = ror["p_min_pu"] * ror["p_nom"]
    ror["discharge"] = ror["efficiency"]
    ror["is_hydro"] = 1
    ror["enable_invest"] = ror["p_nom_extendable"].astype(int)

    return ror[[
        "id", "carrier", "bus", "max_prod", "min_prod", "discharge", "is_hydro",
        "marginal_cost", "enable_invest", "p_nom_max", "capital_cost"
    ]]


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
