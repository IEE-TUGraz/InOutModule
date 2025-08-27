import pandas as pd

def prepare_ac_lines(net):
    lines = net.lines.copy()
    types = net.line_types

    lines["r"] = lines.apply(
        lambda row: row["r"] if row["r"] != 0 else types.loc[row["type"]].r_per_length * row["length"], axis=1)
    lines["x"] = lines.apply(
        lambda row: row["x"] if row["x"] != 0 else types.loc[row["type"]].x_per_length * row["length"], axis=1)
    lines["b"] = lines.apply(
        lambda row: row["b"] if row["b"] != 0 else types.loc[row["type"]].c_per_length * row["length"], axis=1)

    lines["pmax"] = lines["s_nom"] * lines["s_max_pu"]
    lines["id"] = lines.index
    lines["name"] = [f"Line_{i}" for i in range(len(lines))]

    return lines[["bus0", "bus1", "r", "x", "b", "pmax", "id", "name"]]

def prepare_dc_links(net):
    links = net.links[net.links["carrier"] == "DC"].copy()
    links["r"] = 0.0
    links["x"] = 0.0
    links["b"] = 0.0
    links["pmax"] = links["p_nom"]
    links["id"] = links.index
    links["name"] = [f"DC_Link_{i}" for i in range(len(links))]

    return links[["bus0", "bus1", "r", "x", "b", "pmax", "id", "name"]]

def prepare_thermal_generators(net):
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

def prepare_renewable_profiles(net):
    renewable_types = ['solar-hsat', 'onwind', 'solar']
    gens = net.generators.copy()
    vres_gens = gens[gens.carrier.isin(renewable_types)]
    vres_ids = vres_gens.index.to_list()

    profiles = net.generators_t.p_max_pu[vres_ids].copy()
    profiles = profiles.reset_index().melt(
        id_vars="snapshot", var_name="generator_id", value_name="Capacity"
    )
    profiles = profiles.rename(columns={"index": "k"})
    profiles["rp"] = "rp01"  # add a dummy column for compatibility

    return profiles  # flat, column-based, no index set yet

def prepare_renewable_generators(net):
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

def prepare_ror_generators(net):
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

def prepare_storage_units(net):
    su = net.storage_units.copy()

    su["id"] = su.index
    su["max_prod"] = su["p_nom"] * su["p_max_pu"]
    su["min_prod"] = su["p_nom"] * su["p_min_pu"]  # note: often negative
    su["discharge"] = su["efficiency_dispatch"]
    su["charge"] = su["efficiency_store"]
    su["ini_reserve"] = su["state_of_charge_initial"]
    su["is_hydro"] = su["carrier"].isin(["PHS", "hydro"]).astype(int)
    su["enable_invest"] = su["p_nom_extendable"].astype(int)

    # Note: "min_reserve" is not present in PyPSA by default — we'll skip it
    return su[[
        "id", "carrier", "bus", "max_prod", "min_prod", "discharge", "charge",
        "ini_reserve", "is_hydro", "marginal_cost", "enable_invest", "p_nom_max",
        "capital_cost", "max_hours", "lifetime"
    ]]

def prepare_inflow_profiles(net):
    # Get hydro storage inflows
    hydro_ids = net.storage_units[net.storage_units["carrier"] == "hydro"].index.to_list()
    inflow_storage = net.storage_units_t.inflow[hydro_ids].copy()

    # Get RoR generator inflows
    ror = net.generators[net.generators.carrier == "ror"]
    ror_ids = ror.index.to_list()
    p_nom = ror["p_nom"]

    inflow_ror = net.generators_t.p_max_pu[ror_ids].copy()
    inflow_ror = inflow_ror.mul(p_nom, axis=1)

    # Concatenate both: hydro + RoR inflows → [time, generator]
    combined = pd.concat([inflow_storage, inflow_ror], axis=1)
    combined = combined.T  # index: generator_id, columns: time

    # Convert to long format
    inflow_long = combined.reset_index().melt(
        id_vars="index", var_name="k", value_name="Inflow"
    )
    inflow_long = inflow_long.rename(columns={"index": "g"})
    inflow_long["rp"] = "rp01"  # add a dummy column for compatibility

    return inflow_long[["rp", "g", "k", "Inflow"]]

def prepare_demand_profiles(net):
    df = net.loads_t.p_set.copy()  # shape: [time, load_id]
    df = df.rename_axis("k").reset_index()  # 'k' = time

    demand_long = df.melt(id_vars="k", var_name="g", value_name="Demand")
    demand_long["rp"] = "rp01"
    return demand_long[["rp", "g", "k", "Demand"]]


