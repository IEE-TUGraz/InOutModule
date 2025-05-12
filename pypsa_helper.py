import pandas as pd

def prepare_ac_lines(net):
    lines = net.lines.copy()
    types = net.line_types

    lines["r"] = lines.apply(
        lambda row: row["r"] if row["r"] != 0 else types.loc[row["type"]].r_per_length * row["length"], axis=1)
    lines["x"] = lines.apply(
        lambda row: row["x"] if row["x"] != 0 else types.loc[row["type"]].x_per_length * row["length"], axis=1)
    lines["b"] = lines.apply(
        lambda row: row["b"] if row["b"] != 0 else types.loc[row["type"]].x_per_length * row["length"], axis=1)

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
    profiles = profiles.rename(columns={"index": "snapshot"})

    return profiles  # flat, column-based, no index set yet


