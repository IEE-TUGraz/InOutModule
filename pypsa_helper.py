import pandas as pd
import numpy as np


def prepare_ac_lines(net, config: dict):
    """
    Prepares AC line data by calculating missing parameters (r, x, b) from line types,
    converting them to per-unit values using BasePower and v_nom,
    and ensuring consistent naming and carrier definitions.
    """
    lines = net.lines.copy()
    types = net.line_types
    s_base = config.get("BasePower", 100)

    # Define the line parameters r, x, b if only line type is specified
    r_per_len = lines["type"].map(types["r_per_length"])
    x_per_len = lines["type"].map(types["x_per_length"])

    # Determine b_per_length, assuming 50 Hz
    # b_per_len [uS/km] = 2 * pi * 50 * C [nF/km] * 1e-3
    b_per_len = lines["type"].map(types["c_per_length"]) * 2 * np.pi * 50 * 1e-3

    # Vectorized calculation: replace 0 values with type-based defaults (SI values)
    # Note: b_per_length in PyPSA line_types is typically in uS/km, so we multiply by 1e-6 to get Siemens
    lines["r"] = lines["r"].where(lines["r"] != 0, r_per_len * lines["length"])
    lines["x"] = lines["x"].where(lines["x"] != 0, x_per_len * lines["length"])
    lines["b"] = lines["b"].where(lines["b"] != 0, b_per_len * lines["length"] * 1e-6)

    # Get v_nom from buses (kV) for each line (using bus0 as reference)
    v_nom = lines.bus0.map(net.buses.v_nom)

    # Calculate Z_base = V_nom^2 / S_base [Ohm]
    # Since V_nom is in kV and S_base is in MW, (kV^2 / MW) results in Ohms.
    z_base = (v_nom**2) / s_base

    # Convert electrical parameters to per-unit values
    lines["r"] = lines["r"] / z_base
    lines["x"] = lines["x"] / z_base
    lines["b"] = lines["b"] * z_base

    # Add tap ratios and phase shifts with default values (if not already present)
    lines["tap_ratio"] = 1
    lines["phase_shift"] = 0

    # Ensure every line has an individual circuit identifier (c1, c2, ...) for parallel lines
    lines["name"] = "c" + (lines.groupby(["bus0", "bus1"]).cumcount() + 1).astype(str)

    # if carrier is nan or empty string (''), set to AC for all lines to get defined as DC-OPF
    lines.carrier = lines.carrier.fillna("AC").where(lines.carrier != "", "AC")

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
    links["name"] = "DC_Link_" + pd.Series(range(len(links)), index=links.index).astype(
        str
    )

    # if carrier is nan or empty string (''), set to DC for all links to get defined as transport problem (TP)
    links.carrier = links.carrier.fillna("DC").where(links.carrier != "", "DC")

    # Add tap ratios and phase shifts with default values (if not already present)
    links["tap_ratio"] = 1
    links["phase_shift"] = 0

    return links


def prepare_transformers(net, config: dict) -> pd.DataFrame:
    """
    Calculates transformer electrical parameters (r, x, b) based on their types
    if they are not already defined, converts them to per-unit values based
    on the system BasePower and v_nom, and sets default values for LEGO compatibility.
    """
    transformers = net.transformers.copy()
    types = net.transformer_types
    s_base = config.get("BasePower", 100)

    # Define which values are considered "not defined" (0 or NaN)
    r_missing = (transformers["r"] == 0) | transformers["r"].isna()
    x_missing = (transformers["x"] == 0) | transformers["x"].isna()
    b_missing = (transformers["b"] == 0) | transformers["b"].isna()

    # Only perform type-based calculation if there are missing values
    if r_missing.any() or x_missing.any() or b_missing.any():
        vsc = transformers["type"].map(types["vsc"])
        nlc = transformers["type"].map(types["i0"])
        pfe = transformers["type"].map(types["pfe"])

        # Avoid division by zero for s_nom
        s_nom_safe = transformers.s_nom.where(transformers.s_nom != 0, 1)

        if r_missing.any():
            transformers.loc[r_missing, "r"] = vsc.loc[r_missing] / 100

        if x_missing.any():
            r_val = transformers["r"]
            transformers.loc[x_missing, "x"] = np.sqrt(
                np.maximum((vsc.loc[x_missing] / 100) ** 2 - r_val.loc[x_missing] ** 2, 0)
            )

        if b_missing.any():
            g = pfe / (1000 * s_nom_safe)
            transformers.loc[b_missing, "b"] = -np.sqrt(
                np.maximum((nlc.loc[b_missing] / 100) ** 2 - g.loc[b_missing] ** 2, 0)
            )

    # Get v_nom from buses (kV) for each transformer (using bus0 as reference)
    v_nom = transformers.bus0.map(net.buses.v_nom)

    # Calculate Z_base = V_nom^2 / S_base [Ohm]
    z_base = (v_nom**2) / s_base

    # Convert electrical parameters to per-unit values
    # Transformers r, x, b in PyPSA are usually p.u. on transformer base (s_nom)
    # To convert to system base (s_base):
    # Z_pu_sys = Z_pu_trans * (S_base / S_trans)
    # Y_pu_sys = Y_pu_trans * (S_trans / S_base)
    s_nom = transformers.s_nom.where(transformers.s_nom != 0, 1)
    scaling_factor_z = s_base / s_nom
    scaling_factor_y = s_nom / s_base

    transformers["r"] = transformers["r"] * scaling_factor_z
    transformers["x"] = transformers["x"] * scaling_factor_z
    transformers["b"] = transformers["b"] * scaling_factor_y

    # Set carrier to AC for all transformers to get defined as DC-OPF
    transformers["carrier"] = "AC"

    # Ensure every transformer has an individual circuit identifier (c1, c2, ...) for parallel transformers
    transformers["name"] = "c" + (
        transformers.groupby(["bus0", "bus1"]).cumcount() + 1
    ).astype(str)

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

    # Change the index to k0001, k0002, ...
    num_timesteps = len(profiles)
    profiles.index = [f"k{i + 1:04d}" for i in range(num_timesteps)]

    # Ensure the index (snapshots) has a known name before resetting
    profiles = (
        profiles.rename_axis("k")
        .reset_index()
        .melt(id_vars="k", var_name="generator_id", value_name="Capacity")
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
        print(
            "Warning: No hydro storage units have inflow data. Storage inflow profiles will be empty."
        )
        inflow_storage = net.storage_units_t.inflow.copy()
    else:
        inflow_storage = net.storage_units_t.inflow[existing_inflows].copy()
        if len(missing_inflows) > 0:
            print(
                f"Warning: The following hydro storage units are missing inflow data and will not be defined: {missing_inflows}"
            )

    # Get RoR generator inflows
    ror_ids = net.generators.query(config["source"]["filter"]).index.to_list()
    set_ror_ids = set(ror_ids)
    set_ror_inflow_columns = set(net.generators_t.p_max_pu.columns.to_list())

    # check if inflows are specified for RoR generators
    existing_ror_inflows = list(set_ror_ids & set_ror_inflow_columns)
    missing_ror_inflows = list(set_ror_ids - set_ror_inflow_columns)

    if len(existing_ror_inflows) == 0:
        print(
            "Warning: No RoR generators have inflow data. RoR inflow profiles will be empty."
        )
        inflow_ror = pd.DataFrame()
    else:
        ror = net.generators.loc[existing_ror_inflows].copy()
        inflow_ror = net.generators_t.p_max_pu[existing_ror_inflows].copy()
        inflow_ror = inflow_ror.mul(ror["p_nom"], axis=1)
        if len(missing_ror_inflows) > 0:
            print(
                f"Warning: The following RoR generators are missing inflow data and will not be defined: {missing_ror_inflows}"
            )

    # Concatenate both: hydro + RoR inflows → [time, generator]
    combined = pd.concat([inflow_storage, inflow_ror], axis=1)

    # sanitize combined inflow profiles for LEGO model
    combined.fillna(
        0, inplace=True
    )  # fill missing inflows with 0 (if any) to avoid NaNs in the final profiles

    # Change the index to k0001, k0002, ...
    num_timesteps = len(combined)
    combined.index = [f"k{i + 1:04d}" for i in range(num_timesteps)]

    combined = combined.T  # index: generator_id, columns: time

    # Convert to long format by renaming index to 'g' before resetting
    inflow_long = (
        combined.rename_axis("g")
        .reset_index()
        .melt(id_vars="g", var_name="k", value_name="Inflow")
    )
    inflow_long["rp"] = "rp01"  # add a dummy column for compatibility

    return inflow_long[["rp", "g", "k", "Inflow"]]


def prepare_demand_profiles(net, config: dict):
    """
    Extracts load demand profiles and formats them into a long-form DataFrame
    for LEGO representation.
    """
    df = net.loads_t.p_set.copy()  # shape: [time, load_id]

    # Map load IDs to bus IDs
    df.columns = df.columns.map(net.loads.bus)

    # Filter buses based on the filter defined in the config (e.g. from dPower_BusInfo)
    bus_filter = config.get("source", {}).get("filter")
    if bus_filter:
        valid_buses = net.buses.query(bus_filter).index
        # Only keep demands at valid buses
        df = df[df.columns.intersection(valid_buses)]
    else:
        valid_buses = net.buses.index

    # Aggregate demand per bus
    if not df.empty:
        df = df.groupby(level=0, axis=1).sum()

    # Ensure all valid buses are present in the demand DataFrame
    missing_buses = valid_buses.difference(df.columns)
    if not missing_buses.empty:
        index = df.index if not df.empty else net.snapshots
        zero_demand = pd.DataFrame(0.0, index=index, columns=missing_buses)
        df = pd.concat([df, zero_demand], axis=1)

    # Change the index to k0001, k0002, ...
    num_timesteps = len(df)
    df.index = [f"k{i + 1:04d}" for i in range(num_timesteps)]
    df = df.rename_axis("k").reset_index()

    demand_long = df.melt(id_vars="k", var_name="n", value_name="Demand")
    demand_long["rp"] = "rp01"
    return demand_long[["rp", "n", "k", "Demand"]]
