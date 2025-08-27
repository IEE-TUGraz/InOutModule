import pandas as pd


def inflowsToCapacityFactors(inflows_df: pd.DataFrame, vres_df: pd.DataFrame, vresProfiles_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert inflows to capacity factors and concat them to vresProfiles_df.

    - inflows_df: inflow data with inflows per generator (g) and representative period (rp).
    - vres_df: contains generator technical data, including 'MaxProd'.
    - vresProfiles_df: existing VRES profiles (indexed by rp, k, g).
    """
    df = inflows_df.copy()

    # Prepare vres_df with ['g','MaxProd']
    vres_tmp = vres_df.reset_index()[['g', 'MaxProd']]

    if vres_tmp['g'].duplicated().any():
        raise ValueError("Duplicated generator found in Power_VRES.")

    maxProd = vres_tmp.set_index('g')['MaxProd'].astype(float)

    # Join MaxProd into inflows
    df = df.join(maxProd, on='g', how='left')

    if df['MaxProd'].isna().any() or (df['MaxProd'] == 0).any():
        raise ValueError("MaxProd is missing or zero for some generators in inflows.")

    # Divide inflow value by MaxProd
    df['value'] = df['value'] / df['MaxProd']

    # Drop helper column
    df = df.drop(columns=['MaxProd'])

    return pd.concat([vresProfiles_df, df], axis=0)


def capacityFactorsToInflows(vresProfiles_df: pd.DataFrame, vres_df: pd.DataFrame, inflows_df: pd.DataFrame, remove_Inflows_from_VRESProfiles_inplace: bool = False) -> pd.DataFrame:
    """
    Convert capacity factors in vresProfiles_df back to inflows.

    - vresProfiles_df: DataFrame with capacity factors (indexed by rp, k, g).
    - vres_df: DataFrame containing generator technical data, including 'MaxProd'.
    - inflows_df: template inflows DataFrame (used to filter only those generators that are inflow-based).
    - remove_Inflows_from_VRESProfiles_inplace: if True, remove inflow generators from the original vresProfiles_df.
    """
    df = vresProfiles_df.reset_index()

    # Get list of inflow generators
    inflow_generators = inflows_df.reset_index()['g'].unique()

    # Prepare vres_df with ['g','MaxProd']
    vres_tmp = vres_df.reset_index()[['g', 'MaxProd']]

    if vres_tmp['g'].duplicated().any():
        raise ValueError("Duplicated generator found in Power_VRES.")

    maxProd = vres_tmp.set_index('g')['MaxProd'].astype(float)

    # Keep only inflow generators
    df = df[df['g'].isin(inflow_generators)]

    # Join MaxProd
    df = df.join(maxProd, on='g', how='left')

    if df['MaxProd'].isna().any() or (df['MaxProd'] == 0).any():
        raise ValueError("MaxProd is missing or zero for some generators in inflows.")

    # Multiply capacity factor by MaxProd
    df['value'] = df['value'] * df['MaxProd']

    # Drop helper column
    df = df.drop(columns=['MaxProd'])

    # Remove inflow generators from vresProfiles_df after calculation if requested
    if remove_Inflows_from_VRESProfiles_inplace:
        mask = ~vresProfiles_df.index.get_level_values('g').isin(inflow_generators)
        vresProfiles_df.drop(vresProfiles_df.index[~mask], inplace=True)

    return df.set_index(['rp', 'k', 'g']).sort_index(level="k")
