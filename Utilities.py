import numpy as np
import pandas as pd


def inflowsToCapacityFactors(inflows_df: pd.DataFrame, vres_df: pd.DataFrame, vresProfiles_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert inflows to capacity factors and concat them to vresProfiles_df.

    - inflows_df: inflow data with inflows per generator (g) and representative period (rp).
    - vres_df: contains generator technical data, including 'MaxProd'.
    - vresProfiles_df: existing VRES profiles (indexed by rp, k, g).
    """
    df = inflows_df.reset_index()

    # Prepare vres_df with ['g','MaxProd']
    vres_tmp = vres_df.reset_index()[['g', 'MaxProd']]
    maxprod = (
        vres_tmp.drop_duplicates('g')
        .set_index('g')['MaxProd']
        .astype(float)
    )

    # Merge MaxProd into inflows
    df = df.merge(maxprod, on='g', how='left')

    # Divide inflow value by MaxProd
    df['value'] = df['value'] / df['MaxProd'].replace(0, np.nan)

    # Drop helper column
    df = df.drop(columns=['MaxProd'])

    # Restore index structure (rp, k, g)
    df = df.set_index(['rp', 'k', 'g'])

    return pd.concat([vresProfiles_df, df], axis=0).sort_index(level="k")


def capacityFactorsToInflows(vresProfiles_df: pd.DataFrame, vres_df: pd.DataFrame, inflows_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert capacity factors in vresProfiles_df back to inflows.

    - vresProfiles_df: DataFrame with capacity factors (indexed by rp, k, g).
    - vres_df: DataFrame containing generator technical data, including 'MaxProd'.
    - inflows_df: template inflows DataFrame (used to filter only those generators that are inflow-based).
    """
    df = vresProfiles_df.reset_index()

    # Get list of inflow generators
    inflow_generators = inflows_df.reset_index()['g'].unique()

    # Prepare vres_df with ['g','MaxProd']
    vres_tmp = vres_df.reset_index()[['g', 'MaxProd']]
    maxprod = (
        vres_tmp.drop_duplicates('g')
        .set_index('g')['MaxProd']
        .astype(float)
    )

    # Keep only inflow generators
    df = df[df['g'].isin(inflow_generators)]

    # Merge MaxProd
    df = df.merge(maxprod, on='g', how='left')

    # Multiply capacity factor by MaxProd
    df['value'] = df['value'] * df['MaxProd']

    # Drop helper column
    df = df.drop(columns=['MaxProd'])

    return df.set_index(['rp', 'k', 'g']).sort_index(level="k")
