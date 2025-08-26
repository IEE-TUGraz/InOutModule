import numpy as np
import pandas as pd


def inflowsToCapacityFactor(inflows_df: pd.DataFrame, vres_df: pd.DataFrame, vresProfiles_df: pd.DataFrame) -> pd.DataFrame:
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

    # Ensure required metadata columns
    meta_cols = ['dataPackage', 'dataSource', 'id', 'scenario']
    for col in meta_cols:
        if col not in df.columns:
            df[col] = vresProfiles_df[col].iloc[0]

    # Restore index structure (rp, k, g)
    df = df.set_index(['rp', 'k', 'g'])

    return pd.concat([vresProfiles_df, df], axis=0).sort_index(level="k")
