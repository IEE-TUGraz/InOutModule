from typing import Literal, Dict

import numpy as np
import pandas as pd
import tsam.timeseriesaggregation as tsam

from InOutModule.printer import Printer

printer = Printer.getInstance()


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
        printer.warning(f"Some inflows correspond to generators which are not in Power_VRES (or have MaxProd=0). They will be ignored: {df[df['MaxProd'].isna() | (df['MaxProd'] == 0)].index.get_level_values('g').unique()}")
        df = df.dropna(subset=['MaxProd'])
        df = df[df['MaxProd'] != 0]

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
        mask = vresProfiles_df.index.get_level_values('g').isin(inflow_generators)
        vresProfiles_df.drop(vresProfiles_df.index[mask], inplace=True)

    return df.set_index(['rp', 'k', 'g']).sort_index(level="k")


def apply_kmedoids_aggregation(
        case_study,
        k: int,
        rp_length: int = 24,
        cluster_strategy: Literal["aggregated", "disaggregated"] = "aggregated",
        capacity_normalization: Literal["installed", "maxInvestment"] = "maxInvestment",
        sum_production: bool = False
):
    """
    Apply k-medoids temporal aggregation to a CaseStudy object.
    Each scenario from dGlobal_Scenarios is processed independently.

    Args:
        case_study: The CaseStudy object to aggregate
        k: Number of representative periods to create
        rp_length: Hours per representative period (e.g., 24, 48)
        cluster_strategy: "aggregated" (sum across buses) or "disaggregated" (keep buses separate)
        capacity_normalization: "installed" or "maxInvestment" for VRES capacity factor weighting
        sum_production: If True, sum all technologies into single production column

    Returns:
        CaseStudy: New clustered CaseStudy object
    """

    # Create a deep copy to avoid modifying the original
    aggregated_case_study = case_study.copy()

    # Get scenario names
    scenario_names = aggregated_case_study.dGlobal_Scenarios.index.values

    # Process each scenario independently
    all_processed_data = {}
    for scenario in scenario_names:
        print(f"\n=== Processing scenario: {scenario} ===")

        print(f"  Step 1: Extracting data for scenario {scenario}")
        scenario_clustering_data = _extract_scenario_data(case_study, scenario, capacity_normalization)

        if len(scenario_clustering_data) == 0:
            raise ValueError(f"No data found for scenario {scenario}")

        print(f"  Found {len(scenario_clustering_data)} data points for clustering")

        print(f"  \nStep 2: Preparing data using {cluster_strategy} strategy")
        if cluster_strategy == "disaggregated":
            pivot_df = _prepare_disaggregated_data(scenario_clustering_data, sum_production)
        else:
            pivot_df = _prepare_aggregated_data(scenario_clustering_data, sum_production)

        print(f"  Prepared {len(pivot_df)} time periods for clustering")

        print(f"  \nStep 3: Running k-medoids clustering (k={k}, rp_length={rp_length})")
        aggregation_result = _run_kmedoids_clustering(pivot_df, k, rp_length)

        print(f"  \nStep 4: Building representative period data")
        demand_data, vres_data = _build_representative_periods(
            case_study, scenario, aggregation_result, rp_length
        )

        print(f"  \nStep 5: Building weights and hour indices")
        weights_rp, weights_k, hindex = _build_scenario_weights_and_indices(
            aggregation_result, scenario, rp_length
        )

        all_processed_data[scenario] = {
            'demand': demand_data,
            'vres_profiles': vres_data,
            'weights_rp': weights_rp,
            'weights_k': weights_k,
            'hindex': hindex
        }
        print(f"Scenario {scenario} completed successfully")

    # Update CaseStudy with aggregated data
    _update_casestudy_with_scenarios(aggregated_case_study, all_processed_data)

    print(f"\nAll scenarios have been processed and combined successfully!")
    return aggregated_case_study


def _extract_scenario_data(case_study, scenario: str, capacity_normalization: str) -> pd.DataFrame:
    """Extract and combine demand and VRES data for a single scenario - OPTIMIZED."""

    # Extract demand data for this scenario
    demand_df = case_study.dPower_Demand.reset_index()
    demand_df = demand_df[demand_df['scenario'] == scenario].copy()

    if len(demand_df) == 0:
        raise ValueError(f"No demand data found for scenario {scenario}")

    # Initialize with demand data
    scenario_df = demand_df[['scenario', 'rp', 'i', 'k', 'value']].rename(columns={'value': 'demand'})

    # Process VRES data if available
    if (hasattr(case_study, 'dPower_VRES') and case_study.dPower_VRES is not None and
            hasattr(case_study, 'dPower_VRESProfiles') and case_study.dPower_VRESProfiles is not None):

        # Get VRES data for this scenario
        vres_df = case_study.dPower_VRES.reset_index()
        vres_df = vres_df[vres_df['scenario'] == scenario].copy()

        # Get VRES profiles for this scenario
        vres_profiles_df = case_study.dPower_VRESProfiles.reset_index()
        vres_profiles_df = vres_profiles_df[vres_profiles_df['scenario'] == scenario].copy()

        if len(vres_df) > 0 and len(vres_profiles_df) > 0:
            # Merge of VRES with VRESProfiles
            vres_with_profiles = pd.merge(
                vres_profiles_df,
                vres_df[['g', 'tec', 'i', 'ExisUnits', 'MaxProd', 'EnableInvest', 'MaxInvest']],
                on='g',
                how='left'
            )

            # Apply capacity normalization (vectorized)
            if capacity_normalization == "installed":
                normalization_factor = vres_with_profiles['ExisUnits'].fillna(0)
            else:  # maxInvestment
                normalization_factor = np.maximum(
                    vres_with_profiles['ExisUnits'].fillna(0),
                    vres_with_profiles['EnableInvest'].fillna(0) * vres_with_profiles['MaxInvest'].fillna(0)
                )

            # Calculate weighted capacity factor
            vres_with_profiles['weighted_cf'] = (
                    vres_with_profiles['value'].fillna(0) *
                    vres_with_profiles['MaxProd'].fillna(0) *
                    normalization_factor
            )

            # Pivot technologies as columns
            vres_with_profiles = vres_with_profiles.pivot_table(
                index=['scenario', 'rp', 'k', 'g', 'i'],
                columns='tec',
                values='weighted_cf',
                fill_value=0
            ).reset_index().drop(columns=['g'])

            # Merge with demand data
            scenario_df = pd.merge(
                scenario_df,
                vres_with_profiles,
                on=['scenario', 'rp', 'k', 'i'],
                how='left'
            )

    return scenario_df


def _prepare_disaggregated_data(scenario_df: pd.DataFrame, sum_production: bool) -> pd.DataFrame:
    """Prepare data for disaggregated clustering (keeps buses separate)."""
    result_df = scenario_df.copy()

    if sum_production:
        result_df = _sum_technology_columns(result_df)

    return result_df


def _prepare_aggregated_data(scenario_df: pd.DataFrame, sum_production: bool) -> pd.DataFrame:
    """Prepare data for aggregated clustering (sum across buses)."""
    grouping_cols = ['scenario', 'rp', 'k']
    exclude_cols = grouping_cols + ['i']
    value_cols = [col for col in scenario_df.columns if col not in exclude_cols]

    # Aggregate across buses
    aggregated_df = scenario_df.groupby(grouping_cols)[value_cols].sum().reset_index()

    if sum_production:
        aggregated_df = _sum_technology_columns(aggregated_df)

    return aggregated_df


def _sum_technology_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Sum all technology columns into a single 'production' column."""
    result_df = df.copy()
    exclude_cols = {'scenario', 'rp', 'i', 'k', 'demand'}
    tech_cols = [col for col in df.columns if col not in exclude_cols]

    if tech_cols:
        result_df['production'] = df[tech_cols].sum(axis=1)
        result_df = result_df.drop(columns=tech_cols)

    return result_df


def _run_kmedoids_clustering(pivot_df: pd.DataFrame, k: int, rp_length: int):
    """Run k-medoids clustering using tsam."""

    # Prepare data for tsam
    pivot_df_sorted = pivot_df.sort_values('k')

    # Create datetime index
    pivot_df_sorted['datetime'] = pd.date_range(start='2010-01-01', periods=len(pivot_df_sorted), freq='h')

    # Drop grouping columns and set datetime index
    clustering_data = pivot_df_sorted.drop(columns=['scenario', 'rp', 'k']).set_index('datetime')

    print(f"    Running k-medoids with {k} clusters, {rp_length} hours/period, {len(clustering_data)} total hours")

    # Run clustering
    aggregation = tsam.TimeSeriesAggregation(
        clustering_data,
        noTypicalPeriods=k,
        hoursPerPeriod=rp_length,
        clusterMethod='k_medoids',
        rescaleClusterPeriods=False
    )

    typical_periods = aggregation.createTypicalPeriods()
    print(f"    Clustering completed. Created {len(typical_periods)} typical periods.")
    print(f"    Cluster center indices (medoids): {aggregation.clusterCenterIndices}")

    return aggregation


def _build_representative_periods(case_study, scenario: str, aggregation, rp_length: int):
    """Build demand and VRES profile data for representative periods."""

    def _extract_numeric_and_calc_p(df, rp_length):
        """Extract numeric values from rp/k strings and calculate absolute hour."""
        df['rp_num'] = df['rp'].str.extract(r'(\d+)').astype(int)
        df['k_num'] = df['k'].str.extract(r'(\d+)').astype(int)
        df['p'] = (df['rp_num'] - 1) * rp_length + df['k_num']
        return df

    # Process demand data
    demand_original = case_study.dPower_Demand.reset_index()
    demand_original = demand_original[demand_original['scenario'] == scenario].copy()
    demand_original = _extract_numeric_and_calc_p(demand_original, rp_length)

    demand_data = []
    for cluster_idx, medoid_period in enumerate(aggregation.clusterCenterIndices):
        rp_new = f'rp{cluster_idx + 1:02d}'
        medoid_hours = range(medoid_period * rp_length + 1, (medoid_period + 1) * rp_length + 1)
        medoid_demand_data = demand_original[demand_original['p'].isin(medoid_hours)]

        for k_offset, abs_hour in enumerate(medoid_hours, start=1):
            k_new = f'k{k_offset:02d}'
            hour_demand = medoid_demand_data[medoid_demand_data['p'] == abs_hour]

            for _, row in hour_demand.iterrows():
                demand_data.append({
                    'rp': rp_new,
                    'i': row['i'],
                    'k': k_new,
                    'scenario': scenario,
                    'value': row['value']
                })

    # Process VRES data if available
    vres_data = []
    if hasattr(case_study, 'dPower_VRESProfiles') and case_study.dPower_VRESProfiles is not None:
        vres_original = case_study.dPower_VRESProfiles.reset_index()
        vres_original = vres_original[vres_original['scenario'] == scenario].copy()
        vres_original = _extract_numeric_and_calc_p(vres_original, rp_length)

        for cluster_idx, medoid_period in enumerate(aggregation.clusterCenterIndices):
            rp_new = f'rp{cluster_idx + 1:02d}'
            medoid_hours = range(medoid_period * rp_length + 1, (medoid_period + 1) * rp_length + 1)
            medoid_vres_data = vres_original[vres_original['p'].isin(medoid_hours)]

            for k_offset, abs_hour in enumerate(medoid_hours, start=1):
                k_new = f'k{k_offset:02d}'
                hour_vres = medoid_vres_data[medoid_vres_data['p'] == abs_hour]

                for _, row in hour_vres.iterrows():
                    vres_data.append({
                        'rp': rp_new,
                        'k': k_new,
                        'g': row['g'],
                        'scenario': scenario,
                        'value': row['value']
                    })

    return demand_data, vres_data


def _build_scenario_weights_and_indices(aggregation, scenario: str, rp_length: int):
    """Build representative period weights and hour indices for a single scenario."""

    # RP weights
    weights_rp = []
    for rp_idx, weight in aggregation._clusterPeriodNoOccur.items():
        weights_rp.append({
            'rp': f'rp{rp_idx + 1:02d}',
            'scenario': scenario,
            'pWeight_rp': int(weight)
        })

    # K weights (all 1 for hourly resolution)
    weights_k = []
    for k in range(1, rp_length + 1):
        weights_k.append({
            'k': f'k{k:02d}',
            'scenario': scenario,
            'pWeight_k': 1
        })

    # Hindex mapping
    hindex = []
    for orig_p, cluster_id in enumerate(aggregation._clusterOrder):
        for k in range(1, rp_length + 1):
            hindex.append({
                'p': f'h{orig_p * rp_length + k:04d}',
                'rp': f'rp{cluster_id + 1:02d}',
                'k': f'k{k:02d}',
                'scenario': scenario
            })

    return weights_rp, weights_k, hindex


def _update_casestudy_with_scenarios(case_study, all_processed_data: Dict):
    """Update CaseStudy with aggregated data, maintaining original index structures."""

    # Collect all data across scenarios
    all_demand_data = []
    all_vres_data = []
    all_weights_rp_data = []
    all_weights_k_data = []
    all_hindex_data = []

    for scenario, scenario_data in all_processed_data.items():
        all_demand_data.extend(scenario_data['demand'])
        all_vres_data.extend(scenario_data['vres_profiles'])
        all_weights_rp_data.extend(scenario_data['weights_rp'])
        all_weights_k_data.extend(scenario_data['weights_k'])
        all_hindex_data.extend(scenario_data['hindex'])

    print(f"Updating CaseStudy with combined data:")

    if all_demand_data:
        demand_df = pd.DataFrame(all_demand_data)
        case_study.dPower_Demand = demand_df.set_index(['rp', 'i', 'k'])
        print(f"  - Updated demand: {len(all_demand_data)} entries")

    if all_vres_data:
        vres_df = pd.DataFrame(all_vres_data)
        case_study.dPower_VRESProfiles = vres_df.set_index(['rp', 'k', 'g'])
        print(f"  - Updated VRES profiles: {len(all_vres_data)} entries")

    if all_weights_rp_data:
        weights_rp_df = pd.DataFrame(all_weights_rp_data)
        case_study.dPower_WeightsRP = weights_rp_df.set_index(['rp'])
        print(f"  - Updated RP weights: {len(all_weights_rp_data)} entries")

    if all_weights_k_data:
        weights_k_df = pd.DataFrame(all_weights_k_data)
        case_study.dPower_WeightsK = weights_k_df.set_index(['k'])
        print(f"  - Updated K weights: {len(all_weights_k_data)} entries")

    if all_hindex_data:
        hindex_df = pd.DataFrame(all_hindex_data)
        case_study.dPower_Hindex = hindex_df.set_index(['p', 'rp', 'k'])
        print(f"  - Updated Hindex: {len(all_hindex_data)} entries")

    print("CaseStudy update completed successfully!")
