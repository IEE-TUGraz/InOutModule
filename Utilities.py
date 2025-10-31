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
        data = _build_representative_periods(
            case_study, scenario, aggregation_result, rp_length
        )

        print(f"  \nStep 5: Building weights and hour indices")
        weights_rp, weights_k, hindex = _build_scenario_weights_and_indices(
            aggregation_result, scenario, rp_length
        )

        all_processed_data[scenario] = {
            'Power_Demand': data["Power_Demand"],
            'Power_VRESProfiles': data["Power_VRESProfiles"] if "Power_VRESProfiles" in data else [],
            'Power_Inflows': data["Power_Inflows"] if "Power_Inflows" in data else [],
            'weights_rp': weights_rp,
            'weights_k': weights_k,
            'hindex': hindex
        }
        print(f"Scenario {scenario} completed successfully")

    # Update CaseStudy with aggregated data
    _update_casestudy_with_scenarios(aggregated_case_study, all_processed_data)

    print(f"\nAll scenarios have been processed and combined successfully!")
    return aggregated_case_study


def _extract_scenario_data(case_study, scenario: str, capacity_normalization_strategy: str) -> pd.DataFrame:
    """Extract and combine demand, VRES, and inflows data for a single scenario."""

    def _apply_capacity_normalization_strategy(df, capacity_normalization_strategy):
        """Apply capacity normalization strategy to a dataframe with technology data."""
        if capacity_normalization_strategy == "installed":
            return df['ExisUnits'].fillna(0)
        else:  # maxInvestment
            return np.maximum(
                df['ExisUnits'].fillna(0),
                df['EnableInvest'].fillna(0) * df['MaxInvest'].fillna(0)
            )

    def _pivot_technologies(df, value_column, index_cols=None):
        """Pivot technologies as columns and drop 'g' column."""
        if index_cols is None:
            index_cols = ['scenario', 'rp', 'k', 'g', 'i']

        return df.pivot_table(
            index=index_cols,
            columns='tec',
            values=value_column,
            fill_value=0
        ).reset_index().drop(columns=['g'])

    # Extract demand data for this scenario
    demand_df = case_study.dPower_Demand.reset_index()
    demand_df = demand_df[demand_df['scenario'] == scenario].copy()

    if len(demand_df) == 0:
        raise ValueError(f"No demand data found for scenario {scenario}")

    # Initialize with demand data
    scenario_df = demand_df[['scenario', 'rp', 'i', 'k', 'value']].rename(columns={'value': 'demand'})

    vres_with_profiles = None
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

            # Apply capacity normalization and calculate weighted capacity factor
            normalization_factor = _apply_capacity_normalization_strategy(vres_with_profiles, capacity_normalization_strategy)
            vres_with_profiles['weighted_cf'] = (
                    vres_with_profiles['value'].fillna(0) *
                    vres_with_profiles['MaxProd'].fillna(0) *
                    normalization_factor
            )

            # Pivot technologies as columns
            vres_with_profiles = _pivot_technologies(vres_with_profiles, 'weighted_cf')

    inflows_with_tech = None
    # Process Inflows data if available
    if hasattr(case_study, 'dPower_Inflows') and case_study.dPower_Inflows is not None:
        # Get Inflows data for this scenario
        inflows_df = case_study.dPower_Inflows.reset_index()
        inflows_df = inflows_df[inflows_df['scenario'] == scenario].copy()

        if len(inflows_df) > 0:
            # Collect all inflows data from different sources
            inflows_parts = []

            # Try to merge with Power_VRES data
            if (hasattr(case_study, 'dPower_VRES') and case_study.dPower_VRES is not None and
                    'vres_df' in locals() and len(vres_df) > 0):
                inflows_with_vres = pd.merge(
                    inflows_df,
                    vres_df[['g', 'tec', 'i', 'ExisUnits', 'EnableInvest', 'MaxInvest']],
                    on='g',
                    how='left'
                )
                inflows_parts.append(inflows_with_vres)

            # Try to merge with Power_Storage data
            if hasattr(case_study, 'dPower_Storage') and case_study.dPower_Storage is not None:
                storage_df = case_study.dPower_Storage.reset_index()
                storage_df = storage_df[storage_df['scenario'] == scenario].copy()

                if len(storage_df) > 0:
                    inflows_with_storage = pd.merge(
                        inflows_df,
                        storage_df[['g', 'tec', 'i', 'ExisUnits', 'EnableInvest', 'MaxInvest']],
                        on='g',
                        how='inner'
                    )
                    inflows_parts.append(inflows_with_storage)

            # Combine all inflows parts
            if inflows_parts:
                inflows_with_tech = pd.concat(inflows_parts, ignore_index=True)

                # Apply capacity normalization
                normalization_factor = _apply_capacity_normalization_strategy(inflows_with_tech, capacity_normalization_strategy)
                inflows_with_tech['value'] = inflows_with_tech['value'].fillna(0) * normalization_factor

                # Pivot technologies as columns
                inflows_with_tech = _pivot_technologies(inflows_with_tech, 'value')

    # Combine VRES and inflows data
    combined_tech_data = None
    if vres_with_profiles is not None and inflows_with_tech is not None:
        combined_tech_data = pd.concat([vres_with_profiles, inflows_with_tech],
                                       ignore_index=True, sort=False)
    elif vres_with_profiles is not None:
        combined_tech_data = vres_with_profiles
    elif inflows_with_tech is not None:
        combined_tech_data = inflows_with_tech

    # Merge the combined technology data with scenario_df
    if combined_tech_data is not None:
        # Use right join to keep ALL demand data (even nodes without technology data)
        # Replicates demand for nodes with technology, and preserves demand-only nodes
        scenario_df = pd.merge(
            combined_tech_data,
            scenario_df,
            on=['scenario', 'rp', 'k', 'i'],
            how='right'
        )

        # Fill NaN values in technology columns with 0 for demand-only nodes
        tech_columns = [col for col in scenario_df.columns
                        if col not in ['scenario', 'rp', 'k', 'i', 'demand']]
        if tech_columns:
            scenario_df[tech_columns] = scenario_df[tech_columns].fillna(0)

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
        rescaleClusterPeriods=False,
        solver="gurobi"
    )

    typical_periods = aggregation.createTypicalPeriods()
    print(f"    Clustering completed. Created {len(typical_periods)} typical periods.")
    print(f"    Cluster center indices (medoids): {aggregation.clusterCenterIndices}")

    return aggregation


def _build_representative_periods(case_study, scenario: str, aggregation, rp_length: int):
    """Build demand, VRES profile, and inflows data for representative periods."""

    def _extract_numeric_and_calc_p(df, rp_length):
        """Extract numeric values from rp/k strings and calculate absolute hour."""
        df['rp_num'] = df['rp'].str[2:].astype(int)
        df['k_num'] = df['k'].str[1:].astype(int)
        df['p'] = (df['rp_num'] - 1) * rp_length + df['k_num']
        return df

    time_series_tables = [("Power_Demand", case_study.dPower_Demand)]
    if hasattr(case_study, 'dPower_VRESProfiles') and case_study.dPower_VRESProfiles is not None:
        time_series_tables.append(("Power_VRESProfiles", case_study.dPower_VRESProfiles))
    if hasattr(case_study, 'dPower_Inflows') and case_study.dPower_Inflows is not None:
        time_series_tables.append(("Power_Inflows", case_study.dPower_Inflows))

    data = {name: [] for name, _ in time_series_tables}
    for name, df in time_series_tables:
        df_original = df.reset_index()
        df_original = df_original[df_original['scenario'] == scenario].copy()
        df_original = _extract_numeric_and_calc_p(df_original, rp_length)

        for cluster_idx, medoid_period in enumerate(aggregation.clusterCenterIndices):
            rp_new = f'rp{cluster_idx + 1:02d}'
            medoid_hours = range(medoid_period * rp_length + 1, (medoid_period + 1) * rp_length + 1)
            medoid_data = df_original[df_original['p'].isin(medoid_hours)]

            for k_offset, abs_hour in enumerate(medoid_hours, start=1):
                k_new = f'k{k_offset:04d}'
                hour_data = medoid_data[medoid_data['p'] == abs_hour]

                for _, row in hour_data.iterrows():
                    row['rp'] = rp_new
                    row['k'] = k_new
                    data[name].append(row)

    return data


def _build_scenario_weights_and_indices(aggregation, scenario: str, rp_length: int):
    """Build representative period weights and hour indices for a single scenario."""

    # RP weights
    weights_rp = []
    for rp_idx, weight in aggregation._clusterPeriodNoOccur.items():
        weights_rp.append({
            'rp': f'rp{rp_idx + 1:02d}',
            'scenario': scenario,
            'pWeight_rp': int(weight),
            'id': None,
            "dataPackage": None,
            "dataSource": None,
        })

    # K weights (all 1 for hourly resolution)
    weights_k = []
    for k in range(1, rp_length + 1):
        weights_k.append({
            'k': f'k{k:04d}',
            'scenario': scenario,
            'pWeight_k': 1,
            'id': None,
            "dataPackage": None,
            "dataSource": None,
        })

    # Hindex mapping
    hindex = []
    for orig_p, cluster_id in enumerate(aggregation._clusterOrder):
        for k in range(1, rp_length + 1):
            hindex.append({
                'p': f'h{orig_p * rp_length + k:04d}',
                'rp': f'rp{cluster_id + 1:02d}',
                'k': f'k{k:04d}',
                'scenario': scenario,
                'id': None,
                "dataPackage": None,
                "dataSource": None,
            })

    return weights_rp, weights_k, hindex


def _update_casestudy_with_scenarios(case_study, all_processed_data: Dict):
    """Update CaseStudy with aggregated data, maintaining original index structures."""

    # Collect all data across scenarios
    all_demand_data = []
    all_vres_data = []
    all_inflows_data = []
    all_weights_rp_data = []
    all_weights_k_data = []
    all_hindex_data = []

    for scenario, scenario_data in all_processed_data.items():
        all_demand_data.extend(scenario_data['Power_Demand'])
        all_vres_data.extend(scenario_data['Power_VRESProfiles'])
        all_inflows_data.extend(scenario_data['Power_Inflows'])
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

    if all_inflows_data:
        inflows_df = pd.DataFrame(all_inflows_data)
        case_study.dPower_Inflows = inflows_df.set_index(['rp', 'k', 'g'])
        print(f"  - Updated inflows: {len(all_inflows_data)} entries")

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
