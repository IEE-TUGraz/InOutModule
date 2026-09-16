# CLAUDE.md — InOutModule

This file provides guidance to Claude Code when working with code in this directory.
See `README.md` for usage, key concepts, and data structure.

## Architecture Notes

### CaseStudy

- Sequential reads happen first (`Global_Parameters`, `Global_Scenarios`, `Power_Parameters`), because subsequent file selection depends on `pEnable*` flags.
- All remaining files are read in parallel via `ThreadPoolExecutor` — order of assignment is non-deterministic, so no file read may depend on another parallel read.
- `dPower_WeightsRP` is **computed** from `dPower_Hindex` (counting occurrences per `rp`); if a `Power_WeightsRP.xlsx` file also exists, it is read and compared — a mismatch triggers a warning but uses the **file** value, not the computed one.
- `merge_single_node_buses()` preserves the `z` (zone) column as a sorted unique union string of all merged zones (e.g. `"R1_R2"`). This is documented in root `CLAUDE.md` as well.
- `merge_generators()` collapses all generators sharing the same `(tec, i)` into one representative generator with ID `"{i}_{tec}"`. It must be called after construction (scaling is not required). VRESProfiles and Inflows are merged **before** VRES so that the original per-generator `MaxProd` weights are available for the capacity-factor weighted average. Generators in VRESProfiles/Inflows that have no matching entry in `dPower_VRES` (left-join miss) are grouped under `(tec=NaN, i=NaN)` — filter to a single scenario first to avoid mixing scenarios in the groupby.
- `filter_zone(zone)` keeps only buses whose `z` column exactly matches the given zone name(s). Cascade order: BusInfo → Network (both endpoints must survive) → ThermalGen / VRES / Storage (by `i`) → Demand (by `i` index level) → VRESProfiles (by surviving VRES gen IDs) → Inflows (by union of surviving VRES + Storage gen IDs) → ImportExport (by `i` index level). `z` is an exact-match filter; merged-bus zone strings like `"R1_R2"` are not matched by `"R1"` alone.
- `shift_transition_matrix(positions, inplace=True, seed=42)` cyclically shifts every row of `rpTransitionMatrixAbsolute` right by `positions` (via `np.roll`, negative shifts left), normalizes, then rebuilds Hindex by Markov chain sampling from the shifted row distributions — anchoring the first period of each scenario to its original RP assignment. Recomputes `dPower_WeightsRP` and all three TM attributes from the new Hindex. The RNG seed defaults to 42.
- `perturb_transition_matrix(randomness, inplace=True, seed=42)` interpolates each row between its original distribution and a random draw: `new_prob = (1-randomness)*orig_prob + randomness*random_draw`, where `random_draw` is `n` uniform values normalized to sum to 1. `randomness=0.0` leaves the matrix unchanged; `randomness=1.0` replaces it fully. Rebuilds Hindex identically to `shift_transition_matrix` (Markov chain sampling, first period anchored, missing-RP correction loop). Uses a single RNG instance for both the random draws and the chain sampling.
- `CaseStudy.copy()` is a full `deepcopy` — safe to modify independently.
- `to_full_hourly_model()` processes each enabled scenario from `dGlobal_Scenarios` independently. It is a no-op (returns unchanged) if all scenarios are already hourly (no two p-values share the same 'k' within any scenario). The produced Hindex uses consecutive h0001…hN p-labels and k0001…kN k-labels per scenario, all mapped to rp01 with weight 1.
- Transition matrices (`rpTransitionMatrixAbsolute`, `rpTransitionMatrixRelativeTo`, `rpTransitionMatrixRelativeFrom`) are computed in the constructor and attached as attributes.

### ExcelReader

- Excel sheets whose name starts with `~` are silently skipped (used to disable scenarios in a multi-sheet file without deleting them).
- All Excel files have a version specifier in cell `C2` of each sheet. `check_LEGOExcel_version()` warns (or raises, if `fail_on_wrong_version=True`) on mismatch — wrong version can cause silent column misreads.
- The reader uses `calamine` engine (fast), not `openpyxl`.

### ExcelWriter

- All cell styles, column definitions, and table layouts are declared in `TableDefinitions.xml`, not in Python code. When adding a new output table, define its columns there first.
- `ExcelWriter.__init__()` parses the XML once and stores resolved objects (`self.columns`, `self.cell_styles`, etc.). Avoid re-instantiating per row.

### SQLiteWriter

- `model_to_sqlite()` automatically calls `add_objective_decomposition_to_sqlite()` and `add_dual_values_to_sqlite()` — these do not need to be called separately.
- `add_run_parameters_to_sqlite()` stores all run configuration in the `run_parameters` table. Evaluation scripts should read from this table (more reliable than filename parsing).
- Pyomo component types not handled by the writer emit a `printer.warning()` and are skipped silently — add new `case` branches if new Pyomo types need to be stored.

### Utilities

- `inflowsToCapacityFactors()` joins inflows onto `vresProfiles_df` by dividing by `MaxProd`; generators with missing or zero `MaxProd` are dropped with a warning.
- `capacityFactorsToInflows()` is the inverse; the `remove_Inflows_from_VRESProfiles_inplace` flag modifies the input DataFrame in place when set.
- `plot_transition_matrix()` emits **two** figures per call — a row-normalised ("from" perspective) and a column-normalised ("to" perspective) version — via the inner `_render()` helper. When `output` is given, `os.path.splitext` inserts a `-rowNorm` / `-colNorm` suffix before the extension (e.g. `MK-foo.png` → `MK-foo-rowNorm.png`, `MK-foo-colNorm.png`). Colour intensity and the per-cell percentage encode the respective normalisation; absolute counts and the row/col/grand totals are identical between the two. Cell text flips from black to white where the Blue fill is too dark to read on (perceived luminance `0.299·R + 0.587·G + 0.114·B < 0.5`). It passes `bbox=[0, 0, 1, 1]` to `ax.table()` so the table is forced to fill the entire Axes — without it, matplotlib sizes table rows from font metrics rather than the Axes height, leaving large dead space above/below when the figure is taller than the table's natural size. `fig_w`/`fig_h` are tuned per-cell-inch (not just a generic min-size heuristic) since they now directly determine the rendered cell aspect ratio.

### Printer

- `Printer` is a singleton — obtain the instance with `Printer.getInstance()`, never call the constructor directly.
- `set_logfile(path)` redirects all subsequent output to a file (appending). Set to `None` to stop logging.

### Caller

- `Caller.py` is a parallel job runner reading from a text file. It uses sentinel files (`.finished{n}`, `.error{n}`) for barrier synchronization across parallel workers.
- Lines containing only `---` act as barriers — workers wait until all prior jobs are complete before continuing past the barrier.
