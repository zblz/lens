import os
import itertools
import json
from pathlib import Path

import lens
from lens import metrics
import lens.dask_graph
import pandas as pd

ROOT = Path(os.path.abspath(os.path.dirname(__file__)))


def _join_dask_results_nodelay(results):
    report = {"_run_time": 0.0, "_columns": []}

    for result in results:
        if result is not None:
            report["_run_time"] += result["_run_time"]
            report["_columns"] += result["_columns"]
            columns = result.keys()
            report = lens.dask_graph._nested_merge(
                report,
                {
                    column: result[column]
                    for column in columns
                    if column not in ["_columns", "_run_time"]
                },
            )

    report["_columns"] = sorted(list(set(report["_columns"])))

    return report


def _compute_json_size(obj):
    if isinstance(obj, lens.Summary):
        return len(obj.to_json().encode("utf-8"))
    else:
        return len(json.dumps(obj, separators=(",", ":")).encode("utf-8"))


class TimeSummarise:
    """
    Time benchmark for lens.summarise.
    """

    datasets = ["room_occupancy.csv", "test_data.csv"]
    # The timeout in seconds assigned to each individual benchmark test (and
    # setup routine)
    timeout = 1800

    # NB: asv 0.2 appears to exhibit bug when timing routines in combination
    # with the multiprocessing scheduler using time_* functions. We use track_*
    # functions as a workaround, returning the execution time as reported by
    # lens.
    sched = "multiprocessing"
    num_workers = 4

    def setup(self):
        """
        Read the datasets into self.dataframes. Invoked before
        running the benchmark, thus allowing us to exclude any prerequisite
        computation from individual tests.
        """

        self.dataframes = {}
        for D in self.datasets:
            df_path = ROOT / "../tests/data" / D
            self.dataframes[D] = pd.read_csv(df_path)

        def summarise(df, scheduler=self.sched, num_workers=self.num_workers):
            return lens.summarise(
                df, scheduler=self.sched, num_workers=self.num_workers
            )

        self.summarise = summarise

    def time__summarise_room_occupancy_mp(self):
        self.summarise(
            self.dataframes["room_occupancy.csv"], scheduler="multiprocessing"
        )

    def time__summarise_room_occupancy_sync(self):
        self.summarise(self.dataframes["room_occupancy.csv"], scheduler="sync")

    def track__jsonsize_summarise_room_occupancy(self):
        return _compute_json_size(
            self.summarise(self.dataframes["room_occupancy.csv"])
        )

    track__jsonsize_summarise_room_occupancy.unit = "bytes"  # type: ignore

    def time__summarise_test_data_mp(self):
        self.summarise(
            self.dataframes["test_data.csv"], scheduler="multiprocessing"
        )

    def time__summarise_test_data_sync(self):
        self.summarise(self.dataframes["test_data.csv"], scheduler="sync")

    def track__jsonsize__summarise__test_data(self):
        return _compute_json_size(
            self.summarise(self.dataframes["test_data.csv"])
        )

    track__jsonsize__summarise__test_data.unit = "bytes"  # type: ignore


class TimeMetrics:
    """
    An execution time benchmark for lens, implemented for various datasets
    stored on Amazon S3. The benchmark evaluates lens.summarise() using both
    multiprocessing and synchronous scheduling, as well as individual metrics
    computed by lens.summarise().
    """

    datasets = ["room_occupancy.csv", "test_data.csv"]

    # The timeout in seconds assigned to each individual benchmark test (and
    # setup routine)
    timeout = 5400

    # Parameters for tasks scheduled using Dask
    dask_params = dict()
    dask_params["sync"] = {"sched": "sync"}

    def setup(self):
        """
        Read the datasets into self.dataframes. Invoked before
        running the benchmark, thus allowing us to exclude any prerequisite
        computation from individual tests.
        """

        self.dataframes = {}
        self.summarise_tasks = {}

        for D in self.datasets:
            df_path = os.path.join(ROOT, "data", D)
            df = pd.read_csv(df_path)
            self.dataframes[D] = df
            self.summarise_tasks[D] = SummariseTasks(df)

    def time__summarise__room_occupancy__sync(self):
        lens.summarise(
            self.dataframes["room_occupancy.csv"],
            scheduler=self.dask_params["sync"]["sched"],
        )

    def peakmem__summarise__room_occupancy__sync(self):
        lens.summarise(
            self.dataframes["room_occupancy.csv"],
            scheduler=self.dask_params["sync"]["sched"],
        )

    def time__row_count__room_occupancy(self):
        metrics.row_count(self.dataframes["room_occupancy.csv"])

    def time__column_properties__room_occupancy(self):
        for k in self.summarise_tasks["room_occupancy.csv"].columns:
            metrics.column_properties(
                self.summarise_tasks["room_occupancy.csv"].cols[k]
            )

    def time__frequencies__room_occupancy(self):
        for k in self.summarise_tasks["room_occupancy.csv"].columns:
            metrics.frequencies(
                self.summarise_tasks["room_occupancy.csv"].cols[k],
                self.summarise_tasks["room_occupancy.csv"].cprops[k],
            )

    def time__column_summary__room_occupancy(self):
        for k in self.summarise_tasks["room_occupancy.csv"].columns:
            metrics.column_summary(
                self.summarise_tasks["room_occupancy.csv"].cols[k],
                self.summarise_tasks["room_occupancy.csv"].cprops[k],
            )

    def time__outliers__room_occupancy(self):
        for k in self.summarise_tasks["room_occupancy.csv"].columns:
            metrics.outliers(
                self.summarise_tasks["room_occupancy.csv"].cols[k],
                self.summarise_tasks["room_occupancy.csv"].csumms[k],
            )

    def time__correlation__room_occupancy(self):
        metrics.correlation(
            self.dataframes["room_occupancy.csv"],
            self.summarise_tasks["room_occupancy.csv"].joined_cprops,
        )

    def time__pairdensity__room_occupancy(self):
        for col1, col2 in itertools.combinations(
            self.summarise_tasks["room_occupancy.csv"].columns, 2
        ):
            metrics.pairdensity(
                self.summarise_tasks["room_occupancy.csv"].pdens_df[
                    (col1, col2)
                ],
                self.summarise_tasks["room_occupancy.csv"].pdens_cp[
                    (col1, col2)
                ],
                self.summarise_tasks["room_occupancy.csv"].pdens_cs[
                    (col1, col2)
                ],
                self.summarise_tasks["room_occupancy.csv"].pdens_fr[
                    (col1, col2)
                ],
            )

    def time__summarise__test_data__sync(self):
        lens.summarise(
            self.dataframes["test_data.csv"],
            scheduler=self.dask_params["sync"]["sched"],
        )

    def peakmem__summarise__test_data__sync(self):
        lens.summarise(
            self.dataframes["test_data.csv"],
            scheduler=self.dask_params["sync"]["sched"],
        )

    def time__row_count__test_data(self):
        metrics.row_count(self.dataframes["test_data.csv"])

    def time__column_properties__test_data(self):
        for k in self.summarise_tasks["test_data.csv"].columns:
            metrics.column_properties(
                self.summarise_tasks["test_data.csv"].cols[k]
            )

    def time__frequencies__test_data(self):
        for k in self.summarise_tasks["test_data.csv"].columns:
            metrics.frequencies(
                self.summarise_tasks["test_data.csv"].cols[k],
                self.summarise_tasks["test_data.csv"].cprops[k],
            )

    def time__column_summary__test_data(self):
        for k in self.summarise_tasks["test_data.csv"].columns:
            metrics.column_summary(
                self.summarise_tasks["test_data.csv"].cols[k],
                self.summarise_tasks["test_data.csv"].cprops[k],
            )

    def time__outliers__test_data(self):
        for k in self.summarise_tasks["test_data.csv"].columns:
            metrics.outliers(
                self.summarise_tasks["test_data.csv"].cols[k],
                self.summarise_tasks["test_data.csv"].csumms[k],
            )

    def time__correlation__test_data(self):
        metrics.correlation(
            self.dataframes["test_data.csv"],
            self.summarise_tasks["test_data.csv"].joined_cprops,
        )

    def time__pairdensity__test_data(self):
        for col1, col2 in itertools.combinations(
            self.summarise_tasks["test_data.csv"].columns, 2
        ):
            metrics.pairdensity(
                self.summarise_tasks["test_data.csv"].pdens_df[(col1, col2)],
                self.summarise_tasks["test_data.csv"].pdens_cp[(col1, col2)],
                self.summarise_tasks["test_data.csv"].pdens_cs[(col1, col2)],
                self.summarise_tasks["test_data.csv"].pdens_fr[(col1, col2)],
            )


class SummariseTasks:
    """
    To benchmark individual metrics computed by lens.summarise(), we
    pre-compute all data normally obtained using dask and store them as
    instance variables. In this way, a test for an individual metric may refer
    to any prerequisite data without the execution time for obtaining the
    prerequisite data counting towards the execution time of the test.
    """

    def __init__(self, dataframe):
        self.columns = dataframe.columns
        self.cols = {k: dataframe.get(k) for k in self.columns}
        self.row_c = metrics.row_count(dataframe)

        self.cprops = {
            k: metrics.column_properties(self.cols[k]) for k in self.columns
        }
        self.joined_cprops = _join_dask_results_nodelay(
            list(self.cprops.values())
        )

        self.freqs = {
            k: metrics.frequencies(self.cols[k], self.cprops[k])
            for k in self.columns
        }
        self.joined_freqs = _join_dask_results_nodelay(
            list(self.freqs.values())
        )

        self.csumms = {
            k: metrics.column_summary(self.cols[k], self.cprops[k])
            for k in self.columns
        }
        self.joined_csumms = _join_dask_results_nodelay(
            list(self.csumms.values())
        )

        self.out = {
            k: metrics.outliers(self.cols[k], self.csumms[k])
            for k in self.columns
        }
        self.joined_outliers = _join_dask_results_nodelay(
            list(self.out.values())
        )

        self.corr = metrics.correlation(dataframe, self.joined_cprops)

        self.pdens_df = dict()
        self.pdens_cp = dict()
        self.pdens_cs = dict()
        self.pdens_fr = dict()
        for col1, col2 in itertools.combinations(self.columns, 2):
            self.pdens_df[(col1, col2)] = pd.concat(
                [self.cols[col1], self.cols[col2]], axis=1
            )
            self.pdens_cp[(col1, col2)] = {
                k: self.cprops[k] for k in [col1, col2]
            }
            self.pdens_cs[(col1, col2)] = {
                k: self.csumms[k] for k in [col1, col2]
            }
            self.pdens_fr[(col1, col2)] = {
                k: self.freqs[k] for k in [col1, col2]
            }

        # NB: Pairwise density (metrics.pairdensity) is the final task in
        # lens.summarise(). We omit it here, as it is not a prerequisite for
        # any subsequent task.
