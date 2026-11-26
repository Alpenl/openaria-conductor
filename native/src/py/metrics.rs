use crate::hardware::metrics;
use crate::py::convert::metrics_snapshot_dict;
use crate::py::metrics_error;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

#[pyclass]
pub(crate) struct NativePerformanceMetrics {
    pub(crate) metrics: Arc<metrics::Metrics>,
}

#[pymethods]
impl NativePerformanceMetrics {
    #[new]
    fn new() -> Self {
        Self {
            metrics: Arc::new(metrics::Metrics::new()),
        }
    }

    fn record_stage(&self, py: Python<'_>, name: &str, elapsed_ns: u64) -> PyResult<()> {
        py.allow_threads(|| self.metrics.record_stage(name, elapsed_ns))
            .map_err(metrics_error)
    }

    #[pyo3(signature = (name, size, count=1))]
    fn record_copy(&self, py: Python<'_>, name: &str, size: u64, count: u64) -> PyResult<()> {
        py.allow_threads(|| self.metrics.record_copy(name, size, count))
            .map_err(metrics_error)
    }

    fn change_payload(
        &self,
        py: Python<'_>,
        name: &str,
        count_delta: i64,
        bytes_delta: i64,
    ) -> PyResult<()> {
        py.allow_threads(|| self.metrics.change_payload(name, count_delta, bytes_delta))
            .map_err(metrics_error)
    }

    #[pyo3(signature = (depth, capacity, rejected=0, peak_depth=None))]
    fn observe_queue(
        &self,
        py: Python<'_>,
        depth: u64,
        capacity: u64,
        rejected: u64,
        peak_depth: Option<u64>,
    ) -> PyResult<()> {
        py.allow_threads(|| {
            self.metrics
                .observe_queue(depth, capacity, rejected, peak_depth)
        })
        .map_err(metrics_error)
    }

    #[pyo3(signature = (kind, count=1))]
    fn record_loss(&self, py: Python<'_>, kind: &str, count: u64) -> PyResult<()> {
        py.allow_threads(|| self.metrics.record_loss(kind, count))
            .map_err(metrics_error)
    }

    fn snapshot(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let snapshot = py
            .allow_threads(|| self.metrics.snapshot())
            .map_err(metrics_error)?;
        metrics_snapshot_dict(py, &snapshot)
    }
}
