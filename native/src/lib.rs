mod audio;
mod hardware;
mod py;
mod session;
mod sys;

use pyo3::prelude::*;

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    py::register_module(module)
}
