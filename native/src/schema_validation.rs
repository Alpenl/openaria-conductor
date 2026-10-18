//! Optional compiled structural validation. Python retains semantic checks and fallback.
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[pyclass]
pub struct NativeSchemaValidator {
    validator: jsonschema::Validator,
}

#[pymethods]
impl NativeSchemaValidator {
    #[new]
    fn new(py: Python<'_>, schema_json: &str) -> PyResult<Self> {
        py.allow_threads(|| {
            let schema = serde_json::from_str(schema_json)
                .map_err(|error| PyValueError::new_err(error.to_string()))?;
            let validator = jsonschema::draft202012::options()
                .should_validate_formats(true)
                .build(&schema)
                .map_err(|error| PyValueError::new_err(error.to_string()))?;
            Ok(Self { validator })
        })
    }

    fn is_valid(&self, py: Python<'_>, payload: &str) -> PyResult<bool> {
        py.allow_threads(|| {
            let value = serde_json::from_str(payload)
                .map_err(|error| PyValueError::new_err(error.to_string()))?;
            Ok(self.validator.is_valid(&value))
        })
    }
}
