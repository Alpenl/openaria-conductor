//! Optional compiled structural validation. Python retains semantic checks and fallback.

pub(crate) struct SchemaValidator {
    validator: jsonschema::Validator,
}

impl SchemaValidator {
    pub(crate) fn new(schema_json: &str) -> Result<Self, String> {
        let schema = serde_json::from_str(schema_json).map_err(|error| error.to_string())?;
        let validator = jsonschema::draft202012::options()
            .should_validate_formats(true)
            .build(&schema)
            .map_err(|error| error.to_string())?;
        Ok(Self { validator })
    }

    pub(crate) fn is_valid(&self, payload: &str) -> Result<bool, String> {
        let value = serde_json::from_str(payload).map_err(|error| error.to_string())?;
        Ok(self.validator.is_valid(&value))
    }
}
