//! Compiled JSON Schema Draft 2020-12 structural validator.

pub struct SchemaValidator {
    validator: jsonschema::Validator,
}

impl SchemaValidator {
    pub fn new(schema_json: &str) -> Result<Self, String> {
        let schema = serde_json::from_str(schema_json).map_err(|error| error.to_string())?;
        let validator = jsonschema::draft202012::options()
            .should_validate_formats(true)
            .build(&schema)
            .map_err(|error| error.to_string())?;
        Ok(Self { validator })
    }

    pub fn is_valid(&self, payload: &str) -> Result<bool, String> {
        let value = serde_json::from_str(payload).map_err(|error| error.to_string())?;
        Ok(self.validator.is_valid(&value))
    }
}
