import json
import unittest
from unittest.mock import Mock, patch

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError

from rp_ylx.api.schema_validation import validate_manifest_schema
from rp_ylx.native import native_schema_validator_or_none


class SchemaValidationTests(unittest.TestCase):
    def test_native_success_skips_python_structural_work(self):
        validator = Mock(schema={"type": "object"})
        compiled = Mock()
        compiled.is_valid.return_value = True
        with patch(
            "rp_ylx.api.schema_validation.native_schema_validator_or_none", return_value=compiled
        ):
            validate_manifest_schema(validator, {"samples": [1, 2, 3]})
        validator.validate.assert_not_called()
        self.assertEqual(json.loads(compiled.is_valid.call_args.args[0]), {"samples": [1, 2, 3]})

    def test_rejection_absence_and_failure_preserve_original_error(self):
        validator = Draft202012Validator({"type": "integer", "minimum": 1})
        for result in (
            None,
            Mock(is_valid=Mock(return_value=False)),
            Mock(is_valid=Mock(side_effect=ValueError("bad native input"))),
            Mock(is_valid=Mock(return_value=1)),
        ):
            with (
                self.subTest(result=result),
                patch(
                    "rp_ylx.api.schema_validation.native_schema_validator_or_none",
                    return_value=result,
                ),
            ):
                with self.assertRaises(ValidationError) as error:
                    validate_manifest_schema(validator, 0)
                self.assertEqual(error.exception.validator, "minimum")

    def test_native_stricter_rejection_keeps_python_compatibility(self):
        # Python's '$' can match before a final newline; native engines differ.
        validator = Draft202012Validator({"type": "string", "pattern": "^abc$"})
        compiled = Mock(is_valid=Mock(return_value=False))
        with patch(
            "rp_ylx.api.schema_validation.native_schema_validator_or_none", return_value=compiled
        ):
            validate_manifest_schema(validator, "abc\n")

    def test_non_json_types_and_large_numbers_are_never_coerced(self):
        class String(str):
            pass

        values = [
            (1, 2),
            {1: "value"},
            String("value"),
            {"x": 2**53},
            {"x": float("nan")},
            {"x": float("inf")},
            {"x": 1e30},
        ]
        for value in values:
            validator = Mock(schema={})
            with (
                self.subTest(value=value),
                patch("rp_ylx.api.schema_validation.native_schema_validator_or_none") as compiler,
            ):
                validate_manifest_schema(validator, value)
                compiler.assert_not_called()
                validator.validate.assert_called_once_with(value)

    def test_native_module_absence_falls_back_and_cache_is_bounded(self):
        native_schema_validator_or_none.cache_clear()
        try:
            with patch("rp_ylx.native._require_native_feature", side_effect=ImportError):
                for n in range(6):
                    self.assertIsNone(native_schema_validator_or_none(json.dumps({"const": n})))
                self.assertEqual(native_schema_validator_or_none.cache_info().currsize, 4)
        finally:
            native_schema_validator_or_none.cache_clear()

    def test_compiled_draft202012_constraints_and_formats(self):
        schema = {
            "$defs": {"positive": {"type": "integer", "minimum": 1}},
            "type": "object",
            "required": ["kind", "values", "id"],
            "additionalProperties": False,
            "properties": {
                "kind": {"enum": ["one", "many"]},
                "values": {
                    "type": "array",
                    "items": {"$ref": "#/$defs/positive"},
                    "uniqueItems": True,
                },
                "id": {"type": "string", "format": "uuid"},
            },
            "if": {"properties": {"kind": {"const": "one"}}},
            "then": {"properties": {"values": {"maxItems": 1}}},
            "else": {"properties": {"values": {"minItems": 2}}},
        }
        compiler = native_schema_validator_or_none(json.dumps(schema))
        if compiler is None:
            self.skipTest("native schema acceleration unavailable")
        validator = Draft202012Validator(schema, format_checker=FormatChecker())
        for kind in ("one", "many", "unknown", True):
            for values in ([], [1], [1, 2], [1, 1.0], [True], [-1], [0.5]):
                for identity in ("550e8400-e29b-41d4-a716-446655440000", "invalid"):
                    payload = {"kind": kind, "values": values, "id": identity}
                    with self.subTest(payload=payload):
                        self.assertEqual(
                            compiler.is_valid(json.dumps(payload)), validator.is_valid(payload)
                        )
        with self.assertRaises(ValueError):
            compiler.is_valid("{invalid")
