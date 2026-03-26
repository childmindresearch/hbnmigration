"""Tests for secret management utilities."""

from unittest.mock import patch

import pytest

from hbnmigration.utility_functions.secrets import ImportWithFallback

from .conftest import (
    assert_has_name_attribute,
    assert_is_callable_result,
    assert_is_fallback_value,
    assert_not_fallback_value,
    FallbackDataDict,
)

# ============================================================================
# ImportWithFallback.module Tests
# ============================================================================


class TestImportWithFallbackModule:
    """Tests for ImportWithFallback.module method."""

    def test_successful_import_absolute(self):
        """Test successful import with absolute module path."""
        result = ImportWithFallback.module(
            module="os.path",
            name="join",
            fallback_module="pathlib",
            fallback_name="Path",
        )

        assert_has_name_attribute(result, "join")
        assert result("/tmp", "test") == "/tmp/test"

    def test_fallback_when_module_not_found(self):
        """Test fallback when primary module doesn't exist."""
        result = ImportWithFallback.module(
            module="black_orchid.phantom_stranger",
            name="SusanLinden",
            fallback_module="os.path",
            fallback_name="exists",
        )

        assert_has_name_attribute(result, "exists")

    def test_fallback_when_attribute_not_found(self):
        """Test fallback when attribute doesn't exist in module."""
        result = ImportWithFallback.module(
            module="os",
            name="nonexistent_function",
            fallback_module="os.path",
            fallback_name="dirname",
        )

        assert_has_name_attribute(result, "dirname")

    def test_fallback_same_name(self):
        """Test fallback with same attribute name (fallback_name=None)."""
        result = ImportWithFallback.module(
            module="poison_ivy.pamela_isley",
            name="join",
            fallback_module="os.path",
            fallback_name=None,
        )

        assert_has_name_attribute(result, "join")

    @patch("importlib.import_module")
    def test_caller_package_passed_for_relative_import(
        self, mock_import, mock_importable_module
    ):
        """Test that caller's package name is used for relative imports."""
        mock_importable_module.TestAttr = "test_value"
        mock_import.return_value = mock_importable_module

        result = ImportWithFallback.module(
            module=".relative_module",
            name="TestAttr",
            fallback_module="os",
            fallback_name="name",
        )
        assert result

        assert mock_import.called
        call_args = mock_import.call_args_list[0]
        assert call_args[0][0] == ".relative_module"
        assert call_args[1]["package"] is not None
        assert "test_secrets_utilities" in call_args[1]["package"]

    def test_both_imports_fail_raises_error(self):
        """Test that error is raised when both primary and fallback fail."""
        with pytest.raises((ImportError, ModuleNotFoundError, AttributeError)):
            ImportWithFallback.module(
                module="the_gardener.arcane_botanical_knowledge",
                name="FloronicConsciousness",
                fallback_module="poison_ivy.green_realm",
                fallback_name="PlantElemental",
            )


# ============================================================================
# ImportWithFallback.literal Tests
# ============================================================================


class TestImportWithFallbackLiteral:
    """Tests for ImportWithFallback.literal method."""

    def test_successful_import_returns_imported_value(self):
        """Test successful import returns the imported attribute, not fallback."""
        fallback_value = "FALLBACK"

        result = ImportWithFallback.literal(
            module="os.path", name="sep", fallback=fallback_value
        )

        assert_not_fallback_value(result, fallback_value)
        assert result in ("/", "\\")

    def test_fallback_on_module_not_found(self, green_realm_config):
        """Test literal fallback when module doesn't exist."""
        result = ImportWithFallback.literal(
            module="swamp_thing.parliament_of_trees",
            name="collective_wisdom",
            fallback=green_realm_config,
        )

        assert_is_fallback_value(result, green_realm_config)
        assert result["api_key"] == "TEST_KEY"

    def test_fallback_on_attribute_not_found(
        self, swamp_thing_fallback_data: FallbackDataDict
    ) -> None:
        """Test literal fallback when attribute doesn't exist."""
        family_list = swamp_thing_fallback_data["members"]

        result = ImportWithFallback.literal(
            module="os", name="swamp_thing_family", fallback=family_list
        )

        assert_is_fallback_value(result, family_list)
        assert len(result) == len(family_list)

    def test_fallback_with_none_value(self):
        """Test fallback with None as the fallback value."""
        result = ImportWithFallback.literal(
            module="black_orchid.suzi_linden", name="HybridForm", fallback=None
        )

        assert result is None

    def test_fallback_with_callable(self):
        """Test fallback with a callable function."""

        def green_lantern_oath():
            return "In brightest day, in blackest night…"

        result = ImportWithFallback.literal(
            module="dc_comics.green_lantern", name="oath", fallback=green_lantern_oath
        )

        assert_is_callable_result(result)
        assert result() == "In brightest day, in blackest night…"

    def test_fallback_with_mock_object(self, mock_parliament_object):
        """Test fallback with a Mock object (useful for testing)."""
        result = ImportWithFallback.literal(
            module="swamp_thing.meta_consciousness",
            name="Parliament",
            fallback=mock_parliament_object,
        )

        assert_is_fallback_value(result, mock_parliament_object)
        assert len(result.members) == len(mock_parliament_object.members)

    def test_relative_import_with_literal_fallback(self, green_realm_config):
        """Test relative import with literal fallback."""
        result = ImportWithFallback.literal(
            module=".nonexistent_config", name="SETTINGS", fallback=green_realm_config
        )

        assert_is_fallback_value(result, green_realm_config)
        assert result["api_key"] == "TEST_KEY"

    @patch("importlib.import_module")
    def test_caller_package_used_for_relative_import_literal(
        self, mock_import, mock_importable_module
    ):
        """Test that caller's package is correctly identified for literal fallback."""
        mock_importable_module.CONFIG = {"setting": "value"}
        mock_import.return_value = mock_importable_module

        result = ImportWithFallback.literal(
            module=".settings", name="CONFIG", fallback={"fallback": "config"}
        )
        assert result

        assert mock_import.called
        call_args = mock_import.call_args_list[0]
        assert call_args[0][0] == ".settings"
        assert call_args[1]["package"] is not None


# ============================================================================
# Edge Cases and Integration Tests
# ============================================================================


class TestImportWithFallbackEdgeCases:
    """Test edge cases and integration scenarios."""

    def test_nested_module_import(self):
        """Test importing from deeply nested module structure."""
        result = ImportWithFallback.module(
            module="xml.etree.ElementTree",
            name="Element",
            fallback_module="os",
            fallback_name="getcwd",
        )

        assert_has_name_attribute(result, "Element")

    def test_import_builtin_type(self):
        """Test importing built-in types."""
        result = ImportWithFallback.literal(module="builtins", name="dict", fallback={})

        assert isinstance(result, type(dict))

    def test_multiple_sequential_imports(self):
        """Test multiple imports in sequence maintain correct caller context."""
        result1 = ImportWithFallback.literal(
            module="os.path", name="sep", fallback="FALLBACK1"
        )

        result2 = ImportWithFallback.literal(
            module="fake_module", name="fake_attr", fallback="FALLBACK2"
        )

        result3 = ImportWithFallback.module(
            module="os.path",
            name="join",
            fallback_module="os.path",
            fallback_name="dirname",
        )

        assert result1 in ("/", "\\")
        assert_is_fallback_value(result2, "FALLBACK2")
        assert_has_name_attribute(result3, "join")

    def test_empty_string_fallback(self):
        """Test with empty string as fallback value."""
        result = ImportWithFallback.literal(
            module="floronic_man.woodrue", name="humanity", fallback=""
        )

        assert result == ""

    def test_complex_fallback_data_structure(self, swamp_thing_fallback_data):
        """Test with complex nested data structure as fallback."""
        result = ImportWithFallback.literal(
            module="the_green.meta_structure",
            name="COLLECTIVE",
            fallback=swamp_thing_fallback_data,
        )

        assert_is_fallback_value(result, swamp_thing_fallback_data)
        assert len(result["parliament"]["trees"]) == len(
            swamp_thing_fallback_data["parliament"]["trees"]
        )
        assert "Swamp Thing" in result["avatars"]
