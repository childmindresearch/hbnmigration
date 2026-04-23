"""Tests for hbnmigration.config module."""

import os
from unittest.mock import patch


class TestConfigRecoveryMode:
    """Tests for Config.RECOVERY_MODE property."""

    def test_recovery_mode_disabled_by_default(self):
        """Recovery mode should be disabled if env var not set."""
        with patch.dict(os.environ, {}, clear=True):
            # Force reimport to pick up cleared environment
            import importlib

            import hbnmigration.config as config_module

            importlib.reload(config_module)
            assert config_module.Config.RECOVERY_MODE is False

    def test_recovery_mode_enabled_with_1(self):
        """Recovery mode should be enabled with HBNMIGRATION_RECOVERY_MODE=1."""
        with patch.dict(os.environ, {"HBNMIGRATION_RECOVERY_MODE": "1"}):
            import importlib

            import hbnmigration.config as config_module

            importlib.reload(config_module)
            assert config_module.Config.RECOVERY_MODE is True

    def test_recovery_mode_enabled_with_yes(self):
        """Recovery mode should be enabled with HBNMIGRATION_RECOVERY_MODE=yes."""
        with patch.dict(os.environ, {"HBNMIGRATION_RECOVERY_MODE": "yes"}):
            import importlib

            import hbnmigration.config as config_module

            importlib.reload(config_module)
            assert config_module.Config.RECOVERY_MODE is True

    def test_recovery_mode_enabled_with_true(self):
        """Recovery mode should be enabled with HBNMIGRATION_RECOVERY_MODE=true."""
        with patch.dict(os.environ, {"HBNMIGRATION_RECOVERY_MODE": "true"}):
            import importlib

            import hbnmigration.config as config_module

            importlib.reload(config_module)
            assert config_module.Config.RECOVERY_MODE is True

    def test_recovery_mode_enabled_with_uppercase_yes(self):
        """Recovery mode should handle uppercase values."""
        with patch.dict(os.environ, {"HBNMIGRATION_RECOVERY_MODE": "YES"}):
            import importlib

            import hbnmigration.config as config_module

            importlib.reload(config_module)
            assert config_module.Config.RECOVERY_MODE is True

    def test_recovery_mode_disabled_with_0(self):
        """Recovery mode should be disabled with HBNMIGRATION_RECOVERY_MODE=0."""
        with patch.dict(os.environ, {"HBNMIGRATION_RECOVERY_MODE": "0"}):
            import importlib

            import hbnmigration.config as config_module

            importlib.reload(config_module)
            assert config_module.Config.RECOVERY_MODE is False

    def test_recovery_mode_disabled_with_random_string(self):
        """Recovery mode should be disabled with any other value."""
        with patch.dict(os.environ, {"HBNMIGRATION_RECOVERY_MODE": "maybe"}):
            import importlib

            import hbnmigration.config as config_module

            importlib.reload(config_module)
            assert config_module.Config.RECOVERY_MODE is False

    def test_recovery_mode_disabled_with_empty_string(self):
        """Recovery mode should be disabled with empty string."""
        with patch.dict(os.environ, {"HBNMIGRATION_RECOVERY_MODE": ""}):
            import importlib

            import hbnmigration.config as config_module

            importlib.reload(config_module)
            assert config_module.Config.RECOVERY_MODE is False
