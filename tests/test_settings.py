"""Runtime settings come from config, per environment, overridable per pipeline."""

import pytest
from conftest import RETRY

from dlt_framework.core.config import Registry, resolve
from dlt_framework.core.errors import ConfigurationError, DefinitionError
from dlt_framework.core.settings import GuardSettings, RunSettings


def test_a_pipeline_overrides_settings_field_by_field(monkeypatch) -> None:
    monkeypatch.setenv("PIPELINES__SHOP__RETRY__ATTEMPTS", "9")
    shop, other = RunSettings.of("shop").retry, RunSettings.of("other").retry
    assert (shop.attempts, shop.give_up_after_s) == (9, RETRY["give_up_after_s"])
    assert other.attempts == RETRY["attempts"]


def test_a_missing_setting_fails_loudly() -> None:
    with pytest.raises(ConfigurationError):
        resolve(GuardSettings(), "nowhere")


def test_a_registry_key_has_one_implementation() -> None:
    registry: Registry[object] = Registry("thing")
    registry.register("a")(object)
    with pytest.raises(DefinitionError):
        registry.register("a")(int)
