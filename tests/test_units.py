import pytest

from src.climate import convert_precipitation_values, convert_temperature_values


def test_convert_temperature_from_kelvin_to_celsius() -> None:
    converted = convert_temperature_values([273.15, 300.15], "K")

    assert converted == pytest.approx([0.0, 27.0])


def test_convert_temperature_keeps_celsius_values() -> None:
    converted = convert_temperature_values([20.0, 35.5], "degree_Celsius")

    assert converted == pytest.approx([20.0, 35.5])


def test_convert_precipitation_from_m_to_mm() -> None:
    converted = convert_precipitation_values([0.001, 0.025], "m")

    assert converted == pytest.approx([1.0, 25.0])


def test_convert_precipitation_rate_to_mm_using_time_step() -> None:
    converted = convert_precipitation_values([0.001], "kg m-2 s-1", time_step_seconds=3600)

    assert converted == pytest.approx([3.6])
