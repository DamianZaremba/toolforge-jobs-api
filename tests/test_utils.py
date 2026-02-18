from copy import deepcopy
from typing import Any

import pytest

from tjf.core.utils import format_duration, parse_duration


@pytest.mark.parametrize(
    "seconds, formatted",
    [
        (0, "0s"),
        (1, "1s"),
        (20, "20s"),
        (60, "1m"),
        (61, "1m1s"),
        (120, "2m"),
        (121, "2m1s"),
        (3600, "1h"),
        (3601, "1h1s"),
        (3660, "1h1m"),
        (3661, "1h1m1s"),
        (86400, "1d"),
        (86460, "1d1m"),
        (90000, "1d1h"),
        (90060, "1d1h1m"),
        (90120, "1d1h2m"),
        (172800, "2d"),
    ],
)
def test_format_and_parse_duration(seconds: int, formatted: str):
    assert format_duration(seconds) == formatted
    assert parse_duration(formatted) == seconds


@pytest.mark.parametrize(
    "seconds, formatted",
    [
        # for durations longer than a day, seconds are no longer relevant
        (86401, "1d"),
        (90061, "1d1h1m"),
    ],
)
def test_format_duration_lossy(seconds: int, formatted: str):
    assert format_duration(seconds) == formatted


@pytest.mark.parametrize(
    "duration",
    [
        "invalid",
        "1",
        "2d3",
        "1h2",
        "a",
    ],
)
def test_parse_duration_errors(duration):
    with pytest.raises(ValueError):
        parse_duration(duration)


def patch_spec(spec: dict[str, Any], patch: dict[str, Any] | None) -> dict[str, Any]:
    spec = deepcopy(spec)
    if patch is None:
        return spec

    for key, value in patch.items():
        if key in spec and isinstance(spec[key], dict) and isinstance(value, dict):
            spec[key] = patch_spec(spec[key], value)
        elif key in spec and isinstance(spec[key], list) and value and isinstance(value, list):
            for index, (orig_elem, patch_elem) in enumerate(zip(spec[key], value, strict=False)):
                if isinstance(orig_elem, dict) and isinstance(patch_elem, dict):
                    spec[key][index] = patch_spec(orig_elem, patch_elem)
                else:
                    spec[key][index] = patch_elem
            # add the extra elems in the patch if there's more there
            if len(value) > len(spec[key]):
                spec[key].extend(value[len(spec[key]) :])
        else:
            spec[key] = value

    return spec


def cases(params_str, *params_defs):
    """Simple wrapper around parametrize to add test titles in a more readable way.

    Use like:
    >>> @cases(
    >>>     "param1,param2",
    >>>     ["Test something", ["param1value1", "param2value1"]],
    >>>     ["Test something else", ["param1value2", "param2value2"]],
    >>> )
    >>> def test_mytest(param1, param2):
    >>>     ...

    So it shows in pytest like:
    ```
    tests/test_this_file.py::test_mytest[Test something] PASSED
    tests/test_this_file.py::test_mytest[Test something else] PASSED
    ```
    """
    test_names = [name for name, _ in params_defs]
    test_params = [params for _, params in params_defs]
    print(f"Parametrizing with: {params_str}\n{test_params}\nids={test_names}")

    def wrapper(func):
        return pytest.mark.parametrize(params_str, test_params, ids=test_names)(func)

    return wrapper
