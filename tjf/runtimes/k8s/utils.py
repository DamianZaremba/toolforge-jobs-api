from typing import TypeVar

T = TypeVar("T")
U = TypeVar("U")


def dict_get_object[T, U](dict_in: dict[T, U], kind: T) -> U | None:
    for key, value in dict_in.items():
        if key == kind:
            return value

    return None


def remove_prefixes(text: str, prefixes: set[str]) -> str:
    for prefix in prefixes:
        text = text.removeprefix(prefix)
    return text


def format_duration(seconds: int) -> str:
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)

    value = ""
    if d > 0:
        value += f"{d}d"
    if h > 0:
        value += f"{h}h"
    if m > 0:
        value += f"{m}m"
    if (s > 0 and d == 0) or value == "":
        value += f"{s}s"
    return value
