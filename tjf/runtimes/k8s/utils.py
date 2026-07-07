from typing import TypeVar

T = TypeVar("T")
U = TypeVar("U")


def dict_get_object(dict_in: dict[T, U], kind: T) -> U | None:
    for o in dict_in:
        if o == kind:
            return dict_in[o]

    return None


def remove_prefixes(text: str, prefixes: set[str]) -> str:
    for prefix in prefixes:
        if text.startswith(prefix):
            text = text[len(prefix) :]
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
