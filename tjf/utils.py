# Copyright (C) 2021 Arturo Borrero Gonzalez <aborrero@wikimedia.org>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Set, TypeVar

USER_AGENT = "jobs-api"

KUBERNETES_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


T = TypeVar("T")
U = TypeVar("U")

VALID_KUBE_QUANT_SUFFIXES = {
    "Ei": 1024**6,
    "Pi": 1024**5,
    "Ti": 1024**4,
    "Gi": 1024**3,
    "Mi": 1024**2,
    "Ki": 1024**1,
    "E": 1000**6,
    "P": 1000**5,
    "T": 1000**4,
    "G": 1000**3,
    "M": 1000**2,
    "k": 1000**1,
    "m": 1000**-1,
    "u": 1000**-2,
    "n": 1000**-3,
}


def dict_get_object(dict_in: dict[T, U], kind: T) -> U | None:
    for o in dict_in:
        if o == kind:
            return dict_in[o]

    return None


# copied & adapted from https://github.com/kubernetes-client/python/pull/2216/files#diff-7070f0b8e347e5b2bd6a5fcb5ff69ed300853c94d610e984e09f831d028d644b
def parse_quantity(quantity: int | float | Decimal | str) -> Decimal:
    """
    Parse kubernetes canonical form quantity like 200Mi to a decimal number.
    Supported SI suffixes:
    base1024: Ki | Mi | Gi | Ti | Pi | Ei
    base1000: n | u | m | "" | k | M | G | T | P | E

    See https://github.com/kubernetes/apimachinery/blob/master/pkg/api/resource/quantity.go

    Input:
    quantity: string. kubernetes canonical form quantity

    Returns:
    Decimal

    Raises:
    ValueError on invalid or unknown input
    """
    if isinstance(quantity, (int, float, Decimal)):
        return Decimal(quantity)

    quantity = str(quantity)
    number: str | Decimal = quantity
    suffix = None

    if len(quantity) >= 1 and quantity[-1:] in VALID_KUBE_QUANT_SUFFIXES:
        number = quantity[:-1]
        suffix = quantity[-1:]
    elif len(quantity) >= 2 and quantity[-2:] in VALID_KUBE_QUANT_SUFFIXES:
        number = quantity[:-2]
        suffix = quantity[-2:]

    try:
        number = Decimal(number)
    except InvalidOperation:
        raise ValueError("Invalid number format: {}".format(number))

    if suffix is None:
        return number

    multiplier = Decimal(VALID_KUBE_QUANT_SUFFIXES[suffix])
    return number * multiplier


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


def remove_prefixes(text: str, prefixes: Set[str]) -> str:
    for prefix in prefixes:
        if text.startswith(prefix):
            text = text[len(prefix) :]
    return text
