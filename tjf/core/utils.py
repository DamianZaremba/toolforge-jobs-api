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

from decimal import Decimal
from typing import Set, TypeVar

from toolforge_weld.kubernetes import VALID_KUBE_QUANT_SUFFIXES, parse_quantity

USER_AGENT = "jobs-api"

KUBERNETES_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


T = TypeVar("T")
U = TypeVar("U")


def dict_get_object(dict_in: dict[T, U], kind: T) -> U | None:
    for o in dict_in:
        if o == kind:
            return dict_in[o]

    return None


# copied & adapted from https://github.com/kubernetes-client/python/pull/2216/files#diff-7070f0b8e347e5b2bd6a5fcb5ff69ed300853c94d610e984e09f831d028d644b
def format_quantity(quantity_value: Decimal, suffix: str = "", quantize: str = "") -> str:
    """
    Takes a value and produces a string value in kubernetes' canonical quantity form,
    like "200Mi".Users can specify an additional parameter to quantize the output.

    Example -  Relatively increase pod memory limits:

    # retrieve my_pod
    current_memory: Decimal = toolforge_weld.kubernetes.parse_quantity(my_pod.spec.containers[0].resources.limits.memory)
    desired_memory = current_memory * 1.2
    desired_memory_str = format_quantity(desired_memory, suffix="Gi", quantize="1")
    # patch pod with desired_memory_str

    'quantize="1"' ensures that the result does not contain any fractional digits.

    Supported SI suffixes:
    base1024: Ki | Mi | Gi | Ti | Pi | Ei
    base1000: n | u | m | "" | k | M | G | T | P | E

    See https://github.com/kubernetes/apimachinery/blob/master/pkg/api/resource/quantity.go

    Input:
    quantity: Decimal.  Quantity as a number which is supposed to be
                        converted to a string with SI suffix.
    suffix: string.     The desired suffix/unit-of-measure of the output string
    quantize: string.  Can be used to round/quantize the value before the string
                        is returned. Defaults to None. e.g. "1", "0.00", "0.000"

    Returns:
    string. Canonical Kubernetes quantity string containing the SI suffix.

    Raises:
    ValueError if the SI suffix is not supported.
    """

    if suffix and suffix not in VALID_KUBE_QUANT_SUFFIXES:
        raise ValueError(f"{suffix} is not a valid kubernetes quantity unit")

    different_scale = Decimal(quantity_value)

    if suffix:
        different_scale = different_scale / Decimal(VALID_KUBE_QUANT_SUFFIXES[suffix])

    if quantize:
        different_scale = different_scale.quantize(Decimal(quantize))

    return str(float(different_scale)) + suffix


def parse_and_format_mem(mem: str) -> str:
    return format_quantity(quantity_value=parse_quantity(mem), suffix="Gi", quantize="0.000")


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
