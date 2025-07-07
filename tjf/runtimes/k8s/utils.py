import json
from difflib import unified_diff
from logging import getLogger
from typing import Any
from uuid import uuid4

from .account import ToolAccount

K8S_OBJECT_TYPE = dict[str, Any]
LOGGER = getLogger(__name__)


def prune_spec(spec: K8S_OBJECT_TYPE, template: K8S_OBJECT_TYPE) -> K8S_OBJECT_TYPE:
    """
    Recursively prune 'spec' so that only keys present in 'template' remain.

    This function assumes:
      - If template is a dict, then for each key in template, if that key exists in spec,
        include it (recursively pruned).
      - If template is a list and spec is a list, then process each corresponding element.
      - Otherwise, return the spec value.
    """
    if isinstance(template, dict) and isinstance(spec, dict):
        pruned = {}
        for key in template:
            if key in spec:
                pruned[key] = prune_spec(spec[key], template[key])
        return pruned

    if isinstance(template, list) and isinstance(spec, list):
        # Here, we assume that the lists are in a corresponding order. You may need to
        # adjust if your lists are unordered or require merging by a specific key.
        return [prune_spec(spc, templ) for spc, templ in zip(spec, template)]

    # For other data types (or if the structure doesn't match), just return the spec value.
    return spec


def calculate_diff(
    tool_account: ToolAccount,
    job_name: str,
    kind: str,
    current_k8s_obj: K8S_OBJECT_TYPE,
    incoming_k8s_obj: K8S_OBJECT_TYPE,
) -> str:
    """
    Calculate diff between two k8s object dict
    """

    ###################################################################
    # At first glance it might appear you can directly sort and compare new_spec and current_spec (so maybe this block is not neccessary),
    # but doing that leaves us at the mercy of any future change made to the function that generates these specs.
    # what we are doing here is to use k8s to standardize some values like cpu and memory limits and requests,
    # so we don't have to care whatever unit these values are in our code generated specs, k8s will always standardize it for easy comparision.
    current_k8s_obj_dry_run = {}
    incoming_k8s_obj_dry_run = {}
    if current_k8s_obj:
        current_k8s_obj["metadata"][
            "name"
        ] = f"{job_name}-{str(uuid4())}"  # use random name for dry-run object to avoid conflicts
        current_k8s_obj_dry_run = tool_account.k8s_cli.create_object(
            kind=kind,
            spec=current_k8s_obj,
            dry_run=True,
        )
        current_k8s_obj["metadata"]["name"] = job_name
        current_k8s_obj_dry_run["metadata"]["name"] = job_name

    if incoming_k8s_obj:
        incoming_k8s_obj["metadata"]["name"] = f"{job_name}-{str(uuid4())}"
        incoming_k8s_obj_dry_run = tool_account.k8s_cli.create_object(
            kind=kind,
            spec=incoming_k8s_obj,
            dry_run=True,
        )
        incoming_k8s_obj["metadata"]["name"] = job_name
        incoming_k8s_obj_dry_run["metadata"]["name"] = job_name

    current_k8s_obj_dry_run = prune_spec(spec=current_k8s_obj_dry_run, template=current_k8s_obj)
    incoming_k8s_obj_dry_run = prune_spec(spec=incoming_k8s_obj_dry_run, template=incoming_k8s_obj)

    current_k8s_obj_dry_run_str = json.dumps(current_k8s_obj_dry_run, sort_keys=True, indent=4)
    incoming_k8s_obj_dry_run_str = json.dumps(incoming_k8s_obj_dry_run, sort_keys=True, indent=4)
    LOGGER.debug("new k8s_obj: %s", current_k8s_obj_dry_run_str)
    LOGGER.debug("current k8s_obj: %s", incoming_k8s_obj_dry_run_str)
    ###################################################################

    diff = unified_diff(
        current_k8s_obj_dry_run_str.splitlines(keepends=True),
        incoming_k8s_obj_dry_run_str.splitlines(keepends=True),
        lineterm="",
    )
    return "".join([line for line in list(diff) if line is not None])
