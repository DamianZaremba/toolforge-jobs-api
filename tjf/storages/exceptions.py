from typing import Any

import kubernetes  # type: ignore
from fastapi import status

from ..core.error import TjfError


class StorageError(TjfError):
    pass


class NotFoundInStorage(StorageError):
    pass


class AlreadyExistsInStorage(StorageError):
    pass


def get_storage_error(
    *, error: kubernetes.client.ApiException, spec: dict[str, Any], action: str
) -> StorageError:
    """Function to handle some known kubernetes API exceptions."""
    error_data = {
        "k8s_object": spec,
        "k8s_error": str(error),
    }

    error_data["k8s_error"] = {
        "status_code": error.status,
        "body": error.body,
    }

    if error.status == status.HTTP_404_NOT_FOUND:
        return NotFoundInStorage("Unable to find object in storage", data=error_data)

    if error.status == status.HTTP_409_CONFLICT:
        return AlreadyExistsInStorage(
            "An object with the same name exists already", data=error_data
        )

    return StorageError(
        "Failed to {action}, likely an internal bug in the jobs api.", data=error_data
    )
