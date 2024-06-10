from cryptography import x509
from flask.wrappers import Request

from ..error import TjfClientError

AUTH_HEADER = "ssl-client-subject-dn"


class ToolAuthError(TjfClientError):
    """Custom error class for exceptions related to loading user data."""

    http_status_code = 403


def get_tool_from_request(request: Request) -> str:
    name_raw = request.headers.get(AUTH_HEADER)

    if not name_raw:
        raise ToolAuthError(f"missing '{AUTH_HEADER}' header")

    # we are expecting something like 'CN=user,O=Toolforge'
    try:
        name = x509.Name.from_rfc4514_string(name_raw)
    except Exception as e:
        raise ToolAuthError(f"Failed to parse certificate name '{name_raw}'") from e

    cn = name.get_attributes_for_oid(x509.NameOID.COMMON_NAME)
    organizations = [
        attr.value for attr in name.get_attributes_for_oid(x509.NameOID.ORGANIZATION_NAME)
    ]

    if len(cn) != 1:
        raise ToolAuthError(f"Failed to load name for certificate '{name_raw}'")

    if organizations != ["toolforge"]:
        raise ToolAuthError(
            "This certificate can't access the Jobs API. "
            "Double check you're logged in to the correct account? "
            f"(got {organizations})"
        )

    common_name = cn[0].value
    if isinstance(common_name, bytes):
        return common_name.decode()

    return common_name


def validate_toolname(request: Request, toolname: str) -> None:
    actual_toolname = get_tool_from_request(request)
    if actual_toolname != toolname:
        raise ToolAuthError(f"Toolname mismatch: expected '{toolname}', got '{actual_toolname}'")
