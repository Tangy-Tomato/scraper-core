import hmac

from fastapi import Header, HTTPException, Request, status


def require_worker_auth(
    request: Request, authorization: str | None = Header(default=None)
) -> None:
    expected = request.app.state.settings.auth_token
    provided = ""
    if authorization and authorization.startswith("Bearer "):
        provided = authorization[7:]
    if not provided or not hmac.compare_digest(provided, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid or missing bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )
