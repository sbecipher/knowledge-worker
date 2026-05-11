from __future__ import annotations

from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2 import id_token

from app.core.config import settings


def knowledge_api_headers() -> dict[str, str]:
    if not settings.KNOWLEDGEIO_API_AUDIENCE:
        return {}
    token = id_token.fetch_id_token(
        GoogleAuthRequest(),
        settings.KNOWLEDGEIO_API_AUDIENCE,
    )
    return {"Authorization": f"Bearer {token}"}
