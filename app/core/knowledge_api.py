from __future__ import annotations

from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2 import id_token

from app.core.config import settings


def knowledge_api_headers() -> dict[str, str]:
    if not settings.KNOWLEDGEIO_API_AUDIENCE:
        return {}

    try:
        token = id_token.fetch_id_token(
            GoogleAuthRequest(),
            settings.KNOWLEDGEIO_API_AUDIENCE,
        )
    except Exception as e:
        import subprocess

        # Fallback to local gcloud identity token
        try:
            result = subprocess.run(
                [
                    "gcloud",
                    "auth",
                    "print-identity-token",
                    "--impersonate-service-account=project-service-account@data-cipher.iam.gserviceaccount.com",
                    "--include-email",
                    f"--audiences={settings.KNOWLEDGEIO_API_AUDIENCE}",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            # Remove any warnings printed to stdout (e.g. "WARNING: This command is using...")
            stdout_lines = [
                line
                for line in result.stdout.split("\n")
                if not line.startswith("WARNING:")
            ]
            token = "".join(stdout_lines).strip()
        except subprocess.CalledProcessError:
            raise e

    return {"Authorization": f"Bearer {token}"}
