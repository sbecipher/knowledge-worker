import os
import json
import httpx
from urllib.parse import urlencode
from datetime import timedelta
from temporalio import activity

# Base URL of the Knowledge FastAPI application, defaults to localhost
BASE_URL = os.getenv("KNOWLEDGE_API_URL", "http://localhost:8000").rstrip('/')
# Configurable timeouts (in seconds)
HTTP_TIMEOUT = float(os.getenv("HTTP_CLIENT_TIMEOUT", "60"))
STREAM_TIMEOUT = float(os.getenv("STREAM_CLIENT_TIMEOUT", "600"))

@activity.defn(name="check_api_health")
async def check_api_health() -> None:
    """
    Health check against the Knowledge API health endpoint. Raises on non-200.
    """
    # Cancellation guard
    if activity.is_cancelled():
        raise RuntimeError("check_api_health activity cancelled")
    url = f"{BASE_URL}/health"
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        response = await client.get(url)
        response.raise_for_status()
    # Heartbeat to indicate liveness
    activity.heartbeat({"status": "healthy"})

@activity.defn(name="fetch_company_articles_batch")
async def fetch_company_articles_batch(company: str, year: int) -> list[dict]:
    """
    Fetches articles for a company and year. Batch Companies articles per-article via NDJSON.
    """
    # Early cancellation guard
    if activity.is_cancelled():
        raise RuntimeError("fetch_company_articles activity cancelled before start")
    endpoint = f"/api/v1/companies/{company}/{year}/batch"
    url = f"{BASE_URL}{endpoint}"
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.json()

@activity.defn(name="fetch_company_articles_stream")
async def fetch_company_articles(company: str, year: int) -> list[dict]:
    """
    Fetches articles for a company and year. Streams Companies articles per-article via NDJSON.
    """
    # Early cancellation guard
    if activity.is_cancelled():
        raise RuntimeError("fetch_company_articles activity cancelled before start")
    # Use streaming endpoint for compnaies to allow per-article heartbeats

    endpoint = f"/api/v1/companies/{company}/{year}/stream"
    url = f"{BASE_URL}{endpoint}"
    articles: list[dict] = []
    async with httpx.AsyncClient(timeout=STREAM_TIMEOUT) as client:
        response = await client.stream("GET", url)
        response.raise_for_status()
        # Read NDJSON lines
        async for line in response.aiter_lines():
            if not line:
                continue
            try:
                article = json.loads(line)
            except json.JSONDecodeError:
                activity.logger.warning("Skipping invalid JSON line in stream: %r", line)
                continue
            # Heartbeat progress
            activity.heartbeat({"article_title": article.get("title")})
            # Check for cancellation during processing
            if activity.is_cancelled():
                activity.logger.info("fetch_company_articles activity cancelled during FEAM stream")
                break
            articles.append(article)
    return articles

@activity.defn(name="list_company_articles")
async def list_company_articles(company: str, year: int) -> list[dict]:
    """
    Lists all articles for a given company and year (metadata only).
    """
    if activity.is_cancelled():
        raise RuntimeError("list_company_articles activity cancelled before start")
    endpoint = f"/api/v1/companies/{company}/{year}/articles"
    url = f"{BASE_URL}{endpoint}"
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        response = await client.get(url)
        response.raise_for_status()
        articles = response.json()
    activity.heartbeat({"count": len(articles)})
    return articles

@activity.defn(name="process_company_article")
async def process_company_article(company: str, year: int, article: dict) -> dict:
    """
    Processes a single article. Currently a no-op stub that returns the metadata.
    """
    if activity.is_cancelled():
        raise RuntimeError("process_company_article activity cancelled")
    activity.heartbeat({"processing_title": article.get("title")})
    # Perform simple validation of the article URL via HTTP HEAD/GET
    title = article.get("title")
    url = article.get("url")
    validated = False
    content_length = None
    error_msg = None
    if not url:
        error_msg = "No URL provided in article metadata"
    else:
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, follow_redirects=True) as client:
                url = f"{BASE_URL}" + f"/api/v1/companies/{company}/{year}/article"
                params = urlencode({"title": title, "url": url})
                full_url = f"{url}?{params}"
                response = await client.get(full_url)  # Ensure the URL is reachable
                response.raise_for_status()
                validated = True
                # Capture content length if provided
                cl = response.headers.get("content-length")
                content_length = int(cl) if cl and cl.isdigit() else None
                activity.logger.info(
                    f"Article {title} ({url}) validated successfully. "
                    f"Content-Length: {content_length}, Knowledge API URL: {full_url}"
                )
        except Exception as e:
            error_msg = str(e)
            activity.logger.error(
                f"Validation failed for URL {url}: {e}", exc_info=True
            )
    response = response.json()
    if error_msg:
        response["validation_error"] = error_msg
    # Heartbeat final status
    activity.heartbeat({"validated": validated})
    return response
