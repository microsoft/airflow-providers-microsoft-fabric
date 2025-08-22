from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Optional, Mapping, Dict, Any, Tuple

import aiohttp
from aiohttp import ClientResponseError
from airflow.exceptions import AirflowException


class HttpClient:
    """
    Minimal async HTTP client with internal redaction + caller-supplied Tenacity retry.
    
    This client provides two main methods:
    - make_request(): Core HTTP method that handles retries and returns raw aiohttp.ClientResponse
    - make_request_json(): Enhanced method that calls make_request() and adds JSON parsing + logging
    
    Architecture:
    - make_request(): Lightweight, handles HTTP mechanics and retries, returns raw response
    - make_request_json(): Builds on make_request(), consumes response and adds JSON parsing + logging
    """

    RETRYABLE_STATUSES = {408, 425, 429, 500, 502, 503, 504}

    def __init__(self, log):
        self.log = log
        self._session: Optional[aiohttp.ClientSession] = None  # Reusable session
        self._redactor = _HttpRedactor()

    async def _make_request(
        self,
        method: str,
        url: str,
        headers: Optional[Mapping[str, str]],
        tenacity_retry,
        **kwargs
    ) -> aiohttp.ClientResponse:
        """
        Core HTTP request method that handles retries and returns raw response.
        
        This method focuses on HTTP mechanics: making requests, handling retries,
        and returning the raw aiohttp.ClientResponse without consuming its payload.
        
        Returns: aiohttp.ClientResponse (unconsumed)
        """
        
        # Track retry attempts for better logging
        attempt_count = 0

        async def _attempt() -> aiohttp.ClientResponse:
            
            nonlocal attempt_count
            attempt_count += 1
            # Get retry information from tenacity_retry if available
            max_attempts = getattr(tenacity_retry.stop, 'max_attempt_number', 'unknown')

            # Get session
            session = await self._get_session()
            
            # Add Prepare Common Headers
            request_headers = headers or {}
            request_headers = dict(request_headers)  # Make a copy to avoid modifying original            
            if 'referer' not in (k.lower() for k in request_headers.keys()):
                request_headers['Referer'] = "apache-airflow-fabric-provider"
            
            # Make request and fetch request id for tracking
            resp = await session.request(method.upper(), url, headers=request_headers, **kwargs)
            req_id = (resp.headers.get("x-ms-request-id") or 
                     resp.headers.get("x-request-id") or 
                     resp.headers.get("x-ms-root-activity-id") or
                     resp.headers.get("x-ms-job-id") or
                     resp.headers.get("x-job-id") or
                     resp.headers.get("X-Job-Id"))

            # Retryable statuses -> raise ClientResponseError with enhanced logging
            if resp.status in self.RETRYABLE_STATUSES:                
                self.log.warning(
                    "HTTP Request Failed [Status: %s, request_id: %s, attempt: %d/%d]: %s %s",
                    resp.status, req_id, attempt_count, max_attempts, method.upper(), url.split('?',1)[0]
                )
                self.log.warning(resp.headers)
                
                # Read and close response before raising (to avoid ResourceWarning)
                await resp.text()
                resp.close()
                raise ClientResponseError(
                    request_info=resp.request_info,
                    history=resp.history,
                    status=resp.status,
                    message=f"Retryable HTTP {resp.status} ({method} {url.split('?',1)[0]}) req_id={req_id}",
                    headers=resp.headers,
                )

            # Non-retryable 4xx -> raise AirflowException (enhanced logging)
            if 400 <= resp.status < 500:
                self.log.error(
                    "HTTP Request Failed [Status: %s, request_id: %s, attempt: %d/%d (Non Retryable Error)]: %s %s",
                    resp.status, req_id, attempt_count, max_attempts, method.upper(), url.split('?',1)[0]
                )
                # Read and close response before raising (to avoid ResourceWarning)
                body_text = await resp.text()
                resp.close()
                raise AirflowException(
                    f"HTTP {resp.status} calling {method} {url.split('?',1)[0]} (req_id={req_id})"
                )

            # Log successful request on first attempt or after retries
            self.log.info(
                "HTTP Request [Status: %s, request_id:%s, attempt:%d/%d]: %s %s",
                resp.status, req_id, attempt_count, max_attempts, method.upper(), url.split('?',1)[0]
            )

            # Return successful response (unconsumed)
            return resp

        # Apply tenacity retry using AsyncRetrying iteration pattern
        async for attempt in tenacity_retry:
            with attempt:
                result = await _attempt()
                return result
        
        # This should never be reached, but satisfies type checker
        raise AirflowException("All retry attempts exhausted")

    async def make_request_json(
        self,
        method: str,
        url: str,
        headers: Optional[Mapping[str, str]] = None,
        tenacity_retry = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Enhanced HTTP request method with JSON parsing and detailed logging.
        
        This method calls make_request() for the core HTTP logic, then consumes
        the response and adds:
        - JSON parsing with graceful fallback to empty dict
        - Detailed request/response logging with redaction
        - Enhanced error handling for JSON parsing
        
        Returns: {"status_code": int, "headers": dict, "body": dict}
        Note: body is parsed JSON dict (or empty dict for non-JSON responses)
        """
        
        # Call the core make_request method
        response = await self._make_request(
            method=method,
            url=url,
            headers=headers,
            tenacity_retry=tenacity_retry,
            **kwargs
        )
        
        try:
            # Now consume the response payload
            body_text = await response.text()
            status_code = response.status
            response_headers = dict(response.headers)
            ctype = response_headers.get("Content-Type")
            req_id = response_headers.get("x-ms-request-id") or response_headers.get("x-request-id")

            # Log detailed request/response at debug level with redaction
            if self.log.isEnabledFor(logging.INFO):
                self.log.info(self._redactor.preview_response(
                    method, url, status_code, response_headers, body_text, ctype, req_id
                ))
            else:
                self.log.info("HTTP Request[%s]: %s - Status: %s, Request Id: %s",method, url, status_code, req_id)

            # Parse JSON with graceful handling of non-JSON responses
            body = {}  # Default to empty dict
            
            if body_text.strip():  # Only attempt parsing if there's content
                body = json.loads(body_text)

            # Return response with parsed JSON body
            return {
                "status_code": status_code,
                "headers": response_headers,
                "body": body  # Parsed JSON dict
            }
        finally:
            # Always close the response to free resources
            response.close()

    async def _get_session(self) -> aiohttp.ClientSession:
        """Create or reuse a shared aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close_session(self):
        """Explicitly close the shared aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self.log.info("Closed aiohttp session")


# -------------------- Internal redactor --------------------

@dataclass
class _HttpRedactor:
    max_preview: int = 1024
    drop_query_from_url: bool = True
    pretty_json: bool = True
    redaction_text: str = "***"

    sensitive_header_patterns: Tuple[re.Pattern, ...] = field(default_factory=lambda: (
        re.compile(r"^authorization$", re.I),
        re.compile(r"^proxy-authorization$", re.I),
        re.compile(r"^cookie$", re.I),
        re.compile(r"^set-cookie$", re.I),
        re.compile(r"^x-api-key$", re.I),
        re.compile(r"^x-ambassador-api-key$", re.I),
    ))

    sensitive_token_patterns: Tuple[re.Pattern, ...] = field(default_factory=lambda: (
        re.compile(r"([?&])(.*?(?:token|secret|password|apikey|sig|sas|signature)[^=]*)=([^&]+)", re.I),
        re.compile(r'("(?P<k>[^"]*(?:token|secret|password|apikey|sig|sas)[^"]*)"\s*:\s*")(?P<v>[^"]*)(")', re.I),
        re.compile(r"\beyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\b"),
    ))

    def preview_response(
        self,
        method: str,
        url: str,
        status: Optional[int],
        headers: Mapping[str, Any],
        body_text: Optional[str],
        content_type: Optional[str],
        request_id: Optional[str] = None,
    ) -> str:
        safe_url = self._safe_url(url)
        redacted_headers = self._redact_headers(headers)
        body_preview = self._body_preview(body_text, content_type)
        parts = [f"{method.upper()} {safe_url}"]
        if status is not None:
            parts.append(f"-> {status}")
        if request_id:
            parts.append(f"req_id={request_id}")
        parts.append(f"headers={redacted_headers}")
        parts.append(f"body_preview={body_preview}")
        return " ".join(parts)

    # ---- internals ----
    def _safe_url(self, url: str) -> str:
        if self.drop_query_from_url:
            return url.split("?", 1)[0]
        return self._redact_tokens_in_text(url)

    def _redact_headers(self, headers: Mapping[str, Any]) -> dict[str, str]:
        out: dict[str, str] = {}
        for k, v in (headers or {}).items():
            key = str(k)
            val = "" if v is None else str(v)
            out[key] = self.redaction_text if self._is_sensitive_header(key) else val
        return out

    def _body_preview(self, body_text: Optional[str], content_type: Optional[str]) -> str:
        if body_text is None:
            return "<no-body>"
        text = body_text.strip()
        if self.pretty_json and self._is_json_content_type(content_type):
            try:
                obj = json.loads(text)
                text = json.dumps(obj, indent=2, ensure_ascii=False)
            except Exception:
                pass
        text = self._redact_tokens_in_text(text)
        return text[: self.max_preview] + ("…" if len(text) > self.max_preview else "")

    @staticmethod
    def _is_json_content_type(content_type: Optional[str]) -> bool:
        if not content_type:
            return False
        ct = content_type.split(";", 1)[0].strip().lower()
        return ct == "application/json" or ct.endswith("+json")

    def _is_sensitive_header(self, name: str) -> bool:
        return any(pat.search(name) for pat in self.sensitive_header_patterns)

    def _redact_tokens_in_text(self, text: str) -> str:
        redacted = text
        for pat in self.sensitive_token_patterns:
            redacted = pat.sub(self._replacement, redacted)
        return redacted

    def _replacement(self, match: re.Match) -> str:
        if match.lastindex and match.lastindex >= 3:
            prefix, key, _ = match.group(1), match.group(2), match.group(3)
            return f"{prefix}{key}={self.redaction_text}"
        if "k" in match.re.groupindex and "v" in match.re.groupindex:
            return match.group(1) + self.redaction_text + match.group(4)
        return self.redaction_text

