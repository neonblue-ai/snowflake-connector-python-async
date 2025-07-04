"""
Async Network Layer - aiohttp-based HTTP client for Snowflake.

This module provides AsyncSnowflakeRestful that replaces the sync requests-based
HTTP client with aiohttp while reusing all business logic from the sync network layer.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from logging import getLogger
from typing import Any, Optional
from urllib.parse import urlencode

logger = getLogger(__name__)

try:
    import aiohttp
except ImportError:
    raise ImportError(
        "aiohttp is required for async functionality. "
        "Install with: pip install snowflake-connector-python[aio]"
    )

from .. import SnowflakeConnection
from ..auth import AuthByDefault
from ..constants import (
    HTTP_HEADER_ACCEPT,
    HTTP_HEADER_CONTENT_TYPE,
    HTTP_HEADER_USER_AGENT,
)
from ..errorcode import ER_FAILED_TO_RENEW_SESSION
from ..errors import (
    Error,
    ProgrammingError,
    DatabaseError,
    InterfaceError,
)
from ..network import (
    SnowflakeRestfulJsonEncoder, 
    KEY_PAIR_AUTHENTICATOR,
    CONTENT_TYPE_APPLICATION_JSON,
    PYTHON_CONNECTOR_USER_AGENT,
    REQUEST_ID,
    REQUEST_GUID,
    ACCEPT_TYPE_APPLICATION_SNOWFLAKE,
    is_retryable_http_code,
    get_http_retryable_error,
    # Session management constants
    SESSION_EXPIRED_GS_CODE,
    MASTER_TOKEN_EXPIRED_GS_CODE,
    MASTER_TOKEN_NOTFOUND_GS_CODE,
    MASTER_TOKEN_INVALD_GS_CODE,
    ID_TOKEN_EXPIRED_GS_CODE,
    BAD_REQUEST_GS_CODE,
    REQUEST_TYPE_RENEW,
    # Query status constants
    QUERY_IN_PROGRESS_CODE,
    QUERY_IN_PROGRESS_ASYNC_CODE,
    # Exception classes
    ReauthenticationRequest,
)
from .auth import AsyncAuthByDefault, AsyncAuthByKeyPair
from .retry import AsyncRetryableOperation


class AsyncSnowflakeRestful:
    """
    Async REST client for Snowflake that composes sync networking logic.
    
    Async version of: snowflake.connector.network.SnowflakeRestful
    
    This class wraps the sync SnowflakeRestful to provide async HTTP transport
    via aiohttp while reusing all authentication, retry, and business logic.
    """
    
    def __init__(self, sync_connection: SnowflakeConnection) -> None:
        """
        Initialize async REST client by composing sync connection.
        
        Async version of: SnowflakeRestful.__init__()
        
        Args:
            sync_connection: Sync SnowflakeConnection to wrap
        """
        self._sync_connection = sync_connection
        self._session: Optional[aiohttp.ClientSession] = None
        self._base_url = f"{sync_connection._protocol}://{sync_connection._host}:{sync_connection._port}"
        
    async def authenticate(self) -> None:
        """
        Perform async authentication to establish session.
        
        Async version of: SnowflakeRestful authentication flow
        References: SnowflakeConnection.connect() and auth modules
        
        This reuses the sync authentication logic but replaces HTTP calls with aiohttp.
        """
        if self._session is None:
            # Create aiohttp session with appropriate settings
            timeout = aiohttp.ClientTimeout(
                total=self._sync_connection.login_timeout,
                connect=self._sync_connection.login_timeout
            )
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                connector=aiohttp.TCPConnector(limit=10, limit_per_host=10)
            )
            
        # Determine authentication method and create appropriate async authenticator
        auth_instance = await self._create_async_authenticator()
        
        # Prepare authentication (generate JWT token for keypair, etc.)
        await auth_instance.prepare(
            account=self._sync_connection.account,
            user=self._sync_connection.user
        )
        
        # Build base authentication request
        auth_data = self._build_auth_request_data()
        
        # Let authenticator update the request body with auth-specific data
        auth_instance.update_body(auth_data)
        
        # Perform async login request
        await self._perform_login_request(auth_data)
        
    async def _create_async_authenticator(self):
        """
        Create appropriate async authenticator based on connection parameters.
        
        Returns:
            Async authenticator instance
        """
        # Check if keypair authentication is configured
        if (self._sync_connection._private_key or 
            self._sync_connection._private_key_file or 
            self._sync_connection._authenticator == KEY_PAIR_AUTHENTICATOR):
            
            # Determine private key source
            private_key = self._sync_connection._private_key
            if self._sync_connection._private_key_file and not private_key:
                # Import here to avoid circular imports
                from ..connection import _get_private_bytes_from_file
                private_key = _get_private_bytes_from_file(
                    self._sync_connection._private_key_file,
                    self._sync_connection._private_key_file_pwd,
                )
            
            return AsyncAuthByKeyPair(private_key=private_key)
        
        # Default to basic authentication for now
        # TODO: Add support for other auth types (OAuth, SAML, etc.)
        return AsyncAuthByDefault(password=self._sync_connection._password)
        
    def _build_auth_request_data(self) -> dict:
        """
        Build base authentication request data.
        
        Delegates to sync connection logic where possible.
        
        Returns:
            Base auth request dictionary
        """
        return {
            "data": {
                "ACCOUNT_NAME": self._sync_connection.account,
                "LOGIN_NAME": self._sync_connection.user,
                "CLIENT_APP_ID": "PythonConnector",
                "CLIENT_APP_VERSION": "1.0.0",  # TODO: Get from sync connection
                # Authenticator-specific fields will be added by auth instance
            }
        }
        
    async def _perform_login_request(self, auth_data: dict) -> None:
        """
        Perform the actual login HTTP request.
        
        Args:
            auth_data: Authentication data dictionary
            
        Raises:
            Exception: If authentication fails
        """
        url = f"{self._base_url}/session/v1/login-request"
        headers = {
            HTTP_HEADER_CONTENT_TYPE: CONTENT_TYPE_APPLICATION_JSON,
            HTTP_HEADER_ACCEPT: CONTENT_TYPE_APPLICATION_JSON,
            HTTP_HEADER_USER_AGENT: PYTHON_CONNECTOR_USER_AGENT,
        }
        
        async with self._session.post(
            url,
            json=auth_data,
            headers=headers
        ) as response:
            response.raise_for_status()
            auth_result = await response.json()
            
            # Process auth response using sync connection logic
            if auth_result.get("success"):
                data = auth_result.get("data", {})
                # Store tokens in sync connection (it manages token state)
                self._sync_connection._token = data.get("token")
                self._sync_connection._master_token = data.get("masterToken")
                self._sync_connection._session_id = data.get("sessionId")
            else:
                raise Exception(f"Authentication failed: {auth_result.get('message', 'Unknown error')}")
                
    async def cmd_query(
        self,
        sql: str,
        sequence_counter: int,
        request_id: uuid.UUID,
        binding_params: Optional[Any] = None,
        binding_stage: Optional[str] = None,
        is_file_transfer: bool = False,
        statement_params: Optional[dict[str, str]] = None,
        is_internal: bool = False,
        describe_only: bool = False,
        _no_results: bool = False,
        _update_current_object: bool = True,
        _no_retry: bool = False,
        timeout: Optional[int] = None,
        dataframe_ast: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Execute query via async HTTP request using new network infrastructure.
        
        Async version of: SnowflakeConnection.cmd_query()
        
        This now uses the full async network infrastructure (request -> _post_request -> fetch)
        instead of direct aiohttp calls, providing complete feature parity including
        query polling for long-running queries.
        """
        if not self._session:
            raise RuntimeError("Not authenticated. Call authenticate() first.")
            
        # Build query request using sync connection logic (matches sync exactly)
        from ..time_util import get_time_millis
        
        data = {
            "sqlText": sql,
            "asyncExec": _no_results,
            "sequenceId": sequence_counter,
            "querySubmissionTime": get_time_millis(),
        }
        
        if dataframe_ast is not None:
            data["dataframeAst"] = dataframe_ast
        if statement_params:
            data["parameters"] = statement_params
        if is_internal:
            data["isInternal"] = is_internal
        if describe_only:
            data["describeOnly"] = describe_only
        if binding_stage:
            data["bindStage"] = binding_stage
        if binding_params:
            data["bindings"] = binding_params
            
        if not _no_results:
            # Add query context for non-async queries (matches sync behavior)
            query_context = self._sync_connection.get_query_context()
            data["queryContextDTO"] = query_context
            
        # Determine client type for proper header handling
        client = "sfsql_file_transfer" if is_file_transfer else "sfsql"
        
        # Build URL with required requestId parameter (critical for API compatibility)
        from ..compat import urlencode
        url_parameters = {REQUEST_ID: request_id}
        url = f"/queries/v1/query-request?{urlencode(url_parameters)}"
        
        # Use the new network infrastructure for complete feature parity
        # This provides query polling, session management, tracing, and retry logic
        return await self.request(
            url,
            body=data,
            method="post",
            client=client,
            timeout=timeout,
            _no_results=_no_results,
            _include_retry_params=True,  # Enable debugging support
            _no_retry=_no_retry,
        )
            
    async def _heartbeat(self) -> dict[str, Any]:
        """
        Perform async heartbeat request to keep session alive.
        
        Async version of: SnowflakeRestful._heartbeat()
        
        Reuses sync heartbeat business logic but uses aiohttp for transport.
        
        Returns:
            Heartbeat response from server
        """
        if not self._session:
            raise RuntimeError("Not authenticated. Call authenticate() first.")
            
        # Build heartbeat request following sync implementation
        headers = {
            HTTP_HEADER_CONTENT_TYPE: CONTENT_TYPE_APPLICATION_JSON,
            HTTP_HEADER_ACCEPT: CONTENT_TYPE_APPLICATION_JSON,
            HTTP_HEADER_USER_AGENT: PYTHON_CONNECTOR_USER_AGENT,
            "Authorization": f"Snowflake Token=\"{self._sync_connection._token}\"",
        }
        
        # Add service name header if configured
        if self._sync_connection.service_name:
            from ..network import HTTP_HEADER_SERVICE_NAME
            headers[HTTP_HEADER_SERVICE_NAME] = self._sync_connection.service_name
            
        request_id = str(uuid.uuid4())
        logger.debug("heartbeat request_id: %s", request_id)
        
        # Build heartbeat URL following sync pattern
        url = f"{self._base_url}/session/heartbeat?" + urlencode({REQUEST_ID: request_id})
        
        try:
            async with self._session.post(
                url,
                headers=headers,
                json=None  # Heartbeat has no body
            ) as response:
                response.raise_for_status()
                result = await response.json()
                
                # Check for session expiry in heartbeat response
                if result.get("code") == SESSION_EXPIRED_GS_CODE:
                    logger.debug("Session expired during heartbeat, attempting renewal")
                    try:
                        renewal_result = await self._renew_session()
                        if renewal_result.get("success"):
                            logger.debug("Session renewed during heartbeat, retrying")
                            # Update headers with new token and retry heartbeat
                            headers["Authorization"] = f"Snowflake Token=\"{self._sync_connection._token}\""
                            
                            # Retry heartbeat with renewed session
                            async with self._session.post(
                                url,
                                headers=headers,
                                json=None
                            ) as retry_response:
                                retry_response.raise_for_status()
                                result = await retry_response.json()
                    except ReauthenticationRequest as ex:
                        logger.debug("Session renewal failed during heartbeat")
                        # Return heartbeat failure - connection will need reauthentication
                        return {"success": False, "message": "Session renewal failed", "code": SESSION_EXPIRED_GS_CODE}
                
                # Log errors following sync pattern
                if not result.get("success"):
                    logger.error("Failed to heartbeat. code: %s, url: %s", result.get("code"), url)
                    
                return result
                
        except Exception as e:
            logger.error("Heartbeat request failed: %s", e)
            return {"success": False, "message": str(e)}

    async def close(self) -> None:
        """Close async HTTP session gracefully."""
        if self._session and not self._session.closed:
            await self._session.close()
            # Allow underlying connections to close (aiohttp best practice)
            await asyncio.sleep(0)
            self._session = None

    def update_tokens(
        self,
        session_token: str,
        master_token: str = None,
        master_validity_in_seconds: int = None,
        id_token: str = None,
        mfa_token: str = None,
    ) -> None:
        """
        Update session and master tokens in the wrapped sync connection.
        
        Async version of: SnowflakeRestful.update_tokens()
        
        This maintains token state in the sync connection that this async
        client wraps, ensuring consistent token management across both sync
        and async operations.
        
        Args:
            session_token: New session token
            master_token: New master token for session renewal
            master_validity_in_seconds: Master token validity period
            id_token: ID token (optional)
            mfa_token: MFA token (optional)
        """
        # Update tokens in the wrapped sync connection
        self._sync_connection._token = session_token
        if master_token is not None:
            self._sync_connection._master_token = master_token
        if id_token is not None:
            self._sync_connection._id_token = id_token  
        if mfa_token is not None:
            self._sync_connection._mfa_token = mfa_token
        if master_validity_in_seconds is not None:
            self._sync_connection._master_validity_in_seconds = master_validity_in_seconds
            
        logger.debug("Async connector tokens updated successfully")

    async def request(
        self,
        url: str,
        body: dict[str, Any] | None = None,
        method: str = "post",
        client: str = "sfsql",
        timeout: int | None = None,
        _no_results: bool = False,
        _include_retry_params: bool = False,
        _no_retry: bool = False,
    ) -> dict[str, Any]:
        """
        Main entry point for all network operations.
        
        Async version of: SnowflakeRestful.request()
        
        This reuses the sync connection's validation logic and delegates
        to the appropriate async HTTP method (_post_request or _get_request).
        
        Args:
            url: Request URL path
            body: Request body dictionary
            method: HTTP method ("post" or "get")
            client: Client type ("sfsql" or "sfsql_file_transfer")
            timeout: Request timeout in seconds
            _no_results: Skip result waiting for async queries
            _include_retry_params: Include retry parameters in URL
            _no_retry: Disable retry logic
            
        Returns:
            Response dictionary from server
            
        Raises:
            DatabaseError: If connection is closed or request fails
        """
        if body is None:
            body = {}
            
        # Use sync connection token validation logic
        if (not hasattr(self._sync_connection, '_token') or 
            not self._sync_connection._token) and (
            not hasattr(self._sync_connection, '_master_token') or 
            not self._sync_connection._master_token):
            from ..errorcode import ER_CONNECTION_IS_CLOSED
            from ..sqlstate import SQLSTATE_CONNECTION_NOT_EXISTS
            Error.errorhandler_wrapper(
                self._sync_connection,
                None,
                DatabaseError,
                {
                    "msg": "Connection is closed",
                    "errno": ER_CONNECTION_IS_CLOSED,
                    "sqlstate": SQLSTATE_CONNECTION_NOT_EXISTS,
                },
            )
            
        # Determine accept type based on client (matches sync logic)
        if client == "sfsql":
            accept_type = ACCEPT_TYPE_APPLICATION_SNOWFLAKE
        else:
            accept_type = CONTENT_TYPE_APPLICATION_JSON
            
        # Build headers following sync implementation
        headers = {
            HTTP_HEADER_CONTENT_TYPE: CONTENT_TYPE_APPLICATION_JSON,
            HTTP_HEADER_ACCEPT: accept_type,
            HTTP_HEADER_USER_AGENT: PYTHON_CONNECTOR_USER_AGENT,
        }
        
        # Add OpenTelemetry headers if available (matches sync behavior)
        try:
            from opentelemetry.trace.propagation.tracecontext import (
                TraceContextTextMapPropagator,
            )
            TraceContextTextMapPropagator().inject(headers)
        except Exception:
            logger.debug(
                "OpenTelemetry injection failed",
                exc_info=True,
            )
            
        # Add service name header if configured
        if self._sync_connection.service_name:
            from ..constants import HTTP_HEADER_SERVICE_NAME
            headers[HTTP_HEADER_SERVICE_NAME] = self._sync_connection.service_name
            
        # Route to appropriate HTTP method
        if method == "post":
            return await self._post_request(
                url,
                headers,
                json.dumps(body, cls=SnowflakeRestfulJsonEncoder),
                token=getattr(self._sync_connection, '_token', None),
                external_session_id=getattr(self._sync_connection, '_external_session_id', None),
                _no_results=_no_results,
                timeout=timeout,
                _include_retry_params=_include_retry_params,
                no_retry=_no_retry,
            )
        else:
            return await self._get_request(
                url,
                headers,
                token=getattr(self._sync_connection, '_token', None),
                external_session_id=getattr(self._sync_connection, '_external_session_id', None),
                timeout=timeout,
            )

    async def _get_request(
        self,
        url: str,
        headers: dict[str, str],
        token: str | None = None,
        external_session_id: str | None = None,
        timeout: int | None = None,
        is_fetch_query_status: bool = False,
    ) -> dict[str, Any]:
        """
        Execute HTTP GET request with session management.
        
        Async version of: SnowflakeRestful._get_request()
        
        This handles GET requests for result fetching and status polling,
        with automatic session renewal on session expiry.
        
        Args:
            url: Request URL path
            headers: HTTP headers dictionary
            token: Authentication token
            external_session_id: External session ID
            timeout: Request timeout in seconds
            is_fetch_query_status: Whether this is a query status fetch
            
        Returns:
            Response dictionary from server
        """
        # Remove content headers that don't apply to GET requests
        headers = headers.copy()
        if "Content-Encoding" in headers:
            del headers["Content-Encoding"]
        if "Content-Length" in headers:
            del headers["Content-Length"]
            
        # Build full URL
        full_url = f"{self._base_url}{url}"
        
        # Execute GET request via fetch method
        ret = await self.fetch(
            "get",
            full_url,
            headers,
            timeout=timeout,
            token=token,
            external_session_id=external_session_id,
            is_fetch_query_status=is_fetch_query_status,
        )
        
        # Handle session expiry with renewal (matches sync logic)
        if (ret.get("code") == SESSION_EXPIRED_GS_CODE and 
            getattr(self._sync_connection, '_authenticator', None) != "PAT_WITH_EXTERNAL_SESSION"):
            try:
                renewal_ret = await self._renew_session()
                if renewal_ret.get("success"):
                    logger.debug("Session renewed during GET request, retrying")
                    # Retry GET request with renewed session
                    return await self._get_request(
                        url,
                        headers,
                        token=getattr(self._sync_connection, '_token', None),
                        external_session_id=external_session_id,
                        timeout=timeout,
                        is_fetch_query_status=is_fetch_query_status,
                    )
            except ReauthenticationRequest as ex:
                # Handle reauthentication like sync implementation
                if (getattr(self._sync_connection, '_authenticator', None) != 
                    "EXTERNALBROWSER"):
                    raise ex.cause
                # For external browser, would need reauthentication flow
                # TODO: Implement full reauthentication when needed
                raise ex.cause
                
        return ret

    async def fetch(
        self,
        method: str,
        full_url: str,
        headers: dict[str, Any],
        data: str | None = None,
        timeout: int | None = None,
        token: str | None = None,
        external_session_id: str | None = None,
        is_fetch_query_status: bool = False,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Core HTTP request execution with retry logic.
        
        Async version of: SnowflakeRestful.fetch()
        
        This handles the actual HTTP execution with retry logic and backoff,
        replacing the sync requests session with aiohttp.
        
        Args:
            method: HTTP method ("get" or "post")
            full_url: Complete URL for request
            headers: HTTP headers dictionary
            data: Request body data (for POST requests)
            timeout: Request timeout in seconds
            token: Authentication token
            external_session_id: External session ID
            is_fetch_query_status: Whether this is a query status fetch
            
        Returns:
            Response dictionary from server
        """
        if not self._session:
            raise RuntimeError("Not authenticated. Call authenticate() first.")
            
        # Set timeout from sync connection if not provided
        if timeout is None:
            timeout = self._sync_connection.network_timeout
            
        # Add request GUID for tracing (matches sync behavior)
        full_url = self.add_request_guid(full_url)
        
        # Add retry parameters for debugging if requested
        _include_retry_params = kwargs.get("_include_retry_params", False)
        if _include_retry_params:
            full_url = self.add_retry_params(full_url, **kwargs)
            
        # Create async retry operation for request execution
        async def execute_request():
            # Prepare authentication headers
            request_headers = headers.copy()
            if token:
                request_headers["Authorization"] = f"Snowflake Token=\"{token}\""
            if external_session_id:
                request_headers["X-Snowflake-External-Session-ID"] = external_session_id
                
            # Execute HTTP request with aiohttp
            if method.upper() == "POST":
                async with self._session.post(
                    full_url,
                    data=data,
                    headers=request_headers,
                    timeout=aiohttp.ClientTimeout(total=timeout)
                ) as response:
                    # Check for retryable HTTP errors
                    if is_retryable_http_code(response.status):
                        error = get_http_retryable_error(response.status)
                        raise error
                        
                    response.raise_for_status()
                    return await response.json()
            else:  # GET
                async with self._session.get(
                    full_url,
                    headers=request_headers,
                    timeout=aiohttp.ClientTimeout(total=timeout)
                ) as response:
                    # Check for retryable HTTP errors
                    if is_retryable_http_code(response.status):
                        error = get_http_retryable_error(response.status)
                        raise error
                        
                    response.raise_for_status()
                    return await response.json()
        
        # Execute with async retry logic
        retry_operation = AsyncRetryableOperation(
            execute_request,
            max_retries=3,
            timeout=timeout,
            backoff_policy="exponential"
        )
        
        return await retry_operation.execute()

    async def _post_request(
        self,
        url: str,
        headers: dict[str, str],
        body: str,
        token: str | None = None,
        external_session_id: str | None = None,
        timeout: int | None = None,
        socket_timeout: int | None = None,
        _no_results: bool = False,
        no_retry: bool = False,
        _include_retry_params: bool = False,
    ) -> dict[str, Any]:
        """
        Execute HTTP POST request with session management and query polling.
        
        Async version of: SnowflakeRestful._post_request()
        
        This handles POST requests with session renewal, query polling for
        long-running queries, and result URL fetching.
        
        Args:
            url: Request URL path
            headers: HTTP headers dictionary
            body: Request body as JSON string
            token: Authentication token
            external_session_id: External session ID
            timeout: Request timeout in seconds
            socket_timeout: Socket timeout (unused in async)
            _no_results: Skip result waiting for async queries
            no_retry: Disable retry logic
            _include_retry_params: Include retry parameters in URL
            
        Returns:
            Response dictionary from server
        """
        full_url = f"{self._base_url}{url}"
        
        # Execute POST request via fetch method
        ret = await self.fetch(
            "post",
            full_url,
            headers,
            data=body,
            timeout=timeout,
            token=token,
            external_session_id=external_session_id,
            no_retry=no_retry,
            _include_retry_params=_include_retry_params,
        )
        
        logger.debug(
            "ret[code] = %s, after post request", ret.get("code", "N/A")
        )
        
        # Handle master token expiry (matches sync logic)
        if ret.get("code") == MASTER_TOKEN_EXPIRED_GS_CODE:
            # Mark connection as expired like sync implementation
            if hasattr(self._sync_connection, 'expired'):
                self._sync_connection.expired = True
        elif (ret.get("code") == SESSION_EXPIRED_GS_CODE and 
              getattr(self._sync_connection, '_authenticator', None) != "PAT_WITH_EXTERNAL_SESSION"):
            try:
                renewal_ret = await self._renew_session()
                if renewal_ret.get("success"):
                    logger.debug("Session renewed during POST request, retrying")
                    # Retry POST request with renewed session
                    return await self._post_request(
                        url, headers, body, 
                        token=getattr(self._sync_connection, '_token', None),
                        external_session_id=external_session_id,
                        timeout=timeout
                    )
            except ReauthenticationRequest as ex:
                if (getattr(self._sync_connection, '_authenticator', None) != 
                    "EXTERNALBROWSER"):
                    raise ex.cause
                # TODO: Implement full reauthentication flow
                raise ex.cause
                
        # Log query ID if available (matches sync behavior)
        if isinstance(ret.get("data"), dict) and ret["data"].get("queryId"):
            logger.debug("Query id: %s", ret["data"]["queryId"])
            
        # Return early for async queries if no results requested
        if ret.get("code") == QUERY_IN_PROGRESS_ASYNC_CODE and _no_results:
            return ret
            
        # Handle query polling for long-running queries (matches sync implementation)
        while ret.get("code") in (QUERY_IN_PROGRESS_CODE, QUERY_IN_PROGRESS_ASYNC_CODE):
            # Inject client pause if configured (for testing)
            if hasattr(self._sync_connection, '_inject_client_pause'):
                inject_pause = getattr(self._sync_connection, '_inject_client_pause', 0)
                if inject_pause > 0:
                    logger.debug("waiting for %s...", inject_pause)
                    await asyncio.sleep(inject_pause)
                    
            # Get result URL for polling
            result_url = ret["data"]["getResultUrl"]
            logger.debug("ping pong starting...")
            
            # Poll for query completion via GET request
            import re
            is_fetch_query_status = bool(re.match(r"^/queries/.+/result$", result_url))
            ret = await self._get_request(
                result_url,
                headers,
                token=getattr(self._sync_connection, '_token', None),
                external_session_id=external_session_id,
                timeout=timeout,
                is_fetch_query_status=is_fetch_query_status,
            )
            logger.debug("ret[code] = %s", ret.get("code", "N/A"))
            logger.debug("ping pong done")
            
        return ret

    async def delete_session(self, retry: bool = False) -> None:
        """
        Delete the current session.
        
        Async version of: SnowflakeRestful.delete_session()
        
        Args:
            retry: Whether to retry on failure (up to 3 times)
        """
        # Check if master token exists (matches sync validation)
        if not hasattr(self._sync_connection, '_master_token') or not self._sync_connection._master_token:
            from ..errorcode import ER_CONNECTION_IS_CLOSED
            from ..sqlstate import SQLSTATE_CONNECTION_NOT_EXISTS
            Error.errorhandler_wrapper(
                self._sync_connection,
                None,
                DatabaseError,
                {
                    "msg": "Connection is closed",
                    "errno": ER_CONNECTION_IS_CLOSED,
                    "sqlstate": SQLSTATE_CONNECTION_NOT_EXISTS,
                },
            )
            
        # Build session deletion URL and headers
        from ..compat import urlencode
        url = "/session?" + urlencode({"delete": "true"})
        headers = {
            HTTP_HEADER_CONTENT_TYPE: CONTENT_TYPE_APPLICATION_JSON,
            HTTP_HEADER_ACCEPT: CONTENT_TYPE_APPLICATION_JSON,
            HTTP_HEADER_USER_AGENT: PYTHON_CONNECTOR_USER_AGENT,
        }
        if self._sync_connection.service_name:
            from ..constants import HTTP_HEADER_SERVICE_NAME
            headers[HTTP_HEADER_SERVICE_NAME] = self._sync_connection.service_name
            
        body = {}
        retry_limit = 3 if retry else 1
        num_retries = 0
        
        # Retry logic matching sync implementation
        while num_retries < retry_limit:
            try:
                ret = await self._post_request(
                    url,
                    headers,
                    json.dumps(body, cls=SnowflakeRestfulJsonEncoder),
                    token=getattr(self._sync_connection, '_token', None),
                    timeout=5,
                    no_retry=True,
                )
                if ret:
                    logger.debug("Session deleted successfully")
                    return
                elif retry:
                    logger.debug("Session deletion failed, retrying...")
                    num_retries += 1
                    await asyncio.sleep(1)  # Brief delay before retry
                else:
                    logger.debug("Session deletion failed")
                    return
            except Exception as e:
                if retry and num_retries < retry_limit - 1:
                    logger.debug("Session deletion exception, retrying: %s", e)
                    num_retries += 1
                    await asyncio.sleep(1)
                else:
                    logger.debug("Session deletion failed with exception: %s", e)
                    return

    @staticmethod
    def add_request_guid(full_url: str) -> str:
        """
        Add request_guid parameter for HTTP request tracing.
        
        Async version of: SnowflakeRestful.add_request_guid()
        
        This adds a GUID to Snowflake URLs for request tracing and debugging.
        
        Args:
            full_url: Complete URL for request
            
        Returns:
            URL with request_guid parameter added
        """
        import re
        from ..constants import _SNOWFLAKE_HOST_SUFFIX_REGEX
        from ..compat import urlencode, urlparse
        
        parsed_url = urlparse(full_url)
        if not parsed_url.hostname or not re.search(_SNOWFLAKE_HOST_SUFFIX_REGEX, parsed_url.hostname):
            return full_url
            
        request_guid = str(uuid.uuid4())
        suffix = urlencode({REQUEST_GUID: request_guid})
        logger.debug("Request guid: %s", request_guid)
        sep = "&" if parsed_url.query else "?"
        
        return full_url + sep + suffix

    def add_retry_params(self, full_url: str, **kwargs) -> str:
        """
        Add retry parameters to URL for debugging.
        
        Async version of: RetryCtx.add_retry_params() in sync implementation
        
        Args:
            full_url: Complete URL for request
            **kwargs: Additional parameters including retry context
            
        Returns:
            URL with retry parameters added
        """
        from ..compat import urlencode, urlparse
        from ..time_util import get_time_millis
        
        # Get retry context information (would come from retry operation)
        retry_count = kwargs.get("current_retry_count", 0)
        start_time = kwargs.get("start_time_millis", get_time_millis())
        retry_reason = kwargs.get("retry_reason", 0)
        include_retry_reason = kwargs.get("_include_retry_reason", False)
        
        if retry_count > 0:
            retry_params = {
                "clientStartTime": start_time,
                "retryCount": retry_count,
            }
            if include_retry_reason:
                retry_params.update({"retryReason": retry_reason})
                
            suffix = urlencode(retry_params)
            sep = "&" if urlparse(full_url).query else "?"
            return full_url + sep + suffix
        else:
            return full_url

    async def _renew_session(self) -> dict[str, Any]:
        """
        Renew a session using the master token.
        
        Async version of: SnowflakeRestful._renew_session()
        
        Returns:
            Session renewal response from server
            
        Raises:
            ReauthenticationRequest: If renewal fails and reauthentication is needed
        """
        logger.debug("Starting async session renewal")
        return await self._token_request(REQUEST_TYPE_RENEW)

    async def _token_request(self, request_type: str) -> dict[str, Any]:
        """
        Make token renewal request to Snowflake server.
        
        Async version of: SnowflakeRestful._token_request()
        
        Args:
            request_type: Type of token request (e.g., REQUEST_TYPE_RENEW)
            
        Returns:
            Token request response from server
            
        Raises:
            ReauthenticationRequest: If token request fails and reauthentication is needed
        """
        master_token = getattr(self._sync_connection, '_master_token', None)
        current_token = getattr(self._sync_connection, '_token', None)
        
        logger.debug(
            "Updating session. master_token: %s",
            "****" if master_token else None
        )
        
        if not self._session:
            raise RuntimeError("Not authenticated. Call authenticate() first.")
            
        # Build headers following sync implementation
        headers = {
            HTTP_HEADER_CONTENT_TYPE: CONTENT_TYPE_APPLICATION_JSON,
            HTTP_HEADER_ACCEPT: CONTENT_TYPE_APPLICATION_JSON,
            HTTP_HEADER_USER_AGENT: PYTHON_CONNECTOR_USER_AGENT,
        }
        
        # Add service name header if configured
        if self._sync_connection.service_name:
            from ..network import HTTP_HEADER_SERVICE_NAME
            headers[HTTP_HEADER_SERVICE_NAME] = self._sync_connection.service_name
            
        request_id = str(uuid.uuid4())
        logger.debug("Token request_id: %s", request_id)
        
        # Build URL for token renewal
        from urllib.parse import urlencode
        url = f"{self._base_url}/session/token-request?" + urlencode({REQUEST_ID: request_id})
        
        # Build request body following sync implementation
        # NOTE: Use empty string if master token is not set to avoid HTTP 400
        header_token = master_token or ""
        body = {
            "oldSessionToken": current_token,
            "requestType": request_type,
        }
        
        try:
            # Make token renewal request with master token authentication
            async with self._session.post(
                url,
                json=body,
                headers={
                    **headers,
                    "Authorization": f"Snowflake Token=\"{header_token}\""
                }
            ) as response:
                response.raise_for_status()
                result = await response.json()
                
                # Process renewal response following sync logic
                if result.get("success") and result.get("data", {}).get("sessionToken"):
                    logger.debug("Session renewal successful")
                    
                    # Update tokens with new session and master tokens
                    self.update_tokens(
                        result["data"]["sessionToken"],
                        result["data"].get("masterToken"),
                        master_validity_in_seconds=result["data"].get("masterValidityInSeconds"),
                    )
                    
                    logger.debug("Session renewal completed successfully")
                    return result
                else:
                    # Handle renewal failure following sync error handling
                    logger.debug("Session renewal failed: %s", result)
                    err = result.get("message", "Unknown error")
                    if result.get("data"):
                        err += result["data"].get("errorMessage", "")
                    
                    errno = result.get("code") or ER_FAILED_TO_RENEW_SESSION
                    
                    # Check for specific error codes that require reauthentication
                    if errno in (
                        ID_TOKEN_EXPIRED_GS_CODE,
                        SESSION_EXPIRED_GS_CODE,
                        MASTER_TOKEN_NOTFOUND_GS_CODE,
                        MASTER_TOKEN_EXPIRED_GS_CODE,
                        MASTER_TOKEN_INVALD_GS_CODE,
                        BAD_REQUEST_GS_CODE,
                    ):
                        raise ReauthenticationRequest(
                            ProgrammingError(
                                msg=err,
                                errno=errno
                            )
                        )
                    else:
                        # Other errors are treated as operational errors
                        raise DatabaseError(
                            msg=err,
                            errno=errno
                        )
                        
        except aiohttp.ClientError as e:
            logger.error("Token renewal HTTP request failed: %s", e)
            raise DatabaseError(
                msg=f"Token renewal request failed: {e}",
                errno=ER_FAILED_TO_RENEW_SESSION
            )