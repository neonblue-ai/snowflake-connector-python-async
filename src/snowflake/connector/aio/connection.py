"""
Async Snowflake Connection - True async connection using composition.

This module provides AsyncSnowflakeConnection that wraps the synchronous
SnowflakeConnection to provide async I/O while automatically inheriting
all business logic from the base library.
"""

from __future__ import annotations

import uuid
import weakref
from logging import getLogger
from typing import TYPE_CHECKING, Any, Optional

from .. import SnowflakeConnection
from ..cursor import SnowflakeCursor
from ..time_util import get_time_millis
from .network import AsyncSnowflakeRestful
from .time_util import AsyncHeartBeatTimer, create_weak_async_heartbeat_func

if TYPE_CHECKING:
    from .cursor import AsyncSnowflakeCursor

logger = getLogger(__name__)


class AsyncSnowflakeConnection:
    """
    Async Snowflake connection that composes a sync connection.
    
    Async version of: snowflake.connector.connection.SnowflakeConnection
    
    This class wraps the synchronous SnowflakeConnection to provide true async I/O
    while automatically inheriting all business logic improvements from the base
    library. Only the network layer is replaced with aiohttp.
    """
    
    def __init__(self, **kwargs: Any) -> None:
        """
        Initialize async connection by composing sync connection.
        
        Async version of: SnowflakeConnection.__init__()
        
        Args:
            **kwargs: All connection parameters passed to sync SnowflakeConnection
        """
        # Compose sync connection for all business logic
        self._sync_connection = SnowflakeConnection(**kwargs)
        self._async_rest_client: Optional[AsyncSnowflakeRestful] = None
        self._is_connected = False
        # Async heartbeat timer for session keep-alive
        self.heartbeat_timer: Optional[AsyncHeartBeatTimer] = None
        
    async def connect(self) -> None:
        """
        Establish async connection to Snowflake.
        
        Async version of: SnowflakeConnection.connect()
        
        This replaces the sync connection's network layer with an async one
        while reusing all authentication logic.
        """
        # Create async REST client that wraps the sync connection
        self._async_rest_client = AsyncSnowflakeRestful(self._sync_connection)
        
        # Perform async authentication
        await self._async_rest_client.authenticate()
        self._is_connected = True
        
        # Initialize async heartbeat if session keep-alive is enabled
        if self._sync_connection.client_session_keep_alive:
            await self._add_async_heartbeat()
        
    async def commit(self) -> None:
        """
        Commit current transaction.
        
        Async version of: SnowflakeConnection.commit()
        """
        cursor = self.cursor()
        await cursor.execute("COMMIT")
        
    async def rollback(self) -> None:
        """
        Rollback current transaction.
        
        Async version of: SnowflakeConnection.rollback()
        """
        cursor = self.cursor()
        await cursor.execute("ROLLBACK")
        
    async def cmd_query(
        self,
        sql: str,
        sequence_counter: int,
        request_id: uuid.UUID,
        binding_params: None | tuple | dict[str, dict[str, str]] = None,
        binding_stage: str | None = None,
        is_file_transfer: bool = False,
        statement_params: dict[str, str] | None = None,
        is_internal: bool = False,
        describe_only: bool = False,
        _no_results: bool = False,
        _update_current_object: bool = True,
        _no_retry: bool = False,
        timeout: int | None = None,
        dataframe_ast: str | None = None,
    ) -> dict[str, Any]:
        """
        Execute query via async HTTP request.
        
        Async version of: SnowflakeConnection.cmd_query()
        
        This method provides direct access to the async network layer for query execution.
        """
        if not self._is_connected or not self._async_rest_client:
            raise RuntimeError("Connection not established")
            
        return await self._async_rest_client.cmd_query(
            sql=sql,
            sequence_counter=sequence_counter,
            request_id=request_id,
            binding_params=binding_params,
            binding_stage=binding_stage,
            is_file_transfer=is_file_transfer,
            statement_params=statement_params,
            is_internal=is_internal,
            describe_only=describe_only,
            _no_results=_no_results,
            _update_current_object=_update_current_object,
            _no_retry=_no_retry,
            timeout=timeout,
            dataframe_ast=dataframe_ast,
        )
    
    async def autocommit(self, mode: bool) -> None:
        """
        Set autocommit mode.
        
        Async version of: SnowflakeConnection.autocommit()
        
        Args:
            mode: True to enable autocommit, False to disable
        """
        # Delegate to sync connection for consistency
        self._sync_connection.autocommit(mode)
    
    async def execute_string(
        self, 
        sql_text: str, 
        remove_comments: bool = False,
        return_cursors: bool = True
    ) -> list['AsyncSnowflakeCursor'] | list[dict[str, Any]]:
        """
        Execute multiple SQL statements.
        
        Async version of: SnowflakeConnection.execute_string()
        
        Args:
            sql_text: SQL statements to execute
            remove_comments: Whether to remove comments from SQL
            return_cursors: Whether to return cursors or results
            
        Returns:
            List of cursors or results
        """
        # Use sync connection to parse SQL, then execute async
        sync_results = self._sync_connection.execute_string(
            sql_text, 
            remove_comments=remove_comments, 
            return_cursors=False
        )
        
        if return_cursors:
            # Convert results to async cursors
            async_cursors = []
            for _ in sync_results:
                cursor = self.cursor()
                # Execute individual statements would need to be handled
                # For now, delegate to sync behavior
                async_cursors.append(cursor)
            return async_cursors
        else:
            return sync_results
    
    async def is_valid(self) -> bool:
        """
        Check if connection is valid.
        
        Async version of: SnowflakeConnection.is_valid()
        
        Uses async heartbeat to validate session like sync version.
        
        Returns:
            True if connection is valid, False otherwise
        """
        if not self._is_connected:
            logger.debug("connection is not connected and not valid")
            return False
            
        try:
            logger.debug("trying to async heartbeat into the session to validate")
            hb_result = await self._async_heartbeat()
            session_valid = hb_result.get("success")
            logger.debug("session still valid? %s", session_valid)
            return bool(session_valid)
        except Exception as e:
            logger.debug("session could not be validated due to exception: %s", e)
            return False
        
    async def close(self) -> None:
        """
        Close async connection and cleanup resources.
        
        Async version of: SnowflakeConnection.close()
        """
        # Cancel async heartbeat first
        await self._cancel_async_heartbeat()
        
        if self._async_rest_client:
            await self._async_rest_client.close()
            self._async_rest_client = None
            
        # Close sync connection
        self._sync_connection.close()
        self._is_connected = False
        
    def cursor(self) -> 'AsyncSnowflakeCursor':
        """
        Create an async cursor for this connection.
        
        Async version of: SnowflakeConnection.cursor()
        
        Returns:
            AsyncSnowflakeCursor: New async cursor object
        """
        # Import here to avoid circular imports
        from .cursor import AsyncSnowflakeCursor
        return AsyncSnowflakeCursor(self)
    
    async def _add_async_heartbeat(self) -> None:
        """
        Add a periodic async heartbeat to keep connection alive.
        
        Async version of: SnowflakeConnection._add_heartbeat()
        
        Uses AsyncHeartBeatTimer with asyncio tasks instead of threading.
        """
        if not self.heartbeat_timer:
            # Validate heartbeat frequency using sync connection logic
            self._sync_connection._validate_client_session_keep_alive_heartbeat_frequency()
            
            # Create weak reference async heartbeat function
            heartbeat_func = create_weak_async_heartbeat_func(self._async_heartbeat_tick)
            
            # Create async heartbeat timer
            self.heartbeat_timer = AsyncHeartBeatTimer(
                self._sync_connection.client_session_keep_alive_heartbeat_frequency,
                heartbeat_func,
            )
            self.heartbeat_timer.start()
            logger.debug("started async heartbeat")
    
    async def _cancel_async_heartbeat(self) -> None:
        """
        Cancel the async heartbeat timer.
        
        Async version of: SnowflakeConnection._cancel_heartbeat()
        """
        if self.heartbeat_timer:
            self.heartbeat_timer.stop()
            self.heartbeat_timer = None
            logger.debug("stopped async heartbeat")
    
    async def _async_heartbeat_tick(self) -> None:
        """
        Execute an async heartbeat if connection isn't closed.
        
        Async version of: SnowflakeConnection._heartbeat_tick()
        """
        if not self.is_closed():
            logger.debug("async heartbeating!")
            await self._async_rest_client._heartbeat()
    
    async def _async_heartbeat(self) -> dict[str, Any]:
        """
        Perform async heartbeat request to keep session alive.
        
        Async version of network heartbeat functionality.
        
        Returns:
            Heartbeat response from server
        """
        if not self._async_rest_client:
            raise RuntimeError("Async REST client not initialized")
        
        return await self._async_rest_client._heartbeat()
        
    # Delegate properties to sync connection for automatic updates
    
    @property
    def session_id(self) -> Optional[str]:
        """
        Get session ID from sync connection.
        
        Delegates to: SnowflakeConnection.session_id
        """
        return self._sync_connection.session_id
        
    @property
    def user(self) -> Optional[str]:
        """
        Get username from sync connection.
        
        Delegates to: SnowflakeConnection.user
        """
        return self._sync_connection.user
        
    @property
    def account(self) -> Optional[str]:
        """
        Get account from sync connection.
        
        Delegates to: SnowflakeConnection.account
        """
        return self._sync_connection.account
        
    @property
    def database(self) -> Optional[str]:
        """Get database from sync connection."""
        return self._sync_connection.database
        
    @property
    def schema(self) -> Optional[str]:
        """Get schema from sync connection."""
        return self._sync_connection.schema
        
    @property
    def warehouse(self) -> Optional[str]:
        """Get warehouse from sync connection."""
        return self._sync_connection.warehouse
        
    @property
    def role(self) -> Optional[str]:
        """Get role from sync connection."""
        return self._sync_connection.role
        
    @property
    def host(self) -> str:
        """Get host from sync connection."""
        return self._sync_connection.host
        
    @property
    def port(self) -> int:
        """Get port from sync connection."""
        return self._sync_connection.port
    
    @property
    def login_timeout(self) -> Optional[int]:
        """Login timeout in seconds."""
        return getattr(self._sync_connection, 'login_timeout', None)
    
    @login_timeout.setter
    def login_timeout(self, value: int) -> None:
        """Set login timeout."""
        if hasattr(self._sync_connection, 'login_timeout'):
            self._sync_connection.login_timeout = value
    
    @property
    def network_timeout(self) -> Optional[int]:
        """Network timeout in seconds."""
        return getattr(self._sync_connection, 'network_timeout', None)
    
    @network_timeout.setter
    def network_timeout(self, value: int) -> None:
        """Set network timeout."""
        if hasattr(self._sync_connection, 'network_timeout'):
            self._sync_connection.network_timeout = value
    
    @property
    def client_prefetch_threads(self) -> Optional[int]:
        """Number of threads for prefetching results."""
        return getattr(self._sync_connection, 'client_prefetch_threads', None)
    
    @client_prefetch_threads.setter  
    def client_prefetch_threads(self, value: int) -> None:
        """Set client prefetch threads."""
        if hasattr(self._sync_connection, 'client_prefetch_threads'):
            self._sync_connection.client_prefetch_threads = value
    
    @property
    def rest(self):
        """Access to internal REST client (for compatibility)."""
        # Return the async REST client for async operations
        return self._async_rest_client
        
    def is_closed(self) -> bool:
        """Check if connection is closed."""
        return not self._is_connected or self._sync_connection.is_closed()
        
    def __repr__(self) -> str:
        """String representation of async connection."""
        return f"<AsyncSnowflakeConnection(user='{self.user}', account='{self.account}', database='{self.database}')>"