"""
Async Time Utilities - Async versions of timing and heartbeat utilities.

This module provides async-compatible versions of time utilities,
specifically AsyncHeartBeatTimer that replaces the threading-based
HeartBeatTimer with asyncio task-based implementation.
"""

from __future__ import annotations

import asyncio
import weakref
from logging import getLogger
from typing import Awaitable, Callable, Optional

logger = getLogger(__name__)


class AsyncHeartBeatTimer:
    """
    Async version of HeartBeatTimer using asyncio tasks instead of threads.
    
    Executes a function every client_session_keep_alive_heartbeat_frequency seconds
    without blocking the event loop, compatible with asyncio applications.
    """
    
    def __init__(
        self, 
        client_session_keep_alive_heartbeat_frequency: int, 
        f: Callable[[], Awaitable[None]]
    ) -> None:
        """
        Initialize async heartbeat timer.
        
        Args:
            client_session_keep_alive_heartbeat_frequency: Interval in seconds
            f: Async function to call periodically
        """
        self.interval = client_session_keep_alive_heartbeat_frequency
        self.function = f
        self._task: Optional[asyncio.Task] = None
        self._stopped = False
        
    def start(self) -> None:
        """Start the heartbeat timer as an asyncio task."""
        if self._task is None or self._task.done():
            self._stopped = False
            self._task = asyncio.create_task(self._run())
            
    async def _run(self) -> None:
        """
        Main heartbeat loop - async version of HeartBeatTimer.run().
        
        Maintains the same business logic as sync version but uses
        asyncio.sleep() instead of threading primitives.
        """
        while not self._stopped:
            try:
                # Wait for interval using async sleep (non-blocking)
                await asyncio.sleep(self.interval)
                
                if not self._stopped:
                    try:
                        # Call the heartbeat function
                        await self.function()
                    except Exception as e:
                        # Same error handling as sync version
                        logger.debug("failed to heartbeat: %s", e)
                        
            except asyncio.CancelledError:
                # Task was cancelled, exit gracefully
                logger.debug("heartbeat timer cancelled")
                break
            except Exception as e:
                # Unexpected error, log and continue
                logger.debug("unexpected error in heartbeat timer: %s", e)
                
    def stop(self) -> None:
        """Stop the heartbeat timer."""
        self._stopped = True
        if self._task and not self._task.done():
            self._task.cancel()
            
    def is_running(self) -> bool:
        """Check if the heartbeat timer is running."""
        return self._task is not None and not self._task.done() and not self._stopped


def create_async_heartbeat_timer(
    frequency: int,
    heartbeat_func: Callable[[], Awaitable[None]]
) -> AsyncHeartBeatTimer:
    """
    Factory function to create async heartbeat timer.
    
    This maintains compatibility with sync heartbeat creation patterns
    while providing async functionality.
    
    Args:
        frequency: Heartbeat frequency in seconds
        heartbeat_func: Async function to call for heartbeat
        
    Returns:
        Configured AsyncHeartBeatTimer instance
    """
    return AsyncHeartBeatTimer(frequency, heartbeat_func)


def create_weak_async_heartbeat_func(
    method: Callable[[], Awaitable[None]]
) -> Callable[[], Awaitable[None]]:
    """
    Create a weak reference wrapper for async heartbeat function.
    
    This prevents circular references similar to the sync implementation
    while maintaining async compatibility.
    
    Args:
        method: Async method to wrap with weak reference
        
    Returns:
        Weak reference wrapper function
    """
    weak_method = weakref.WeakMethod(method)
    
    async def beat_if_possible() -> None:
        """Call heartbeat method if object still exists."""
        method_ref = weak_method()
        if method_ref is not None:
            await method_ref()
        else:
            logger.debug("heartbeat method object has been garbage collected")
            
    return beat_if_possible