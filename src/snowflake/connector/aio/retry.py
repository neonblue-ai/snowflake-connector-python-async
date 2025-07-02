"""
Async Retry Mechanisms - Async-compatible retry utilities.

This module provides async versions of retry mechanisms and backoff policies,
reusing the business logic from sync implementations while using asyncio.sleep()
instead of time.sleep().
"""

from __future__ import annotations

import asyncio
from typing import AsyncGenerator, Generator

from ..backoff_policies import (
    exponential_backoff,
    linear_backoff,
    mixed_backoff,
)
from ..time_util import TimeoutBackoffCtx, get_time_millis


class AsyncTimeoutBackoffCtx(TimeoutBackoffCtx):
    """
    Async version of TimeoutBackoffCtx for retry coordination.
    
    Reuses all sync business logic but provides async-compatible sleep method.
    """
    
    async def async_sleep(self) -> None:
        """
        Sleep for current sleep time using async sleep.
        
        Uses asyncio.sleep() instead of time.sleep() to avoid blocking
        the event loop during retry delays.
        """
        if self._current_sleep_time > 0:
            await asyncio.sleep(self._current_sleep_time)
            
    async def wait_and_increment(self) -> None:
        """
        Wait for backoff period and increment retry count.
        
        Combines sleep and increment operations for convenience.
        """
        await self.async_sleep()
        self.increment()


def create_async_backoff_generator(
    backoff_generator: Generator[int, None, None]
) -> AsyncGenerator[int, None]:
    """
    Convert sync backoff generator to async version.
    
    Args:
        backoff_generator: Sync backoff generator from backoff_policies
        
    Yields:
        Backoff intervals for async retry loops
    """
    async def async_wrapper():
        for backoff_time in backoff_generator:
            yield backoff_time
    
    return async_wrapper()


async def async_retry_with_backoff(
    operation,
    max_retries: int = 3,
    timeout: int | None = None,
    backoff_generator: Generator[int, None, None] | None = None,
    exception_types: tuple = (Exception,)
):
    """
    Async retry wrapper with configurable backoff policy.
    
    Reuses sync retry business logic but uses async sleep for delays.
    
    Args:
        operation: Async callable to retry
        max_retries: Maximum number of retry attempts
        timeout: Total timeout in seconds
        backoff_generator: Backoff policy generator
        exception_types: Exception types to retry on
        
    Returns:
        Result of successful operation
        
    Raises:
        Last exception if all retries exhausted
    """
    if backoff_generator is None:
        backoff_generator = exponential_backoff()()
        
    ctx = AsyncTimeoutBackoffCtx(
        max_retry_attempts=max_retries,
        timeout=timeout,
        backoff_generator=backoff_generator
    )
    
    ctx.set_start_time()
    last_exception = None
    
    while ctx.should_retry:
        try:
            result = await operation()
            return result
        except exception_types as e:
            last_exception = e
            if ctx.should_retry:
                await ctx.wait_and_increment()
            else:
                break
    
    # If we get here, all retries were exhausted
    if last_exception:
        raise last_exception
    else:
        raise RuntimeError("All retries exhausted without exception")


class AsyncRetryableOperation:
    """
    Wrapper for async operations that need retry logic.
    
    Provides a consistent interface for retryable async operations
    while reusing sync retry business logic.
    """
    
    def __init__(
        self,
        operation,
        max_retries: int = 3,
        timeout: int | None = None,
        backoff_policy: str = "exponential"
    ):
        """
        Initialize retryable async operation.
        
        Args:
            operation: Async callable to wrap with retry logic
            max_retries: Maximum retry attempts
            timeout: Total timeout in seconds
            backoff_policy: Backoff policy name (exponential, linear, mixed)
        """
        self.operation = operation
        self.max_retries = max_retries
        self.timeout = timeout
        
        # Select backoff policy - call generator function to get iterator
        if backoff_policy == "exponential":
            self.backoff_generator = exponential_backoff()()
        elif backoff_policy == "linear":
            self.backoff_generator = linear_backoff()()
        elif backoff_policy == "mixed":
            self.backoff_generator = mixed_backoff()()
        else:
            raise ValueError(f"Unknown backoff policy: {backoff_policy}")
    
    async def execute(self, *args, **kwargs):
        """
        Execute the operation with retry logic.
        
        Args:
            *args: Arguments to pass to the operation
            **kwargs: Keyword arguments to pass to the operation
            
        Returns:
            Result of successful operation
        """
        async def wrapped_operation():
            return await self.operation(*args, **kwargs)
            
        return await async_retry_with_backoff(
            wrapped_operation,
            max_retries=self.max_retries,
            timeout=self.timeout,
            backoff_generator=self.backoff_generator
        )