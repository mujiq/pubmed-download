"""
Rate limiting utilities for PubChem FTP downloads.
"""

import time
import threading
import logging
from typing import Optional
from collections import deque


class RateLimiter:
    """Thread-safe rate limiter for controlling download request frequency."""
    
    def __init__(self, delay_seconds: float = 2.0, max_requests_per_minute: Optional[int] = None):
        """
        Initialize rate limiter.
        
        Args:
            delay_seconds: Minimum delay between requests
            max_requests_per_minute: Maximum requests per minute (optional)
        """
        self.delay_seconds = delay_seconds
        self.max_requests_per_minute = max_requests_per_minute
        self.last_request_time = 0.0
        self.request_times = deque()
        self.lock = threading.Lock()
        self.logger = logging.getLogger(__name__)
        
    def wait(self):
        """Wait if necessary to respect rate limits."""
        with self.lock:
            current_time = time.time()
            
            # Clean old request times (older than 60 seconds)
            if self.max_requests_per_minute:
                while (self.request_times and 
                       current_time - self.request_times[0] > 60):
                    self.request_times.popleft()
                
                # Check if we've exceeded requests per minute
                if len(self.request_times) >= self.max_requests_per_minute:
                    sleep_time = 60 - (current_time - self.request_times[0])
                    if sleep_time > 0:
                        self.logger.debug(f"Rate limit reached, sleeping for {sleep_time:.2f} seconds")
                        time.sleep(sleep_time)
                        current_time = time.time()
            
            # Enforce minimum delay between requests
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.delay_seconds:
                sleep_time = self.delay_seconds - time_since_last
                self.logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
                current_time = time.time()
            
            # Record this request
            self.last_request_time = current_time
            if self.max_requests_per_minute:
                self.request_times.append(current_time)
    
    def get_stats(self) -> dict:
        """Get rate limiter statistics."""
        with self.lock:
            current_time = time.time()
            
            # Clean old request times
            if self.max_requests_per_minute:
                while (self.request_times and 
                       current_time - self.request_times[0] > 60):
                    self.request_times.popleft()
            
            return {
                'delay_seconds': self.delay_seconds,
                'max_requests_per_minute': self.max_requests_per_minute,
                'requests_in_last_minute': len(self.request_times) if self.max_requests_per_minute else None,
                'time_since_last_request': current_time - self.last_request_time
            }


class AdaptiveRateLimiter(RateLimiter):
    """Rate limiter that adapts based on server response and errors."""
    
    def __init__(self, initial_delay: float = 2.0, min_delay: float = 0.5, 
                 max_delay: float = 30.0, backoff_factor: float = 2.0):
        """
        Initialize adaptive rate limiter.
        
        Args:
            initial_delay: Initial delay between requests
            min_delay: Minimum delay allowed
            max_delay: Maximum delay allowed
            backoff_factor: Factor to increase delay on errors
        """
        super().__init__(initial_delay)
        self.initial_delay = initial_delay
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.error_count = 0
        self.success_count = 0
        
    def on_success(self):
        """Call this method when a request succeeds."""
        with self.lock:
            self.success_count += 1
            
            # Gradually reduce delay on consecutive successes
            if self.success_count >= 5 and self.delay_seconds > self.min_delay:
                old_delay = self.delay_seconds
                self.delay_seconds = max(
                    self.min_delay, 
                    self.delay_seconds * 0.9
                )
                if old_delay != self.delay_seconds:
                    self.logger.debug(f"Reduced delay to {self.delay_seconds:.2f}s after {self.success_count} successes")
                self.success_count = 0
            
            # Reset error count on success
            if self.error_count > 0:
                self.error_count = 0
    
    def on_error(self, error_type: str = "general"):
        """
        Call this method when a request fails.
        
        Args:
            error_type: Type of error (timeout, connection, etc.)
        """
        with self.lock:
            self.error_count += 1
            self.success_count = 0
            
            # Increase delay on errors
            old_delay = self.delay_seconds
            self.delay_seconds = min(
                self.max_delay,
                self.delay_seconds * self.backoff_factor
            )
            
            self.logger.warning(
                f"Error #{self.error_count} ({error_type}): "
                f"increased delay from {old_delay:.2f}s to {self.delay_seconds:.2f}s"
            )
    
    def reset(self):
        """Reset the rate limiter to initial state."""
        with self.lock:
            self.delay_seconds = self.initial_delay
            self.error_count = 0
            self.success_count = 0
            self.logger.info("Rate limiter reset to initial state")
    
    def get_stats(self) -> dict:
        """Get adaptive rate limiter statistics."""
        stats = super().get_stats()
        stats.update({
            'error_count': self.error_count,
            'success_count': self.success_count,
            'initial_delay': self.initial_delay,
            'min_delay': self.min_delay,
            'max_delay': self.max_delay,
            'backoff_factor': self.backoff_factor
        })
        return stats 