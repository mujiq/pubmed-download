"""
PubChem Knowledge Graph Downloader

A robust Python application for downloading and processing PubChem RDF data
with resumable downloads, rate limiting, disk space monitoring, and idempotent operations.
"""

__version__ = "1.0.0"
__author__ = "PubChem Downloader Team"
__description__ = "Robust PubChem RDF data downloader and knowledge graph builder"

from .downloader.ftp_downloader import PubChemFTPDownloader
from .utils.config_manager import ConfigManager
from .utils.disk_monitor import DiskSpaceMonitor
from .utils.rate_limiter import RateLimiter, AdaptiveRateLimiter
from .utils.progress_tracker import ProgressTracker
from .utils.logging_setup import setup_logging

__all__ = [
    'PubChemFTPDownloader',
    'ConfigManager',
    'DiskSpaceMonitor',
    'RateLimiter',
    'AdaptiveRateLimiter',
    'ProgressTracker',
    'setup_logging'
] 