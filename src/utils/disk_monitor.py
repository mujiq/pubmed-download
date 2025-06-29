"""
Disk space monitoring utilities for PubChem downloader.
"""

import os
import logging
import psutil
from typing import Optional
from pathlib import Path


class DiskSpaceMonitor:
    """Monitor disk space and prevent downloads when space is insufficient."""
    
    def __init__(self, min_free_space_gb: float = 50.0, check_interval: int = 60):
        """
        Initialize disk space monitor.
        
        Args:
            min_free_space_gb: Minimum free space required in GB
            check_interval: Interval between checks in seconds
        """
        self.min_free_space_gb = min_free_space_gb
        self.min_free_space_bytes = min_free_space_gb * 1024 * 1024 * 1024
        self.check_interval = check_interval
        self.logger = logging.getLogger(__name__)
        
    def get_free_space(self, path: str) -> float:
        """
        Get free space for the given path in GB.
        
        Args:
            path: Path to check
            
        Returns:
            Free space in GB
        """
        try:
            # Ensure path exists
            Path(path).mkdir(parents=True, exist_ok=True)
            
            # Get disk usage statistics
            usage = psutil.disk_usage(path)
            free_gb = usage.free / (1024 * 1024 * 1024)
            
            self.logger.debug(f"Free space at {path}: {free_gb:.2f} GB")
            return free_gb
            
        except Exception as e:
            self.logger.error(f"Failed to get disk space for {path}: {e}")
            return 0.0
    
    def has_sufficient_space(self, path: str, required_space_gb: Optional[float] = None) -> bool:
        """
        Check if there's sufficient disk space.
        
        Args:
            path: Path to check
            required_space_gb: Required space in GB (defaults to min_free_space_gb)
            
        Returns:
            True if sufficient space available
        """
        if required_space_gb is None:
            required_space_gb = self.min_free_space_gb
            
        free_space_gb = self.get_free_space(path)
        sufficient = free_space_gb >= required_space_gb
        
        if not sufficient:
            self.logger.warning(
                f"Insufficient disk space. Required: {required_space_gb:.2f} GB, "
                f"Available: {free_space_gb:.2f} GB"
            )
        
        return sufficient
    
    def get_directory_size(self, path: str) -> float:
        """
        Get the total size of a directory in GB.
        
        Args:
            path: Directory path
            
        Returns:
            Directory size in GB
        """
        try:
            total_size = 0
            for dirpath, dirnames, filenames in os.walk(path):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    if os.path.exists(filepath):
                        total_size += os.path.getsize(filepath)
            
            size_gb = total_size / (1024 * 1024 * 1024)
            self.logger.debug(f"Directory {path} size: {size_gb:.2f} GB")
            return size_gb
            
        except Exception as e:
            self.logger.error(f"Failed to calculate directory size for {path}: {e}")
            return 0.0
    
    def cleanup_temp_files(self, temp_dir: str) -> float:
        """
        Clean up temporary files to free space.
        
        Args:
            temp_dir: Temporary directory to clean
            
        Returns:
            Space freed in GB
        """
        try:
            if not os.path.exists(temp_dir):
                return 0.0
                
            initial_size = self.get_directory_size(temp_dir)
            
            # Remove all files in temp directory
            for root, dirs, files in os.walk(temp_dir, topdown=False):
                for file in files:
                    try:
                        os.remove(os.path.join(root, file))
                    except Exception as e:
                        self.logger.debug(f"Failed to remove temp file {file}: {e}")
                        
                for dir in dirs:
                    try:
                        os.rmdir(os.path.join(root, dir))
                    except Exception as e:
                        self.logger.debug(f"Failed to remove temp dir {dir}: {e}")
            
            final_size = self.get_directory_size(temp_dir)
            freed_space = initial_size - final_size
            
            self.logger.info(f"Cleaned up {freed_space:.2f} GB from temp directory")
            return freed_space
            
        except Exception as e:
            self.logger.error(f"Failed to cleanup temp files: {e}")
            return 0.0
    
    def get_disk_usage_info(self, path: str) -> dict:
        """
        Get comprehensive disk usage information.
        
        Args:
            path: Path to check
            
        Returns:
            Dictionary with disk usage information
        """
        try:
            usage = psutil.disk_usage(path)
            
            return {
                'total_gb': usage.total / (1024 * 1024 * 1024),
                'used_gb': usage.used / (1024 * 1024 * 1024),
                'free_gb': usage.free / (1024 * 1024 * 1024),
                'percent_used': (usage.used / usage.total) * 100,
                'percent_free': (usage.free / usage.total) * 100,
                'sufficient_space': usage.free >= self.min_free_space_bytes
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get disk usage info: {e}")
            return {} 