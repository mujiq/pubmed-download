"""
Logging setup utilities for PubChem downloader.
"""

import os
import logging
import logging.handlers
from typing import Dict, Any
from pathlib import Path


def setup_logging(config: Dict[str, Any]) -> logging.Logger:
    """
    Set up logging configuration based on config.
    
    Args:
        config: Logging configuration dictionary
        
    Returns:
        Configured logger
    """
    log_config = config.get('logging', {})
    
    # Extract configuration values
    log_level = log_config.get('level', 'INFO').upper()
    log_file = log_config.get('log_file', 'logs/pubchem_downloader.log')
    log_format = log_config.get('log_format', 
                               '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    max_log_size_mb = log_config.get('max_log_size_mb', 100)
    backup_count = log_config.get('backup_count', 5)
    
    # Ensure log directory exists
    log_dir = Path(log_file).parent
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level, logging.INFO))
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(log_format)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, log_level, logging.INFO))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler with rotation
    max_bytes = max_log_size_mb * 1024 * 1024  # Convert MB to bytes
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    file_handler.setLevel(getattr(logging, log_level, logging.INFO))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Create a specific logger for the application
    app_logger = logging.getLogger('pubchem_downloader')
    app_logger.info(f"Logging initialized - Level: {log_level}, File: {log_file}")
    
    return app_logger


def create_progress_logger(log_file: str = "logs/download_progress.log") -> logging.Logger:
    """
    Create a separate logger for download progress.
    
    Args:
        log_file: Path to progress log file
        
    Returns:
        Progress logger
    """
    # Ensure log directory exists
    log_dir = Path(log_file).parent
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Create progress logger
    progress_logger = logging.getLogger('pubchem_progress')
    progress_logger.setLevel(logging.INFO)
    
    # Clear any existing handlers
    progress_logger.handlers.clear()
    
    # Create formatter for progress logs
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    
    # File handler for progress
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=50 * 1024 * 1024,  # 50MB
        backupCount=3,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    progress_logger.addHandler(file_handler)
    
    # Prevent propagation to avoid double logging
    progress_logger.propagate = False
    
    return progress_logger


def create_error_logger(log_file: str = "logs/download_errors.log") -> logging.Logger:
    """
    Create a separate logger for errors and warnings.
    
    Args:
        log_file: Path to error log file
        
    Returns:
        Error logger
    """
    # Ensure log directory exists
    log_dir = Path(log_file).parent
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Create error logger
    error_logger = logging.getLogger('pubchem_errors')
    error_logger.setLevel(logging.WARNING)
    
    # Clear any existing handlers
    error_logger.handlers.clear()
    
    # Create formatter for error logs
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    
    # File handler for errors
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=100 * 1024 * 1024,  # 100MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.WARNING)
    file_handler.setFormatter(formatter)
    error_logger.addHandler(file_handler)
    
    # Prevent propagation to avoid double logging
    error_logger.propagate = False
    
    return error_logger


def log_system_info(logger: logging.Logger):
    """Log system information at startup."""
    import sys
    import platform
    import psutil
    
    logger.info("="*60)
    logger.info("PUBCHEM DOWNLOADER STARTUP")
    logger.info("="*60)
    logger.info(f"Python Version: {sys.version}")
    logger.info(f"Platform: {platform.platform()}")
    logger.info(f"Architecture: {platform.architecture()}")
    logger.info(f"Processor: {platform.processor()}")
    logger.info(f"CPU Count: {psutil.cpu_count()}")
    
    # Memory information
    memory = psutil.virtual_memory()
    logger.info(f"Total Memory: {memory.total / (1024**3):.2f} GB")
    logger.info(f"Available Memory: {memory.available / (1024**3):.2f} GB")
    
    # Disk information for current directory
    disk = psutil.disk_usage('.')
    logger.info(f"Disk Total: {disk.total / (1024**3):.2f} GB")
    logger.info(f"Disk Free: {disk.free / (1024**3):.2f} GB")
    
    logger.info("="*60)


def configure_library_loggers():
    """Configure logging levels for third-party libraries."""
    # Reduce verbosity of third-party libraries
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('ftplib').setLevel(logging.WARNING)
    logging.getLogger('rdflib').setLevel(logging.WARNING)
    
    # Keep our loggers at appropriate levels
    logging.getLogger('pubchem_downloader').setLevel(logging.INFO)
    logging.getLogger('pubchem_progress').setLevel(logging.INFO)
    logging.getLogger('pubchem_errors').setLevel(logging.WARNING)


class ProgressLogHandler:
    """Custom handler for logging download progress."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.last_log_time = 0
        self.log_interval = 60  # Log progress every 60 seconds
    
    def log_progress(self, message: str, force: bool = False):
        """Log progress message with time-based throttling."""
        import time
        
        current_time = time.time()
        if force or (current_time - self.last_log_time) >= self.log_interval:
            self.logger.info(message)
            self.last_log_time = current_time
    
    def log_file_completed(self, remote_path: str, file_size: int, duration: float):
        """Log completed file download."""
        size_mb = file_size / (1024 * 1024)
        speed_mbps = size_mb / duration if duration > 0 else 0
        
        self.logger.info(
            f"COMPLETED: {remote_path} ({size_mb:.2f} MB) "
            f"in {duration:.2f}s ({speed_mbps:.2f} MB/s)"
        )
    
    def log_file_failed(self, remote_path: str, error: str):
        """Log failed file download."""
        self.logger.error(f"FAILED: {remote_path} - {error}")
    
    def log_directory_started(self, remote_dir: str, file_count: int, total_size: int):
        """Log directory download start."""
        size_gb = total_size / (1024**3)
        self.logger.info(
            f"DIRECTORY_START: {remote_dir} "
            f"({file_count:,} files, {size_gb:.2f} GB)"
        )
    
    def log_directory_completed(self, remote_dir: str, completed: int, failed: int, duration: float):
        """Log directory download completion."""
        self.logger.info(
            f"DIRECTORY_COMPLETE: {remote_dir} "
            f"(Completed: {completed:,}, Failed: {failed:,}) "
            f"in {duration/3600:.2f} hours"
        )


def cleanup_old_logs(log_directory: str = "logs", max_age_days: int = 30):
    """Clean up old log files."""
    import time
    
    if not os.path.exists(log_directory):
        return
    
    current_time = time.time()
    max_age_seconds = max_age_days * 24 * 3600
    
    logger = logging.getLogger('pubchem_downloader')
    cleaned_count = 0
    
    for filename in os.listdir(log_directory):
        if filename.endswith('.log') or filename.endswith('.log.1'):
            filepath = os.path.join(log_directory, filename)
            try:
                file_age = current_time - os.path.getmtime(filepath)
                if file_age > max_age_seconds:
                    os.remove(filepath)
                    cleaned_count += 1
                    logger.debug(f"Removed old log file: {filename}")
            except Exception as e:
                logger.warning(f"Failed to clean up log file {filename}: {e}")
    
    if cleaned_count > 0:
        logger.info(f"Cleaned up {cleaned_count} old log files") 