"""
Robust FTP downloader for PubChem RDF data with resumable downloads,
rate limiting, disk space monitoring, and idempotent operations.
"""

import os
import ftplib
import gzip
import time
import logging
import threading
from typing import List, Optional, Tuple, Set
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from retrying import retry

from ..utils.disk_monitor import DiskSpaceMonitor
from ..utils.rate_limiter import AdaptiveRateLimiter
from ..utils.progress_tracker import ProgressTracker


class PubChemFTPDownloader:
    """Robust FTP downloader for PubChem RDF data."""
    
    def __init__(self, config: dict):
        """Initialize the FTP downloader with configuration."""
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.disk_monitor = DiskSpaceMonitor(
            min_free_space_gb=config['storage']['min_free_space_gb'],
            check_interval=config['storage']['check_space_interval']
        )
        
        self.rate_limiter = AdaptiveRateLimiter(
            initial_delay=config['download']['rate_limit_delay'],
            min_delay=0.5,
            max_delay=30.0
        )
        
        self.progress_tracker = ProgressTracker(
            progress_file=config['progress']['progress_file'],
            save_interval=config['progress']['save_interval']
        )
        
        # Download settings
        self.ftp_host = config['ftp']['host']
        self.ftp_base_path = config['ftp']['base_path']
        self.local_data_dir = config['download']['local_data_dir']
        self.temp_dir = config['download']['temp_dir']
        self.max_concurrent_downloads = config['download']['max_concurrent_downloads']
        self.chunk_size = config['download']['chunk_size']
        self.max_retries = config['ftp']['retries']
        self.ftp_timeout = config['ftp']['timeout']
        
        # Directories to download
        self.directories_to_download = config['directories_to_download']
        
        # Thread safety
        self.download_lock = threading.Lock()
        self.stats_lock = threading.Lock()
        
        # Statistics
        self.session_stats = {
            'files_downloaded': 0,
            'bytes_downloaded': 0,
            'files_failed': 0,
            'files_skipped': 0,
            'errors': []
        }
        
        # Ensure directories exist
        Path(self.local_data_dir).mkdir(parents=True, exist_ok=True)
        Path(self.temp_dir).mkdir(parents=True, exist_ok=True)
    
    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000)
    def _create_ftp_connection(self) -> ftplib.FTP:
        """Create and return an FTP connection."""
        try:
            ftp = ftplib.FTP(timeout=self.ftp_timeout)
            ftp.connect(self.ftp_host)
            ftp.login()  # Anonymous login
            return ftp
        except Exception as e:
            self.logger.error(f"Failed to create FTP connection: {e}")
            raise
    
    def _get_directory_listing(self, ftp: ftplib.FTP, remote_path: str) -> List[Tuple[str, str, int]]:
        """
        Get directory listing with file sizes.
        
        Returns:
            List of tuples (filename, file_type, size_bytes)
        """
        try:
            ftp.cwd(remote_path)
            files = []
            
            def parse_line(line):
                parts = line.split()
                if len(parts) >= 9:
                    permissions = parts[0]
                    size_str = parts[4]
                    filename = ' '.join(parts[8:])
                    
                    # Skip . and .. entries
                    if filename in ['.', '..']:
                        return
                    
                    # Determine if it's a directory or file
                    is_directory = permissions.startswith('d')
                    file_type = 'directory' if is_directory else 'file'
                    
                    # Parse size (directories might not have meaningful size)
                    try:
                        size_bytes = int(size_str) if not is_directory else 0
                    except ValueError:
                        size_bytes = 0
                    
                    files.append((filename, file_type, size_bytes))
            
            ftp.retrlines('LIST', callback=parse_line)
            return files
            
        except Exception as e:
            self.logger.error(f"Failed to get directory listing for {remote_path}: {e}")
            return []
    
    def _download_file(self, ftp: ftplib.FTP, remote_path: str, local_path: str, 
                      file_size: Optional[int] = None) -> bool:
        """
        Download a single file with resume capability.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if file already exists and is complete
            if os.path.exists(local_path) and file_size:
                existing_size = os.path.getsize(local_path)
                if existing_size == file_size:
                    self.logger.debug(f"File already complete: {local_path}")
                    return True
                elif existing_size > file_size:
                    # File is larger than expected, remove it
                    os.remove(local_path)
                    self.logger.warning(f"Removed oversized file: {local_path}")
            
            # Create temporary file path
            temp_path = os.path.join(self.temp_dir, os.path.basename(local_path) + '.tmp')
            
            # Resume download if partial file exists
            start_byte = 0
            if os.path.exists(temp_path):
                start_byte = os.path.getsize(temp_path)
                self.logger.debug(f"Resuming download from byte {start_byte}: {remote_path}")
            
            # Open file for writing (append mode for resume)
            mode = 'ab' if start_byte > 0 else 'wb'
            
            with open(temp_path, mode) as f:
                def callback(data):
                    f.write(data)
                    # Update progress
                    current_size = f.tell()
                    self.progress_tracker.update_file_progress(
                        remote_path, current_size, "downloading"
                    )
                
                # Set up resume if needed
                if start_byte > 0:
                    ftp.sendcmd(f'REST {start_byte}')
                
                # Download the file
                ftp.retrbinary(f'RETR {remote_path}', callback, blocksize=self.chunk_size)
            
            # Verify file size if known
            if file_size:
                actual_size = os.path.getsize(temp_path)
                if actual_size != file_size:
                    raise Exception(f"File size mismatch: expected {file_size}, got {actual_size}")
            
            # Move completed file to final location
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            os.replace(temp_path, local_path)
            
            self.logger.info(f"Successfully downloaded: {remote_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to download {remote_path}: {e}")
            # Clean up temporary file on error
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass
            return False
    
    def _download_file_with_retry(self, remote_path: str, local_path: str, 
                                 file_size: Optional[int] = None) -> bool:
        """Download a file with retry logic and rate limiting."""
        # Check if already completed
        if self.progress_tracker.is_file_completed(remote_path):
            self.logger.debug(f"Skipping already completed file: {remote_path}")
            with self.stats_lock:
                self.session_stats['files_skipped'] += 1
            return True
        
        # Check disk space before download
        if not self.disk_monitor.has_sufficient_space(self.local_data_dir):
            self.logger.error("Insufficient disk space for download")
            return False
        
        # Apply rate limiting
        self.rate_limiter.wait()
        
        # Add file to progress tracker
        self.progress_tracker.add_file(remote_path, local_path, file_size)
        
        success = False
        for attempt in range(self.max_retries):
            try:
                # Create FTP connection for this download
                ftp = self._create_ftp_connection()
                
                try:
                    success = self._download_file(ftp, remote_path, local_path, file_size)
                    if success:
                        self.rate_limiter.on_success()
                        self.progress_tracker.update_file_progress(
                            remote_path, file_size or os.path.getsize(local_path), "completed"
                        )
                        with self.stats_lock:
                            self.session_stats['files_downloaded'] += 1
                            self.session_stats['bytes_downloaded'] += file_size or 0
                        break
                    else:
                        self.rate_limiter.on_error("download_failed")
                        
                finally:
                    ftp.quit()
                    
            except Exception as e:
                error_msg = f"Download attempt {attempt + 1} failed for {remote_path}: {e}"
                self.logger.warning(error_msg)
                self.rate_limiter.on_error("connection_error")
                
                if attempt < self.max_retries - 1:
                    sleep_time = 2 ** attempt  # Exponential backoff
                    time.sleep(sleep_time)
        
        if not success:
            self.progress_tracker.set_file_error(remote_path, f"Failed after {self.max_retries} attempts")
            with self.stats_lock:
                self.session_stats['files_failed'] += 1
                self.session_stats['errors'].append(remote_path)
        
        return success
    
    def _download_directory_files(self, remote_dir: str, local_dir: str, 
                                 files_to_download: List[Tuple[str, str, int]]) -> None:
        """Download files from a directory using thread pool."""
        if not files_to_download:
            self.logger.info(f"No files to download in {remote_dir}")
            return
        
        self.logger.info(f"Starting download of {len(files_to_download)} files from {remote_dir}")
        
        # Add directory to progress tracker
        total_bytes = sum(size for _, _, size in files_to_download)
        self.progress_tracker.add_directory(remote_dir, local_dir, len(files_to_download), total_bytes)
        
        # Use ThreadPoolExecutor for concurrent downloads
        with ThreadPoolExecutor(max_workers=self.max_concurrent_downloads) as executor:
            future_to_file = {}
            
            for filename, file_type, size_bytes in files_to_download:
                if file_type == 'file':
                    remote_file_path = f"{remote_dir}/{filename}"
                    local_file_path = os.path.join(local_dir, filename)
                    
                    future = executor.submit(
                        self._download_file_with_retry,
                        remote_file_path,
                        local_file_path,
                        size_bytes
                    )
                    future_to_file[future] = (remote_file_path, local_file_path)
            
            # Process completed downloads
            for future in as_completed(future_to_file):
                remote_path, local_path = future_to_file[future]
                try:
                    success = future.result()
                    if success:
                        self.logger.debug(f"Completed download: {remote_path}")
                    else:
                        self.logger.error(f"Failed download: {remote_path}")
                except Exception as e:
                    self.logger.error(f"Exception during download of {remote_path}: {e}")
        
        self.logger.info(f"Finished downloading files from {remote_dir}")
    
    def _process_directory(self, remote_dir: str) -> None:
        """Process a single directory for download."""
        try:
            local_dir = os.path.join(self.local_data_dir, os.path.basename(remote_dir))
            
            self.logger.info(f"Processing directory: {remote_dir}")
            
            # Get directory listing
            ftp = self._create_ftp_connection()
            try:
                files_info = self._get_directory_listing(ftp, remote_dir)
            finally:
                ftp.quit()
            
            if not files_info:
                self.logger.warning(f"No files found in {remote_dir}")
                return
            
            # Filter files that need to be downloaded
            files_to_download = []
            for filename, file_type, size_bytes in files_info:
                if file_type == 'file':
                    remote_file_path = f"{remote_dir}/{filename}"
                    if not self.progress_tracker.is_file_completed(remote_file_path):
                        files_to_download.append((filename, file_type, size_bytes))
            
            if files_to_download:
                self.logger.info(f"Found {len(files_to_download)} new files to download in {remote_dir}")
                self._download_directory_files(remote_dir, local_dir, files_to_download)
            else:
                self.logger.info(f"All files already downloaded in {remote_dir}")
                
        except Exception as e:
            self.logger.error(f"Failed to process directory {remote_dir}: {e}")
    
    def download_all(self) -> bool:
        """Download all specified directories."""
        try:
            self.logger.info("Starting PubChem RDF download")
            self.logger.info(f"Directories to download: {self.directories_to_download}")
            
            # Check initial disk space
            if not self.disk_monitor.has_sufficient_space(self.local_data_dir):
                self.logger.error("Insufficient disk space to start download")
                return False
            
            start_time = time.time()
            
            # Process each directory
            for directory in self.directories_to_download:
                remote_dir = f"{self.ftp_base_path.rstrip('/')}/{directory}"
                
                # Check disk space before each directory
                if not self.disk_monitor.has_sufficient_space(self.local_data_dir):
                    self.logger.error(f"Insufficient disk space before downloading {directory}")
                    # Try cleaning up temp files
                    freed_space = self.disk_monitor.cleanup_temp_files(self.temp_dir)
                    if freed_space > 0:
                        self.logger.info(f"Freed {freed_space:.2f} GB of temp space")
                        if not self.disk_monitor.has_sufficient_space(self.local_data_dir):
                            self.logger.error("Still insufficient disk space after cleanup")
                            break
                    else:
                        break
                
                self._process_directory(remote_dir)
                
                # Print periodic progress
                self.progress_tracker.print_statistics()
            
            # Final statistics
            duration = time.time() - start_time
            self.logger.info(f"Download session completed in {duration/3600:.2f} hours")
            
            # Print final statistics
            self.progress_tracker.print_statistics()
            self._print_session_stats()
            
            return True
            
        except KeyboardInterrupt:
            self.logger.info("Download interrupted by user")
            return False
        except Exception as e:
            self.logger.error(f"Download failed: {e}")
            return False
        finally:
            # Clean up
            self.progress_tracker.cleanup()
            if self.config['storage']['cleanup_temp_files']:
                self.disk_monitor.cleanup_temp_files(self.temp_dir)
    
    def _print_session_stats(self):
        """Print session statistics."""
        print(f"\n{'='*60}")
        print("SESSION STATISTICS")
        print(f"{'='*60}")
        print(f"Files Downloaded: {self.session_stats['files_downloaded']:,}")
        print(f"Files Failed: {self.session_stats['files_failed']:,}")
        print(f"Files Skipped: {self.session_stats['files_skipped']:,}")
        print(f"Bytes Downloaded: {self.session_stats['bytes_downloaded'] / (1024**3):.2f} GB")
        
        if self.session_stats['errors']:
            print(f"\nFailed Files ({len(self.session_stats['errors'])}):")
            for error in self.session_stats['errors'][:10]:  # Show first 10
                print(f"  - {error}")
            if len(self.session_stats['errors']) > 10:
                print(f"  ... and {len(self.session_stats['errors']) - 10} more")
        
        print(f"{'='*60}\n")
    
    def retry_failed_downloads(self, max_retries: int = 3) -> bool:
        """Retry failed downloads."""
        failed_files = self.progress_tracker.get_failed_files(max_retries)
        
        if not failed_files:
            self.logger.info("No failed files to retry")
            return True
        
        self.logger.info(f"Retrying {len(failed_files)} failed downloads")
        
        success_count = 0
        for remote_path in failed_files:
            if remote_path in self.progress_tracker.files:
                file_progress = self.progress_tracker.files[remote_path]
                local_path = file_progress.local_path
                file_size = file_progress.size_bytes
                
                if self._download_file_with_retry(remote_path, local_path, file_size):
                    success_count += 1
        
        self.logger.info(f"Successfully retried {success_count}/{len(failed_files)} files")
        return success_count == len(failed_files)
    
    def get_download_status(self) -> dict:
        """Get current download status."""
        stats = self.progress_tracker.get_statistics()
        disk_info = self.disk_monitor.get_disk_usage_info(self.local_data_dir)
        rate_stats = self.rate_limiter.get_stats()
        
        return {
            'progress': stats,
            'disk_usage': disk_info,
            'rate_limiter': rate_stats,
            'session_stats': self.session_stats
        } 