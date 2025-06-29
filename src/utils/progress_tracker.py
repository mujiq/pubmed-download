"""
Progress tracking utilities for resumable PubChem downloads.
"""

import json
import os
import time
import logging
import threading
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass
class FileProgress:
    """Track progress for individual file downloads."""
    remote_path: str
    local_path: str
    size_bytes: Optional[int] = None
    downloaded_bytes: int = 0
    checksum: Optional[str] = None
    status: str = "pending"  # pending, downloading, completed, failed, skipped
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    error_message: Optional[str] = None
    retry_count: int = 0


@dataclass
class DirectoryProgress:
    """Track progress for directory downloads."""
    remote_path: str
    local_path: str
    total_files: int = 0
    completed_files: int = 0
    failed_files: int = 0
    skipped_files: int = 0
    total_bytes: int = 0
    downloaded_bytes: int = 0
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    status: str = "pending"  # pending, in_progress, completed, failed


class ProgressTracker:
    """Thread-safe progress tracker for resumable downloads."""
    
    def __init__(self, progress_file: str = "data/download_progress.json", 
                 save_interval: int = 10):
        """
        Initialize progress tracker.
        
        Args:
            progress_file: Path to save progress data
            save_interval: Save progress every N file updates
        """
        self.progress_file = progress_file
        self.save_interval = save_interval
        self.lock = threading.Lock()
        self.logger = logging.getLogger(__name__)
        
        # Progress data
        self.directories: Dict[str, DirectoryProgress] = {}
        self.files: Dict[str, FileProgress] = {}
        self.completed_files: Set[str] = set()
        self.failed_files: Set[str] = set()
        
        # Counters
        self.total_files_processed = 0
        self.files_since_last_save = 0
        self.session_start_time = time.time()
        
        # Load existing progress
        self.load_progress()
    
    def load_progress(self):
        """Load progress from file if it exists."""
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r') as f:
                    data = json.load(f)
                
                # Load directories
                for dir_path, dir_data in data.get('directories', {}).items():
                    self.directories[dir_path] = DirectoryProgress(**dir_data)
                
                # Load files
                for file_path, file_data in data.get('files', {}).items():
                    self.files[file_path] = FileProgress(**file_data)
                
                # Load completed and failed files sets
                self.completed_files = set(data.get('completed_files', []))
                self.failed_files = set(data.get('failed_files', []))
                
                self.logger.info(f"Loaded progress: {len(self.completed_files)} completed, "
                               f"{len(self.failed_files)} failed files")
            else:
                self.logger.info("No existing progress file found, starting fresh")
                
        except Exception as e:
            self.logger.error(f"Failed to load progress file: {e}")
            self.logger.info("Starting with empty progress")
    
    def save_progress(self, force: bool = False):
        """Save progress to file."""
        with self.lock:
            if not force and self.files_since_last_save < self.save_interval:
                return
            
            try:
                # Ensure directory exists
                Path(self.progress_file).parent.mkdir(parents=True, exist_ok=True)
                
                # Prepare data for serialization
                data = {
                    'directories': {
                        path: asdict(progress) for path, progress in self.directories.items()
                    },
                    'files': {
                        path: asdict(progress) for path, progress in self.files.items()
                    },
                    'completed_files': list(self.completed_files),
                    'failed_files': list(self.failed_files),
                    'session_start_time': self.session_start_time,
                    'last_save_time': time.time(),
                    'total_files_processed': self.total_files_processed
                }
                
                # Write to temporary file first
                temp_file = self.progress_file + '.tmp'
                with open(temp_file, 'w') as f:
                    json.dump(data, f, indent=2)
                
                # Atomic move
                os.replace(temp_file, self.progress_file)
                
                self.files_since_last_save = 0
                self.logger.debug(f"Progress saved to {self.progress_file}")
                
            except Exception as e:
                self.logger.error(f"Failed to save progress: {e}")
    
    def add_directory(self, remote_path: str, local_path: str, 
                     total_files: int = 0, total_bytes: int = 0):
        """Add a directory to track."""
        with self.lock:
            if remote_path not in self.directories:
                self.directories[remote_path] = DirectoryProgress(
                    remote_path=remote_path,
                    local_path=local_path,
                    total_files=total_files,
                    total_bytes=total_bytes,
                    start_time=time.time(),
                    status="in_progress"
                )
                self.logger.debug(f"Added directory to track: {remote_path}")
    
    def add_file(self, remote_path: str, local_path: str, 
                 size_bytes: Optional[int] = None):
        """Add a file to track."""
        with self.lock:
            if remote_path not in self.files:
                self.files[remote_path] = FileProgress(
                    remote_path=remote_path,
                    local_path=local_path,
                    size_bytes=size_bytes,
                    start_time=time.time()
                )
                self.logger.debug(f"Added file to track: {remote_path}")
    
    def is_file_completed(self, remote_path: str) -> bool:
        """Check if a file is already completed."""
        with self.lock:
            if remote_path in self.completed_files:
                return True
            
            if remote_path in self.files:
                file_progress = self.files[remote_path]
                if file_progress.status == "completed":
                    self.completed_files.add(remote_path)
                    return True
            
            return False
    
    def is_file_failed(self, remote_path: str) -> bool:
        """Check if a file has failed and should be retried."""
        with self.lock:
            return remote_path in self.failed_files
    
    def update_file_progress(self, remote_path: str, downloaded_bytes: int, 
                           status: str = "downloading"):
        """Update file download progress."""
        with self.lock:
            if remote_path in self.files:
                file_progress = self.files[remote_path]
                file_progress.downloaded_bytes = downloaded_bytes
                file_progress.status = status
                
                if status in ["completed", "failed", "skipped"]:
                    file_progress.end_time = time.time()
                    self.total_files_processed += 1
                    self.files_since_last_save += 1
                    
                    if status == "completed":
                        self.completed_files.add(remote_path)
                        self.failed_files.discard(remote_path)
                    elif status == "failed":
                        self.failed_files.add(remote_path)
                        file_progress.retry_count += 1
    
    def set_file_error(self, remote_path: str, error_message: str):
        """Set error message for a file."""
        with self.lock:
            if remote_path in self.files:
                self.files[remote_path].error_message = error_message
                self.files[remote_path].status = "failed"
                self.files[remote_path].end_time = time.time()
                self.failed_files.add(remote_path)
    
    def get_files_to_download(self, directory_files: List[str]) -> List[str]:
        """Get list of files that still need to be downloaded."""
        with self.lock:
            return [f for f in directory_files if not self.is_file_completed(f)]
    
    def get_failed_files(self, max_retries: int = 3) -> List[str]:
        """Get list of failed files that should be retried."""
        with self.lock:
            failed_to_retry = []
            for remote_path in self.failed_files:
                if remote_path in self.files:
                    file_progress = self.files[remote_path]
                    if file_progress.retry_count < max_retries:
                        failed_to_retry.append(remote_path)
            return failed_to_retry
    
    def get_statistics(self) -> dict:
        """Get download statistics."""
        with self.lock:
            total_files = len(self.files)
            completed_files = len(self.completed_files)
            failed_files = len(self.failed_files)
            in_progress_files = sum(1 for f in self.files.values() 
                                  if f.status == "downloading")
            
            total_bytes = sum(f.size_bytes or 0 for f in self.files.values())
            downloaded_bytes = sum(f.downloaded_bytes for f in self.files.values())
            
            session_duration = time.time() - self.session_start_time
            
            return {
                'total_files': total_files,
                'completed_files': completed_files,
                'failed_files': failed_files,
                'in_progress_files': in_progress_files,
                'pending_files': total_files - completed_files - failed_files - in_progress_files,
                'completion_rate': completed_files / total_files if total_files > 0 else 0,
                'total_bytes': total_bytes,
                'downloaded_bytes': downloaded_bytes,
                'download_rate': downloaded_bytes / total_bytes if total_bytes > 0 else 0,
                'session_duration': session_duration,
                'files_per_second': self.total_files_processed / session_duration if session_duration > 0 else 0
            }
    
    def print_statistics(self):
        """Print current download statistics."""
        stats = self.get_statistics()
        
        print(f"\n{'='*60}")
        print("DOWNLOAD PROGRESS STATISTICS")
        print(f"{'='*60}")
        print(f"Total Files: {stats['total_files']:,}")
        print(f"Completed: {stats['completed_files']:,} ({stats['completion_rate']:.1%})")
        print(f"Failed: {stats['failed_files']:,}")
        print(f"In Progress: {stats['in_progress_files']:,}")
        print(f"Pending: {stats['pending_files']:,}")
        
        if stats['total_bytes'] > 0:
            print(f"\nData Transfer:")
            print(f"Total Size: {stats['total_bytes'] / (1024**3):.2f} GB")
            print(f"Downloaded: {stats['downloaded_bytes'] / (1024**3):.2f} GB ({stats['download_rate']:.1%})")
        
        print(f"\nSession Duration: {stats['session_duration'] / 3600:.2f} hours")
        print(f"Processing Rate: {stats['files_per_second']:.2f} files/second")
        print(f"{'='*60}\n")
    
    def cleanup(self):
        """Clean up and save final progress."""
        self.save_progress(force=True)
        self.logger.info("Progress tracker cleanup completed") 