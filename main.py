#!/usr/bin/env python3
"""
PubChem Knowledge Graph Downloader

Main entry point for downloading PubChem RDF data with resumable downloads,
rate limiting, disk space monitoring, and idempotent operations.
"""

import sys
import signal
import argparse
from pathlib import Path

from src.utils.config_manager import ConfigManager
from src.utils.logging_setup import (
    setup_logging, 
    configure_library_loggers, 
    log_system_info,
    cleanup_old_logs
)
from src.downloader.ftp_downloader import PubChemFTPDownloader


def signal_handler(signum, frame):
    """Handle interrupt signals gracefully."""
    print(f"\nReceived signal {signum}. Shutting down gracefully...")
    sys.exit(0)


def create_argument_parser():
    """Create and configure argument parser."""
    parser = argparse.ArgumentParser(
        description="PubChem Knowledge Graph Downloader",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          # Start download with default config
  %(prog)s --config custom.yaml     # Use custom configuration
  %(prog)s --retry-failed           # Retry previously failed downloads
  %(prog)s --status                 # Show current download status
  %(prog)s --create-config          # Create default configuration file
  %(prog)s --directories compound substance  # Download specific directories
        """
    )
    
    parser.add_argument(
        '--config', '-c',
        type=str,
        default='config/config.yaml',
        help='Path to configuration file (default: config/config.yaml)'
    )
    
    parser.add_argument(
        '--create-config',
        action='store_true',
        help='Create a default configuration file'
    )
    
    parser.add_argument(
        '--retry-failed',
        action='store_true',
        help='Retry previously failed downloads'
    )
    
    parser.add_argument(
        '--status',
        action='store_true',
        help='Show current download status and exit'
    )
    
    parser.add_argument(
        '--directories',
        nargs='+',
        help='Specific directories to download (overrides config)'
    )
    
    parser.add_argument(
        '--max-concurrent',
        type=int,
        help='Maximum concurrent downloads (overrides config)'
    )
    
    parser.add_argument(
        '--rate-limit',
        type=float,
        help='Rate limit delay in seconds (overrides config)'
    )
    
    parser.add_argument(
        '--min-space',
        type=float,
        help='Minimum free space in GB (overrides config)'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Set logging level (overrides config)'
    )
    
    parser.add_argument(
        '--data-dir',
        type=str,
        help='Local data directory (overrides config)'
    )
    
    parser.add_argument(
        '--cleanup-logs',
        action='store_true',
        help='Clean up old log files before starting'
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version='PubChem Downloader 1.0.0'
    )
    
    return parser


def apply_cli_overrides(config, args):
    """Apply command line argument overrides to configuration."""
    if args.directories:
        config['directories_to_download'] = args.directories
    
    if args.max_concurrent:
        config['download']['max_concurrent_downloads'] = args.max_concurrent
    
    if args.rate_limit:
        config['download']['rate_limit_delay'] = args.rate_limit
    
    if args.min_space:
        config['storage']['min_free_space_gb'] = args.min_space
    
    if args.log_level:
        config['logging']['level'] = args.log_level
    
    if args.data_dir:
        config['download']['local_data_dir'] = args.data_dir


def show_download_status(config):
    """Show current download status."""
    try:
        downloader = PubChemFTPDownloader(config)
        status = downloader.get_download_status()
        
        print("\n" + "="*60)
        print("DOWNLOAD STATUS")
        print("="*60)
        
        # Progress information
        progress = status['progress']
        print(f"Total Files: {progress['total_files']:,}")
        print(f"Completed: {progress['completed_files']:,} ({progress['completion_rate']:.1%})")
        print(f"Failed: {progress['failed_files']:,}")
        print(f"In Progress: {progress['in_progress_files']:,}")
        print(f"Pending: {progress['pending_files']:,}")
        
        if progress['total_bytes'] > 0:
            print(f"\nData Transfer:")
            print(f"Total Size: {progress['total_bytes'] / (1024**3):.2f} GB")
            print(f"Downloaded: {progress['downloaded_bytes'] / (1024**3):.2f} GB")
            print(f"Progress: {progress['download_rate']:.1%}")
        
        # Disk usage
        disk = status['disk_usage']
        if disk:
            print(f"\nDisk Usage:")
            print(f"Total: {disk['total_gb']:.2f} GB")
            print(f"Used: {disk['used_gb']:.2f} GB ({disk['percent_used']:.1f}%)")
            print(f"Free: {disk['free_gb']:.2f} GB ({disk['percent_free']:.1f}%)")
            print(f"Sufficient Space: {'Yes' if disk['sufficient_space'] else 'No'}")
        
        # Session stats
        session = status['session_stats']
        print(f"\nSession Statistics:")
        print(f"Files Downloaded: {session['files_downloaded']:,}")
        print(f"Files Failed: {session['files_failed']:,}")
        print(f"Files Skipped: {session['files_skipped']:,}")
        print(f"Data Downloaded: {session['bytes_downloaded'] / (1024**3):.2f} GB")
        
        print("="*60)
        
    except Exception as e:
        print(f"Error getting download status: {e}")
        return False
    
    return True


def main():
    """Main application entry point."""
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Parse command line arguments
    parser = create_argument_parser()
    args = parser.parse_args()
    
    try:
        # Create default config if requested
        if args.create_config:
            config_manager = ConfigManager()
            config_manager.create_default_config(args.config)
            print(f"Default configuration created at: {args.config}")
            print("Please review and modify the configuration as needed.")
            return 0
        
        # Load configuration
        config_manager = ConfigManager(args.config)
        
        try:
            config = config_manager.load_config()
        except FileNotFoundError:
            print(f"Configuration file not found: {args.config}")
            print(f"Create one with: {sys.argv[0]} --create-config")
            return 1
        except Exception as e:
            print(f"Failed to load configuration: {e}")
            return 1
        
        # Apply CLI overrides
        apply_cli_overrides(config, args)
        
        # Set up logging
        logger = setup_logging(config)
        configure_library_loggers()
        
        # Clean up old logs if requested
        if args.cleanup_logs:
            cleanup_old_logs()
        
        # Log system information
        log_system_info(logger)
        
        # Show configuration
        if logger.isEnabledFor('DEBUG'):
            config_manager.print_config()
        
        # Handle status request
        if args.status:
            return 0 if show_download_status(config) else 1
        
        # Create downloader
        downloader = PubChemFTPDownloader(config)
        
        # Handle retry failed downloads
        if args.retry_failed:
            logger.info("Starting retry of failed downloads")
            success = downloader.retry_failed_downloads()
            if success:
                logger.info("All failed downloads retried successfully")
                return 0
            else:
                logger.error("Some downloads still failed after retry")
                return 1
        
        # Start main download process
        logger.info("Starting PubChem RDF download process")
        success = downloader.download_all()
        
        if success:
            logger.info("Download process completed successfully")
            return 0
        else:
            logger.error("Download process failed")
            return 1
    
    except KeyboardInterrupt:
        print("\nDownload interrupted by user")
        return 130  # Standard exit code for SIGINT
    
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main()) 