"""
Configuration management utilities for PubChem downloader.
"""

import os
import yaml
import logging
from typing import Dict, Any, Optional
from pathlib import Path


class ConfigManager:
    """Manage configuration loading and validation."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize configuration manager."""
        self.config_path = config_path
        self.config = {}
        self.logger = logging.getLogger(__name__)
    
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from file."""
        try:
            if not os.path.exists(self.config_path):
                raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
            
            with open(self.config_path, 'r') as f:
                self.config = yaml.safe_load(f)
            
            # Validate configuration
            self._validate_config()
            
            # Apply environment variable overrides
            self._apply_env_overrides()
            
            self.logger.info(f"Configuration loaded from {self.config_path}")
            return self.config
            
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {e}")
            raise
    
    def _validate_config(self):
        """Validate configuration structure and values."""
        required_sections = ['ftp', 'download', 'storage', 'logging', 'progress']
        
        for section in required_sections:
            if section not in self.config:
                raise ValueError(f"Missing required configuration section: {section}")
        
        # Validate FTP configuration
        ftp_config = self.config['ftp']
        required_ftp_keys = ['host', 'base_path', 'timeout', 'retries']
        for key in required_ftp_keys:
            if key not in ftp_config:
                raise ValueError(f"Missing required FTP configuration: {key}")
        
        # Validate download configuration
        download_config = self.config['download']
        required_download_keys = ['local_data_dir', 'temp_dir', 'rate_limit_delay']
        for key in required_download_keys:
            if key not in download_config:
                raise ValueError(f"Missing required download configuration: {key}")
        
        # Validate storage configuration
        storage_config = self.config['storage']
        if 'min_free_space_gb' not in storage_config:
            raise ValueError("Missing required storage configuration: min_free_space_gb")
        
        # Validate directories to download
        if 'directories_to_download' not in self.config:
            raise ValueError("Missing required configuration: directories_to_download")
        
        if not isinstance(self.config['directories_to_download'], list):
            raise ValueError("directories_to_download must be a list")
        
        # Validate numeric values
        numeric_validations = [
            ('ftp.timeout', int, 1, 300),
            ('ftp.retries', int, 1, 10),
            ('download.rate_limit_delay', float, 0.1, 60.0),
            ('download.max_concurrent_downloads', int, 1, 20),
            ('storage.min_free_space_gb', float, 1.0, 10000.0),
            ('progress.save_interval', int, 1, 1000)
        ]
        
        for path, data_type, min_val, max_val in numeric_validations:
            value = self._get_nested_value(path)
            if value is not None:
                if not isinstance(value, (int, float)):
                    raise TypeError(f"Configuration {path} must be numeric")
                if value < min_val or value > max_val:
                    raise ValueError(f"Configuration {path} must be between {min_val} and {max_val}")
    
    def _get_nested_value(self, path: str) -> Any:
        """Get value from nested configuration path."""
        keys = path.split('.')
        value = self.config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
        
        return value
    
    def _apply_env_overrides(self):
        """Apply environment variable overrides to configuration."""
        env_mappings = {
            'PUBCHEM_FTP_HOST': 'ftp.host',
            'PUBCHEM_DATA_DIR': 'download.local_data_dir',
            'PUBCHEM_TEMP_DIR': 'download.temp_dir',
            'PUBCHEM_RATE_LIMIT': 'download.rate_limit_delay',
            'PUBCHEM_MIN_SPACE': 'storage.min_free_space_gb',
            'PUBCHEM_LOG_LEVEL': 'logging.level',
            'PUBCHEM_MAX_CONCURRENT': 'download.max_concurrent_downloads'
        }
        
        for env_var, config_path in env_mappings.items():
            env_value = os.getenv(env_var)
            if env_value is not None:
                self._set_nested_value(config_path, env_value)
                self.logger.info(f"Applied environment override: {env_var} -> {config_path}")
    
    def _set_nested_value(self, path: str, value: Any):
        """Set value in nested configuration path."""
        keys = path.split('.')
        config = self.config
        
        # Navigate to the parent of the target key
        for key in keys[:-1]:
            if key not in config:
                config[key] = {}
            config = config[key]
        
        # Set the final value, converting type if necessary
        final_key = keys[-1]
        
        # Try to convert to appropriate type based on existing value
        if final_key in config:
            existing_value = config[final_key]
            if isinstance(existing_value, bool):
                value = str(value).lower() in ('true', '1', 'yes', 'on')
            elif isinstance(existing_value, int):
                value = int(value)
            elif isinstance(existing_value, float):
                value = float(value)
        
        config[final_key] = value
    
    def get_config(self) -> Dict[str, Any]:
        """Get the current configuration."""
        return self.config
    
    def get_section(self, section: str) -> Dict[str, Any]:
        """Get a specific configuration section."""
        return self.config.get(section, {})
    
    def get_value(self, path: str, default: Any = None) -> Any:
        """Get a specific configuration value by path."""
        value = self._get_nested_value(path)
        return value if value is not None else default
    
    def save_config(self, output_path: Optional[str] = None):
        """Save current configuration to file."""
        output_path = output_path or self.config_path
        
        try:
            # Ensure directory exists
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w') as f:
                yaml.dump(self.config, f, default_flow_style=False, indent=2)
            
            self.logger.info(f"Configuration saved to {output_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to save configuration: {e}")
            raise
    
    def print_config(self):
        """Print current configuration in a readable format."""
        print("\n" + "="*60)
        print("CURRENT CONFIGURATION")
        print("="*60)
        
        def print_section(data, indent=0):
            for key, value in data.items():
                if isinstance(value, dict):
                    print("  " * indent + f"{key}:")
                    print_section(value, indent + 1)
                else:
                    print("  " * indent + f"{key}: {value}")
        
        print_section(self.config)
        print("="*60 + "\n")
    
    def create_default_config(self, output_path: str):
        """Create a default configuration file."""
        default_config = {
            'ftp': {
                'host': 'ftp.ncbi.nlm.nih.gov',
                'base_path': '/pubchem/RDF/',
                'anonymous_login': True,
                'timeout': 30,
                'retries': 3
            },
            'download': {
                'local_data_dir': 'data/pubchem_rdf',
                'temp_dir': 'data/temp',
                'max_concurrent_downloads': 3,
                'rate_limit_delay': 2.0,
                'chunk_size': 8192,
                'resume_downloads': True
            },
            'storage': {
                'min_free_space_gb': 50,
                'check_space_interval': 60,
                'cleanup_temp_files': True,
                'compress_downloaded': False
            },
            'logging': {
                'level': 'INFO',
                'log_file': 'logs/pubchem_downloader.log',
                'log_format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                'max_log_size_mb': 100,
                'backup_count': 5
            },
            'progress': {
                'save_interval': 10,
                'progress_file': 'data/download_progress.json'
            },
            'rdf': {
                'parse_on_download': False,
                'validate_files': True,
                'supported_formats': ['ttl', 'turtle', 'rdf', 'xml']
            },
            'triple_store': {
                'type': 'fuseki',
                'endpoint': 'http://localhost:3030/pubchem',
                'bulk_load': True
            },
            'directories_to_download': [
                'compound',
                'substance',
                'bioassay',
                'protein',
                'gene',
                'taxonomy',
                'pathway'
            ]
        }
        
        try:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w') as f:
                yaml.dump(default_config, f, default_flow_style=False, indent=2)
            
            print(f"Default configuration created at: {output_path}")
            
        except Exception as e:
            print(f"Failed to create default configuration: {e}")
            raise 