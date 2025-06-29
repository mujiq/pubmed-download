# PubChem Knowledge Graph Downloader

A robust Python application for downloading and processing PubChem RDF data with resumable downloads, rate limiting, disk space monitoring, and idempotent operations.

## Features

### Core Capabilities
- **Resumable Downloads**: Automatically resume interrupted downloads from where they left off
- **Rate Limiting**: Intelligent rate limiting with adaptive backoff to be respectful to PubChem servers
- **Disk Space Monitoring**: Continuous monitoring of available disk space with automatic cleanup
- **Idempotent Operations**: Skip already downloaded files and continue from previous sessions
- **Concurrent Downloads**: Multi-threaded downloading with configurable concurrency
- **Comprehensive Logging**: Detailed logging with rotation and error tracking
- **Progress Tracking**: Real-time progress tracking with persistent state

### Advanced Features
- **Configuration Management**: YAML-based configuration with environment variable overrides
- **Error Recovery**: Automatic retry with exponential backoff for failed downloads
- **File Validation**: Verify file integrity and handle corrupted downloads
- **Status Monitoring**: Real-time status monitoring and reporting
- **Signal Handling**: Graceful shutdown on interruption with state preservation

## Project Structure

```
pubchem-downloader/
├── main.py                     # Main CLI application
├── requirements.txt            # Python dependencies
├── README.md                  # This file
├── config/
│   └── config.yaml            # Configuration file
├── src/
│   ├── __init__.py
│   ├── downloader/
│   │   ├── __init__.py
│   │   └── ftp_downloader.py  # Main FTP downloader
│   └── utils/
│       ├── __init__.py
│       ├── config_manager.py   # Configuration management
│       ├── disk_monitor.py     # Disk space monitoring
│       ├── rate_limiter.py     # Rate limiting utilities
│       ├── progress_tracker.py # Progress tracking
│       └── logging_setup.py    # Logging configuration
├── data/                      # Downloaded data (created automatically)
├── logs/                      # Log files (created automatically)
└── project-specs/
    └── 01-downloadpubchem.md  # Original specifications
```

## Installation

### Prerequisites
- Python 3.8 or higher
- At least 500GB of free disk space (PubChem RDF data is massive)
- Stable internet connection
- Administrative privileges (for creating directories)

### Setup

1. **Clone or download the project**:
   ```bash
   git clone <repository-url>
   cd pubchem-downloader
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Create default configuration**:
   ```bash
   python main.py --create-config
   ```

4. **Review and customize configuration**:
   ```bash
   # Edit config/config.yaml as needed
   notepad config/config.yaml  # Windows
   nano config/config.yaml     # Linux/Mac
   ```

## Configuration

The application uses a YAML configuration file (`config/config.yaml`) with the following structure:

### FTP Settings
```yaml
ftp:
  host: "ftp.ncbi.nlm.nih.gov"
  base_path: "/pubchem/RDF/"
  timeout: 30
  retries: 3
```

### Download Settings
```yaml
download:
  local_data_dir: "data/pubchem_rdf"
  temp_dir: "data/temp"
  max_concurrent_downloads: 3
  rate_limit_delay: 2.0  # seconds between downloads
  chunk_size: 8192
  resume_downloads: true
```

### Storage Management
```yaml
storage:
  min_free_space_gb: 50  # Minimum free space required
  check_space_interval: 60  # Check interval in seconds
  cleanup_temp_files: true
```

### Directory Selection
```yaml
directories_to_download:
  - "compound"
  - "substance"
  - "bioassay"
  - "protein"
  - "gene"
  - "taxonomy"
  - "pathway"
```

### Environment Variable Overrides

You can override configuration values using environment variables:

```bash
export PUBCHEM_DATA_DIR="/path/to/data"
export PUBCHEM_RATE_LIMIT=3.0
export PUBCHEM_MIN_SPACE=100
export PUBCHEM_MAX_CONCURRENT=5
export PUBCHEM_LOG_LEVEL=DEBUG
```

## Usage

### Basic Usage

1. **Start download with default configuration**:
   ```bash
   python main.py
   ```

2. **Use custom configuration file**:
   ```bash
   python main.py --config /path/to/custom/config.yaml
   ```

3. **Download specific directories only**:
   ```bash
   python main.py --directories compound substance
   ```

### Advanced Usage

4. **Check download status**:
   ```bash
   python main.py --status
   ```

5. **Retry failed downloads**:
   ```bash
   python main.py --retry-failed
   ```

6. **Override configuration via command line**:
   ```bash
   python main.py --max-concurrent 5 --rate-limit 1.5 --min-space 100
   ```

7. **Run with debug logging**:
   ```bash
   python main.py --log-level DEBUG
   ```

8. **Clean up old logs before starting**:
   ```bash
   python main.py --cleanup-logs
   ```

### Command Line Options

```bash
Options:
  -h, --help            Show help message and exit
  --config CONFIG       Path to configuration file
  --create-config       Create a default configuration file
  --retry-failed        Retry previously failed downloads
  --status              Show current download status
  --directories DIR [DIR ...]
                        Specific directories to download
  --max-concurrent N    Maximum concurrent downloads
  --rate-limit SECONDS  Rate limit delay in seconds
  --min-space GB        Minimum free space in GB
  --log-level LEVEL     Set logging level (DEBUG, INFO, WARNING, ERROR)
  --data-dir PATH       Local data directory
  --cleanup-logs        Clean up old log files before starting
  --version             Show version information
```

## Features in Detail

### Resumable Downloads
- Automatically detects partially downloaded files
- Resumes from the exact byte where download was interrupted
- Validates file integrity after completion
- Handles corrupt partial files by restarting download

### Rate Limiting
- Adaptive rate limiting that responds to server conditions
- Exponential backoff on errors
- Configurable base delay between requests
- Respects server response times and adjusts accordingly

### Disk Space Monitoring
- Continuous monitoring of available disk space
- Automatic cleanup of temporary files when space is low
- Prevents downloads when insufficient space is available
- Configurable minimum free space threshold

### Progress Tracking
- Persistent progress tracking across sessions
- Real-time statistics and completion rates
- Detailed logging of completed, failed, and skipped files
- JSON-based progress state for easy inspection

### Error Handling
- Comprehensive error recovery mechanisms
- Automatic retry with exponential backoff
- Detailed error logging and categorization
- Graceful handling of network interruptions

## Monitoring and Logging

### Log Files
- **Main Log**: `logs/pubchem_downloader.log` - General application logs
- **Progress Log**: `logs/download_progress.log` - Download progress tracking
- **Error Log**: `logs/download_errors.log` - Detailed error information

### Progress Files
- **Progress State**: `data/download_progress.json` - Resumable progress state
- **Statistics**: Real-time statistics available via `--status` command

### Log Rotation
- Automatic log rotation when files exceed configured size
- Configurable number of backup files to keep
- Automatic cleanup of old log files

## Troubleshooting

### Common Issues

1. **Insufficient Disk Space**:
   ```bash
   # Check available space
   python main.py --status
   
   # Reduce minimum space requirement
   python main.py --min-space 25
   ```

2. **Connection Timeout Issues**:
   ```bash
   # Increase rate limiting delay
   python main.py --rate-limit 5.0
   
   # Reduce concurrent downloads
   python main.py --max-concurrent 1
   ```

3. **Too Many Failed Downloads**:
   ```bash
   # Retry failed downloads
   python main.py --retry-failed
   
   # Check network connectivity and server status
   ```

4. **Configuration Issues**:
   ```bash
   # Recreate default configuration
   python main.py --create-config --config config/new-config.yaml
   ```

### Debug Mode
Enable debug logging for detailed troubleshooting:
```bash
python main.py --log-level DEBUG
```

### Performance Tuning

For faster downloads (use carefully):
```bash
python main.py --max-concurrent 8 --rate-limit 1.0
```

For conservative/slower downloads:
```bash
python main.py --max-concurrent 1 --rate-limit 5.0
```

## Data Organization

Downloaded files are organized as follows:
```
data/pubchem_rdf/
├── compound/           # Compound RDF files
├── substance/          # Substance RDF files
├── bioassay/          # Bioassay RDF files
├── protein/           # Protein RDF files
├── gene/              # Gene RDF files
├── taxonomy/          # Taxonomy RDF files
└── pathway/           # Pathway RDF files
```

Each directory contains `.ttl` (Turtle) RDF files, often compressed as `.ttl.gz`.

## Integration with Triple Stores

After downloading, you can load the RDF data into triple stores like:
- Apache Jena Fuseki
- GraphDB
- Blazegraph
- Virtuoso

Example for Fuseki:
```bash
# Load data into Fuseki
fuseki-server --update --mem /pubchem

# Bulk load RDF files
s-put http://localhost:3030/pubchem/data default data/pubchem_rdf/compound/*.ttl
```

## System Requirements

### Minimum Requirements
- **CPU**: 2 cores
- **RAM**: 4GB
- **Disk**: 500GB free space
- **Network**: Broadband internet connection

### Recommended Requirements
- **CPU**: 4+ cores
- **RAM**: 8GB+
- **Disk**: 1TB+ free space (SSD preferred)
- **Network**: High-speed broadband with unlimited data

## Performance Expectations

Download times vary significantly based on:
- Internet connection speed
- Server load
- Number of concurrent downloads
- Rate limiting settings

Typical expectations:
- **Complete dataset**: 1-7 days (depending on connection)
- **Single directory**: 2-24 hours
- **File sizes**: Range from KB to several GB each
- **Total size**: 500GB+ for complete dataset

## Contributing

### Development Setup
1. Install development dependencies
2. Follow PEP 8 style guidelines
3. Add type hints for new functions
4. Update documentation for new features
5. Test with various configurations

### Adding New Features
- Follow the existing code structure
- Add appropriate logging
- Include error handling
- Update configuration schema if needed
- Document new features in README

## License

This project is provided for educational and research purposes. Please respect PubChem's terms of service and be considerate of their server resources.

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review log files for detailed error information
3. Verify configuration settings
4. Check PubChem server status
5. Ensure adequate system resources

## Acknowledgments

- **PubChem**: For providing comprehensive chemical and biological data
- **NCBI**: For maintaining the FTP infrastructure
- **RDFLib**: For RDF processing capabilities
- **Python Community**: For excellent libraries and tools 