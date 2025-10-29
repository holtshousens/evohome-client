import logging
import time
import configparser
from pathlib import Path
from typing import Dict, Optional
from contextlib import contextmanager
import pyodbc
from evohomeclient2 import EvohomeClient
import servicemanager
import win32serviceutil
import win32service
import win32event
import threading
import json
from datetime import datetime

# --------------------------
# Configuration
# --------------------------
PROJECT_LOCATION = Path.home() / "OneDrive/Documents/GitHub/evohome-client"
CONFIG_PATH = PROJECT_LOCATION / "config.ini"
LOG_FILE = PROJECT_LOCATION / "evohome_service.log"
STATE_FILE = PROJECT_LOCATION / "last_success.json"

# Constants
RETRY_DELAY = 60
POLL_INTERVAL = 300
MAX_RETRIES = 5
SLEEP_CHECK_INTERVAL = 1

# --------------------------
# Logging Setup
# --------------------------
def setup_logging():
    """Configure logging with rotation to prevent log file bloat."""
    from logging.handlers import RotatingFileHandler
    
    handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5
    )
    handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    
    return logger

logger = setup_logging()
logger.info("Starting Evohome service")

# --------------------------
# Configuration Loading
# --------------------------
def load_config(config_path: Path) -> configparser.ConfigParser:
    """Load and validate configuration file."""
    if not config_path.exists():
        error_msg = f"Config file not found at {config_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    config = configparser.ConfigParser()
    config.read(config_path)
    
    # Validate required sections and keys
    required_config = {
        'DB': ['dbname', 'dbserver', 'dbdriver'],
        'evohome': ['username', 'password']
    }
    
    for section, keys in required_config.items():
        if section not in config:
            raise ValueError(f"Missing required section: [{section}]")
        for key in keys:
            if key not in config[section]:
                raise ValueError(f"Missing required key: {key} in [{section}]")
    
    return config

config = load_config(CONFIG_PATH)

DB_NAME = str(config.get('DB', 'dbname')).strip()
DB_SERVER = str(config.get('DB', 'dbserver')).strip()
DB_DRIVER = str(config.get('DB', 'dbdriver')).strip()
USERNAME = str(config.get('evohome', 'username')).strip()
PASSWORD = str(config.get('evohome', 'password')).strip()

# Log configuration (without sensitive data)
logger.info(f"Config loaded - DB: {DB_NAME}, Server: {DB_SERVER}, User: {USERNAME[:3]}***")

CONNECTION_STRING = f"DRIVER={DB_DRIVER};SERVER={DB_SERVER};DATABASE={DB_NAME};Trusted_Connection=yes"

# --------------------------
# State Management
# --------------------------
def load_last_success() -> Dict[str, int]:
    """Load the last successful update timestamps."""
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, 'r') as f:
                data = json.load(f)
                logger.info(f"Loaded state for {len(data)} zones")
                return data
        except json.JSONDecodeError as e:
            logger.error(f"State file corrupted: {e}. Starting fresh.")
        except Exception as e:
            logger.error(f"Failed to read state file: {e}")
    return {}

def save_last_success(data: Dict[str, int]) -> None:
    """Save the last successful update timestamps atomically."""
    try:
        # Write to temporary file first for atomic operation
        temp_file = STATE_FILE.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        # Atomic rename
        temp_file.replace(STATE_FILE)
        logger.debug(f"Saved state for {len(data)} zones")
    except Exception as e:
        logger.error(f"Failed to save state file: {e}")

# --------------------------
# Database Operations
# --------------------------
@contextmanager
def get_db_cursor():
    """Context manager for database connections."""
    connection = None
    try:
        connection = pyodbc.connect(CONNECTION_STRING, timeout=30)
        cursor = connection.cursor()
        yield cursor
        connection.commit()
    except pyodbc.Error as e:
        if connection:
            connection.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        if connection:
            connection.close()

def insert_zone(cursor, thermostat: str, zone_id: str, name: str, 
                temp: Optional[float], setpoint: Optional[float], 
                timestamp: Optional[str] = None) -> bool:
    """Insert zone data into database with validation."""
    try:
        # Validate inputs
        if temp is None and setpoint is None:
            logger.warning(f"Skipping zone {zone_id}: No valid data")
            return False
        
        sSQL = """
            INSERT INTO dbo.Zones (uid, timestamp, thermostat, id, [name], temp, setpoint)
            VALUES (NEWID(), COALESCE(?, CURRENT_TIMESTAMP), ?, ?, ?, ?, ?)
        """
        cursor.execute(sSQL, timestamp, thermostat, zone_id, name, temp, setpoint)
        logger.info(f"Inserted zone: {name} (T:{thermostat}, ID:{zone_id}, "
                   f"Temp:{temp}°C, Setpoint:{setpoint}°C)")
        return True
    except pyodbc.Error as e:
        logger.error(f"Failed to insert zone {zone_id} ({name}): {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error inserting zone {zone_id}: {e}")
        return False

# --------------------------
# Evohome Client Management
# --------------------------
def connect_to_evohome(stop_event, max_retries: int = MAX_RETRIES) -> Optional[EvohomeClient]:
    """Connect to Evohome API with exponential backoff."""
    for attempt in range(max_retries):
        if stop_requested(stop_event):
            return None
        
        try:
            logger.info(f"Connecting to Evohome API (attempt {attempt + 1}/{max_retries})")
            logger.debug(f"Using username: {USERNAME[:3]}*** (length: {len(USERNAME)})")
            logger.debug(f"Password length: {len(PASSWORD)}")
            
            client = EvohomeClient(USERNAME, PASSWORD, debug=False)
            logger.info("Successfully connected to Evohome API")
            return client
        except TypeError as e:
            logger.error(f"Type error during connection (attempt {attempt + 1}): {e}", exc_info=True)
            logger.error(f"Username type: {type(USERNAME)}, Password type: {type(PASSWORD)}")
            # Don't retry on type errors - they won't fix themselves
            return None
        except Exception as e:
            logger.error(f"Evohome connection failed (attempt {attempt + 1}): {e}", exc_info=True)
            
            if attempt < max_retries - 1:
                # First retry is quick (10s), then exponential backoff
                if attempt == 0:
                    delay = 10
                else:
                    delay = RETRY_DELAY * (2 ** (attempt - 1))
                logger.info(f"Retrying in {delay} seconds...")
                
                if interruptible_sleep(stop_event, delay):
                    return None
    
    logger.critical(f"Failed to connect to Evohome API after {max_retries} attempts")
    return None

# --------------------------
# Data Collection
# --------------------------
def backfill(cursor, client: EvohomeClient, last_state: Dict[str, int]) -> int:
    """
    Collect and insert current temperature readings.
    Returns the number of zones successfully updated.
    """
    try:
        # Convert generator to list so we can check length and iterate
        devices = list(client.temperatures())
        
        if not devices:
            logger.warning("No devices returned from Evohome API")
            return 0
        
        logger.info(f"Retrieved {len(devices)} zones from Evohome")
        current_time = int(time.time())
        success_count = 0
        
        for device in devices:
            thermostat = device.get('thermostat')
            zone_id = device.get('id')
            name = device.get('name', 'Unknown')
            temp = device.get('temp')
            setpoint = device.get('setpoint')
            
            # Validate required fields
            if thermostat is None or zone_id is None:
                logger.warning(f"Skipping device with missing thermostat or id: {device}")
                continue
            
            key = f"{thermostat}_{zone_id}"
            last_timestamp = last_state.get(key)
            
            # Only insert if this is new or enough time has passed
            if last_timestamp is None or current_time > last_timestamp:
                if insert_zone(cursor, str(thermostat), str(zone_id), 
                             name, temp, setpoint):
                    last_state[key] = current_time
                    success_count += 1
        
        if success_count > 0:
            save_last_success(last_state)
            logger.info(f"Successfully updated {success_count} zones")
        
        return success_count
        
    except Exception as e:
        logger.error(f"Backfill failed: {e}", exc_info=True)
        return 0

# --------------------------
# Service Control Helpers
# --------------------------
def stop_requested(stop_event) -> bool:
    """Check if service stop has been requested."""
    if isinstance(stop_event, threading.Event):
        return stop_event.is_set()
    else:
        return win32event.WaitForSingleObject(stop_event, 0) == win32event.WAIT_OBJECT_0

def interruptible_sleep(stop_event, duration: int) -> bool:
    """
    Sleep for duration seconds, but check for stop every second.
    Returns True if stop was requested, False otherwise.
    """
    for _ in range(duration):
        if stop_requested(stop_event):
            logger.info("Stop requested during sleep")
            return True
        time.sleep(SLEEP_CHECK_INTERVAL)
    return False

# --------------------------
# Main Service Loop
# --------------------------
def main_loop(stop_event):
    """Main service loop with error recovery and graceful shutdown."""
    last_state = load_last_success()
    consecutive_failures = 0
    max_consecutive_failures = 3
    
    logger.info("Main loop started")
    
    while not stop_requested(stop_event):
        cycle_start = time.time()
        
        try:
            # Connect to database
            with get_db_cursor() as cursor:
                # Connect to Evohome API
                client = connect_to_evohome(stop_event)
                if client is None:
                    if stop_requested(stop_event):
                        break
                    consecutive_failures += 1
                    logger.error(f"Consecutive failures: {consecutive_failures}")
                    
                    if consecutive_failures >= max_consecutive_failures:
                        logger.critical("Too many consecutive failures, extended sleep")
                        if interruptible_sleep(stop_event, POLL_INTERVAL * 2):
                            break
                    else:
                        if interruptible_sleep(stop_event, POLL_INTERVAL):
                            break
                    continue
                
                # Collect and insert data
                success_count = backfill(cursor, client, last_state)
                
                if success_count > 0:
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
                    logger.warning("No zones updated this cycle")
        
        except pyodbc.Error as db_err:
            consecutive_failures += 1
            logger.error(f"Database error: {db_err}", exc_info=True)
        except Exception as e:
            consecutive_failures += 1
            logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
        
        # Calculate remaining sleep time
        cycle_duration = time.time() - cycle_start
        remaining_sleep = max(0, POLL_INTERVAL - cycle_duration)
        
        logger.info(f"Cycle completed in {cycle_duration:.1f}s. "
                   f"Sleeping for {remaining_sleep:.1f}s")
        
        if interruptible_sleep(stop_event, int(remaining_sleep)):
            break
    
    logger.info("Main loop exited gracefully")

# --------------------------
# Windows Service
# --------------------------
class EvohomeService(win32serviceutil.ServiceFramework):
    _svc_name_ = "EvohomeService"
    _svc_display_name_ = "Evohome Temperature Logger Service"
    _svc_description_ = "Logs Evohome thermostat data into SQL Server every 5 minutes"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)
        self.thread = None

    def SvcStop(self):
        """Handle service stop request."""
        logger.info("Service stop requested")
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.stop_event)
        
        if self.thread and self.thread.is_alive():
            logger.info("Waiting for main thread to finish...")
            self.thread.join(timeout=30)
            if self.thread.is_alive():
                logger.warning("Main thread did not stop gracefully")
        
        logger.info("Service stopped")

    def SvcDoRun(self):
        """Handle service start request."""
        try:
            self.ReportServiceStatus(win32service.SERVICE_RUNNING)
            servicemanager.LogMsg(
                servicemanager.EVENTLOG_INFORMATION_TYPE,
                servicemanager.PYS_SERVICE_STARTED,
                (self._svc_name_, "")
            )
            logger.info("Service started successfully")
            
            self.thread = threading.Thread(
                target=main_loop, 
                args=(self.stop_event,),
                name="EvohomeMainLoop"
            )
            self.thread.daemon = False
            self.thread.start()
        except Exception as e:
            logger.error(f"Service startup failed: {e}", exc_info=True)
            servicemanager.LogErrorMsg(f"Service startup failed: {e}")
            raise

# --------------------------
# Entry Point
# --------------------------
if __name__ == '__main__':
    import sys

    if len(sys.argv) == 1:
        # Direct run mode for testing
        print("=" * 60)
        print("Running in TEST MODE (not as Windows service)")
        print("Press Ctrl+C to stop")
        print("=" * 60)
        
        logger.info("Running in direct test mode")
        stop_event = threading.Event()
        
        try:
            main_loop(stop_event)
        except KeyboardInterrupt:
            print("\nStopping service...")
            logger.info("Stopping service via KeyboardInterrupt")
            stop_event.set()
            time.sleep(2)
            print("Service stopped")
    else:
        # Service mode
        win32serviceutil.HandleCommandLine(EvohomeService)