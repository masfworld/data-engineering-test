import logging
import os

logging.getLogger(__name__)

def logging_conf():
    """
    Logging configuration
    """
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)

    # Configure logging
    log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # File handler (writes logs to app.log)
    file_handler = logging.FileHandler("logs/app.log")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(log_formatter)

    # Console handler (writes logs to the console)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(log_formatter)

    # Root logger
    logging.basicConfig(level=logging.INFO, handlers=[file_handler, console_handler])