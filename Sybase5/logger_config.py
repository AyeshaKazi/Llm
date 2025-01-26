import logging
import os
from logging.handlers import RotatingFileHandler

def setup_logger(log_file='app.log', log_level=logging.INFO):
    """
    Configure and set up a rotating file logger.
    
    Args:
        log_file (str): Path to the log file
        log_level (int): Logging level
    
    Returns:
        logging.Logger: Configured logger instance
    """
    # Ensure log directory exists
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # Create logger
    logger = logging.getLogger('DatabaseAnalyzer')
    logger.setLevel(log_level)
    
    # Create file handler with log rotation
    file_handler = RotatingFileHandler(
        log_file, 
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(log_level)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger
