from loguru import logger
import os
import sys
from datetime import datetime

class Logger:
    def __init__(self):
        # Use absolute path to logs directory from project root
        log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "logs"))
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, f"data_entry_{datetime.now().strftime('%Y%m%d')}.log")
        
        logger.remove()  # Remove default handler
        
        # Add console handler
        logger.add(sys.stderr, level="INFO", format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")
        
        # Add file handler
        logger.add(log_file, rotation="5 MB", level="DEBUG", format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}")
    
    def get_logger(self):
        return logger

log_manager = Logger()
app_logger = log_manager.get_logger()