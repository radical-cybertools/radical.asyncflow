from __future__ import annotations

import logging
import pathlib
import sys
import threading
import time
from typing import Dict, Optional, Union


class _ColoredFormatter(logging.Formatter):
    """Custom formatter with enhanced color schemes and flexible formatting."""
    
    def __init__(self, use_colors: bool = True, show_details: bool = False, style: str = "modern") -> None:
        self.use_colors = use_colors
        self.show_details = show_details
        self.style = style
        
        # Enhanced color palette
        if use_colors:
            colors = {
                'grey': '\033[90m',      # Darker grey
                'green': '\033[92m',     # Bright green
                'cyan': '\033[96m',      # Bright cyan
                'blue': '\033[94m',      # Bright blue
                'yellow': '\033[93m',    # Bright yellow
                'red': '\033[91m',       # Bright red
                'magenta': '\033[95m',   # Bright magenta
                'white': '\033[97m',     # Bright white
                'purple': '\033[95m',    # Bright magenta
                'bright_purple': '\033[38;5;165m',  # Bright purple
                'bold': '\033[1m',       # Bold
                'reset': '\033[0m'       # Reset
            }
        else:
            colors = {k: '' for k in ['grey', 'green', 'cyan', 'blue', 'yellow', 'red', 'magenta', 'white', 'purple', 'bright_purple', 'bold', 'reset']}

        # Detail formatting
        if show_details:
            detail_info = f'{colors["green"]}[TID:{colors["white"]}%(thread_id)s{colors["green"]} PID:{colors["white"]}%(process)d{colors["green"]}]{colors["reset"]} '
        else:
            detail_info = ''

        # Style variations
        timestamp_fmt = f'{colors["grey"]}%(asctime)s.%(msecs)03d{colors["reset"]}'
        level_fmt = f'{colors["cyan"]}%(levelname)s{colors["reset"]}'  # Fixed: added color placeholder
        logger_fmt = f'{colors["bright_purple"]}[%(short_name)s]{colors["reset"]}'
        separator = ' │ '


        # Date format
        date_format = '%Y-%m-%d %H:%M:%S'
        
        # Build format string - Fixed: removed .format() call that was breaking the formatter
        if style == "core":
            base_format = f'{timestamp_fmt}{separator}{detail_info}{{level_color}}%(levelname)s{colors["reset"]}{separator}{logger_fmt}{separator}%(message)s'
        elif style == "execution_backend":
            base_format = f'{timestamp_fmt}{separator}{detail_info}{{level_color}}%(levelname)s{colors["reset"]}{separator}{logger_fmt}{separator}%(message)s'
        else:  # modern
            base_format = f'{timestamp_fmt}{separator}{detail_info}{{level_color}}%(levelname)s{colors["reset"]}{separator}{logger_fmt}{separator}%(message)s'
        
        # Level-specific formatters
        self.level_formatters = {
            logging.DEBUG: logging.Formatter(
                base_format.format(level_color=colors['cyan']), 
                datefmt=date_format
            ),
            logging.INFO: logging.Formatter(
                base_format.format(level_color=colors['blue']), 
                datefmt=date_format
            ),
            logging.WARNING: logging.Formatter(
                base_format.format(level_color=colors['yellow']), 
                datefmt=date_format
            ),
            logging.ERROR: logging.Formatter(
                base_format.format(level_color=colors['red']), 
                datefmt=date_format
            ),
            logging.CRITICAL: logging.Formatter(
                base_format.format(level_color=colors['red'] + colors['bold']), 
                datefmt=date_format
            ),
        }

    def format(self, record: logging.LogRecord) -> str:
        # Extract short name from full logger name
        if hasattr(record, 'name') and record.name:
            if record.name == '__main__':
                record.short_name = 'main'
            elif 'backends.execution' in record.name:
                record.short_name = f"execution.backend({record.name.split('.')[-1]})"
            else:
                # Get the last component of the logger name
                record.short_name = record.name.split('.')[-1]
        else:
            record.short_name = 'unknown'

        return self.level_formatters[record.levelno].format(record)


def _thread_info_filter(record: logging.LogRecord) -> bool:  # Fixed: should return bool
    """Add thread information to log records."""
    record.thread_id = threading.get_native_id()
    return True  # Fixed: filters must return True to pass the record


def init_default_logger(
    log_level: Union[int, str] = logging.INFO,
    *,
    output_file: Optional[Union[str, pathlib.Path]] = None,
    file_log_level: Optional[Union[int, str]] = None,
    use_colors: bool = True,
    show_details: bool = False,
    style: str = "modern",
    clear_handlers: bool = False,
    logger_name: Optional[str] = None,
) -> logging.Logger:
    """Setup and configure logging with enhanced features.

    Args:
        log_level: Base logging level for console output.
        output_file: Path to log file. If provided, file logging is enabled.
        file_log_level: Logging level for file output. Defaults to log_level.
        use_colors: Enable colored console output.
        show_details: Include thread/process info in log messages.
        style: Format style - 'modern', 'core', 'execution_backend', or 'minimal'.
        clear_handlers: Remove existing handlers from root logger.
        logger_name: Name for the logger. If None, returns root logger.

    Returns:
        Configured logger instance.
    """
    # Get or create logger
    logger = logging.getLogger(logger_name) if logger_name else logging.getLogger()
    
    # Clear existing handlers if requested
    if clear_handlers:
        logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_formatter = _ColoredFormatter(
        use_colors=use_colors, 
        show_details=show_details, 
        style=style
    )
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(log_level)

    if show_details:
        console_handler.addFilter(_thread_info_filter)

    logger.addHandler(console_handler)  # Fixed: add handler to logger, not just to list

    # File handler (if requested)
    if output_file is not None:
        file_level = log_level if file_log_level is None else file_log_level
        file_path = pathlib.Path(output_file)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(file_path)
        file_formatter = _ColoredFormatter(
            use_colors=False,  # No colors in file
            show_details=show_details, 
            style=style
        )
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(file_level)
        
        if show_details:
            file_handler.addFilter(_thread_info_filter)
        
        logger.addHandler(file_handler)  # Fixed: add handler to logger

    # Set logger level
    logger.setLevel(logging.NOTSET)  # Fixed: don't use basicConfig, just set logger level

    # Capture warnings
    logging.captureWarnings(True)

    # Log configuration info
    level_name = logging.getLevelName(log_level) if isinstance(log_level, int) else log_level
    file_level_name = logging.getLevelName(file_log_level) if isinstance(file_log_level, int) else file_log_level
    
    logger.info(
        'Logger configured successfully - Console: %s, File: %s (%s), Style: %s',
        level_name,
        output_file or 'disabled',
        file_level_name or 'N/A',
        style
    )

    return logger


# Convenience function for quick setup
def get_logger(name: str = None, level: Union[int, str] = logging.INFO) -> logging.Logger:
    """Quick logger setup for simple use cases."""
    if not logging.getLogger().handlers:
        init_default_logger(level, logger_name=name, style="modern")
    return logging.getLogger(name)
