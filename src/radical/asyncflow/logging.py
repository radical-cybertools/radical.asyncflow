from __future__ import annotations

import json
import logging
import pathlib
import sys
import threading
from typing import Optional, Union


class _ColoredFormatter(logging.Formatter):
    """Custom formatter with enhanced color schemes and flexible formatting."""

    def __init__(
        self,
        use_colors: bool = True,
        show_details: bool = False,
        style: str = "modern",
    ) -> None:
        self.use_colors = use_colors
        self.show_details = show_details
        self.style = style

        # Enhanced color palette
        if use_colors:
            colors = {
                "grey": "\033[90m",  # Darker grey
                "green": "\033[92m",  # Bright green
                "cyan": "\033[96m",  # Bright cyan
                "blue": "\033[94m",  # Bright blue
                "yellow": "\033[93m",  # Bright yellow
                "red": "\033[91m",  # Bright red
                "magenta": "\033[95m",  # Bright magenta
                "white": "\033[97m",  # Bright white
                "purple": "\033[95m",  # Bright magenta
                "bright_purple": "\033[38;5;165m",  # Bright purple
                "bold": "\033[1m",  # Bold
                "reset": "\033[0m",  # Reset
            }
        else:
            colors = {
                k: ""
                for k in [
                    "grey",
                    "green",
                    "cyan",
                    "blue",
                    "yellow",
                    "red",
                    "magenta",
                    "white",
                    "purple",
                    "bright_purple",
                    "bold",
                    "reset",
                ]
            }

        # Detail formatting
        if show_details:
            detail_info = (
                f"{colors['green']}[TID:{colors['white']}%(thread_id)s"
                f"{colors['green']} PID:{colors['white']}%(process)d"
                f"{colors['green']}]{colors['reset']} "
            )
        else:
            detail_info = ""

        # Style variations
        timestamp_fmt = f"{colors['grey']}%(asctime)s.%(msecs)03d{colors['reset']}"
        logger_fmt = f"{colors['bright_purple']}[%(short_name)s]{colors['reset']}"
        separator = " │ "

        # Date format
        date_format = "%Y-%m-%d %H:%M:%S"

        level_colors = {
            logging.DEBUG: colors["cyan"],
            logging.INFO: colors["blue"],
            logging.WARNING: colors["yellow"],
            logging.ERROR: colors["red"],
            logging.CRITICAL: colors["red"] + colors["bold"],
        }

        # Build base format string
        base_format = (
            f"{timestamp_fmt}{separator}{detail_info}{{level_color}}"
            f"%(levelname)s{colors['reset']}{separator}{logger_fmt}"
            f"{separator}%(message)s"
        )

        # Level-specific formatters with style-dependent colors
        self.level_formatters = {
            logging.DEBUG: logging.Formatter(
                base_format.format(level_color=level_colors[logging.DEBUG]),
                datefmt=date_format,
            ),
            logging.INFO: logging.Formatter(
                base_format.format(level_color=level_colors[logging.INFO]),
                datefmt=date_format,
            ),
            logging.WARNING: logging.Formatter(
                base_format.format(level_color=level_colors[logging.WARNING]),
                datefmt=date_format,
            ),
            logging.ERROR: logging.Formatter(
                base_format.format(level_color=level_colors[logging.ERROR]),
                datefmt=date_format,
            ),
            logging.CRITICAL: logging.Formatter(
                base_format.format(level_color=level_colors[logging.CRITICAL]),
                datefmt=date_format,
            ),
        }

    def format(self, record: logging.LogRecord) -> str:
        # Extract short name from full logger name
        if hasattr(record, "name") and record.name:
            if record.name == "__main__":
                record.short_name = "main"
            elif "backends.execution" in record.name:
                record.short_name = f"execution.backend({record.name.split('.')[-1]})"
            else:
                # Get the last component of the logger name
                record.short_name = record.name.split(".")[-1]
        else:
            record.short_name = "unknown"

        return self.level_formatters[record.levelno].format(record)


class _StructuredFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add any extra fields passed via extra= parameter
        for key, value in record.__dict__.items():
            if key not in (
                "name",
                "msg",
                "args",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "lineno",
                "funcName",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
                "message",
                "exc_info",
                "exc_text",
                "stack_info",
            ):
                log_data[key] = value

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data, default=str)


class StructuredLogger:
    """Drop-in replacement that adds structured logging capabilities."""

    def __init__(self, logger: logging.Logger):
        self._logger = logger

    def info(self, message: str, **kwargs):
        """Enhanced info() that accepts both old and new usage patterns."""
        if kwargs:
            self._logger.info(message, extra=kwargs)
        else:
            self._logger.info(message)

    def error(self, message: str, **kwargs):
        if kwargs:
            self._logger.error(message, extra=kwargs)
        else:
            self._logger.error(message)

    def debug(self, message: str, **kwargs):
        if kwargs:
            self._logger.debug(message, extra=kwargs)
        else:
            self._logger.debug(message)

    def warning(self, message: str, **kwargs):
        if kwargs:
            self._logger.warning(message, extra=kwargs)
        else:
            self._logger.warning(message)

    def critical(self, message: str, **kwargs):
        if kwargs:
            self._logger.critical(message, extra=kwargs)
        else:
            self._logger.critical(message)

    # Delegate other methods to underlying logger
    def __getattr__(self, name):
        return getattr(self._logger, name)


def _thread_info_filter(record: logging.LogRecord) -> bool:
    """Add thread information to log records."""
    record.thread_id = threading.get_native_id()
    return True


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
    structured_logging: bool = False,
    structured_file: Optional[Union[str, pathlib.Path]] = None,
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
        structured_logging: Enable JSON structured logging to file.
        structured_file: Custom path for structured JSON log file.

    Returns:
        Configured logger instance.
    """
    # Get or create logger
    logger = logging.getLogger(logger_name) if logger_name else logging.getLogger()

    # Clear existing handlers if requested
    if clear_handlers:
        logger.handlers.clear()

    # Console always uses colored formatter (never JSON)
    console_formatter = _ColoredFormatter(
        use_colors=use_colors, show_details=show_details, style=style
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(log_level)

    if show_details:
        console_handler.addFilter(_thread_info_filter)

    logger.addHandler(console_handler)

    # Regular file handler (if requested) - uses colored formatter without colors
    if output_file is not None:
        file_level = log_level if file_log_level is None else file_log_level
        file_path = pathlib.Path(output_file)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(file_path)
        file_formatter = _ColoredFormatter(
            use_colors=False,  # No colors in file
            show_details=show_details,
            style=style,
        )
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(file_level)

        if show_details:
            file_handler.addFilter(_thread_info_filter)

        logger.addHandler(file_handler)

    # Structured JSON file handler (if requested)
    if structured_logging:
        # Auto-generate structured log file name if not provided
        if structured_file is None:
            if output_file is not None:
                # Use same directory as regular log file
                structured_file = pathlib.Path(output_file).with_suffix(".json")
            else:
                # Default to structured.log in current directory
                structured_file = "radical.asyncflow.logs.json"

        struct_level = log_level if file_log_level is None else file_log_level
        struct_path = pathlib.Path(structured_file)
        struct_path.parent.mkdir(parents=True, exist_ok=True)

        struct_handler = logging.FileHandler(struct_path)
        struct_formatter = _StructuredFormatter()
        struct_handler.setFormatter(struct_formatter)
        struct_handler.setLevel(struct_level)

        if show_details:
            struct_handler.addFilter(_thread_info_filter)

        logger.addHandler(struct_handler)

    # Set logger level
    logger.setLevel(logging.NOTSET)

    # Capture warnings
    logging.captureWarnings(True)

    # Log configuration info
    level_name = (
        logging.getLevelName(log_level) if isinstance(log_level, int) else log_level
    )
    file_level_name = (
        logging.getLevelName(file_log_level)
        if isinstance(file_log_level, int)
        else file_log_level
    )

    structured_info = str(structured_file) if structured_logging else "disabled"

    logger.info(
        "Logger configured successfully - "
        "Console: %s, File: %s (%s), Structured: %s, Style: %s",
        level_name,
        output_file or "disabled",
        file_level_name or "N/A",
        structured_info,
        style,
    )

    return logger


def get_structured_logger(
    name: str = None, level: Union[int, str] = logging.INFO
) -> StructuredLogger:
    """Get a structured logger that supports both old and new usage patterns."""
    # Check if logger already configured, if not configure it with structured logging
    base_logger = logging.getLogger(name)
    if not base_logger.handlers:
        init_default_logger(level, logger_name=name, structured_logging=True)
        base_logger = logging.getLogger(name)

    return StructuredLogger(base_logger)


# Convenience function for quick setup
def get_logger(
    name: str = None, level: Union[int, str] = logging.INFO
) -> logging.Logger:
    """Quick logger setup for simple use cases."""
    if not logging.getLogger().handlers:
        init_default_logger(level, logger_name=name, style="modern")
    return logging.getLogger(name)
