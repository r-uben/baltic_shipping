import logging
import colorama

colorama.init()

class ColorFormatter(logging.Formatter):
    """A logging formatter that adds color to log levels."""

    COLORS = {
        "DEBUG": colorama.Fore.CYAN,
        "INFO": colorama.Fore.GREEN,
        "WARNING": colorama.Fore.YELLOW,
        "ERROR": colorama.Fore.RED,
        "CRITICAL": colorama.Fore.MAGENTA,
    }

    def format(self, record):
        color = self.COLORS.get(record.levelname, colorama.Fore.WHITE)
        record.levelname = f"{color}{record.levelname}{colorama.Style.RESET_ALL}"
        return super().format(record)

def get_logger(name):
    """Creates and configures a logger."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    # Prevent duplicate handlers
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = ColorFormatter(
            "%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
    return logger
