import logging
import os
import time

def setup_logging(step_name: str, log_dir: str = "logs"):
    """
    Configure logging to both file and console for a specific ETL step.
    Example: extract.log, transform.log, load.log
    """
    os.makedirs(log_dir, exist_ok=True)
    log_file_name = os.path.join(log_dir, f"{step_name}.log")

    log_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] [%(threadName)-12.12s] %(message)s"
    )
    root_logger = logging.getLogger(step_name)

    # Prevent duplicate handlers if logger is reused
    if not root_logger.handlers:
        root_logger.setLevel(logging.INFO)

        # File handler
        file_handler = logging.FileHandler(log_file_name)
        file_handler.setFormatter(log_formatter)
        root_logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_formatter)
        root_logger.addHandler(console_handler)

    return root_logger


def format_time(seconds: float) -> str:
    """
    Format seconds into H:M:S string for timing ETL tasks.
    """
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"


def log_execution_time(logger, task_name: str, func, *args, **kwargs):
    """
    Utility wrapper to log execution time of a function.
    Example:
        log_execution_time(logger, "Transform Orders", transform_orders, df)
    """
    start_time = time.time()
    logger.info(f"Starting task: {task_name}")
    result = func(*args, **kwargs)
    elapsed = time.time() - start_time
    logger.info(f"Finished task: {task_name} in {format_time(elapsed)}")
    return result
