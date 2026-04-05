import json
import logging
import os
import sys
from collections import deque
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler

import psutil


class JsonFormatter(logging.Formatter):
    """Serialize log records as compact JSON for machines and dashboards."""

    RESERVED_ATTRS = {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "message",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "thread",
        "threadName",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "line": record.lineno,
        }

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        for key, value in record.__dict__.items():
            if key in self.RESERVED_ATTRS or key.startswith("_"):
                continue
            payload[key] = value

        return json.dumps(payload, default=str)


def setup_logging() -> str:
    """Configure root logger to emit JSON to stdout and a rotating file."""
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    log_file = os.getenv("LOG_FILE", "logs/app.log")
    log_max_bytes = int(os.getenv("LOG_MAX_BYTES", 2 * 1024 * 1024))
    log_backup_count = int(os.getenv("LOG_BACKUP_COUNT", 5))

    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    formatter = JsonFormatter()
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers.clear()

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=log_max_bytes,
        backupCount=log_backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(logger_name)
        logger.handlers.clear()
        logger.propagate = True

    root_logger.info(
        "Logging configured",
        extra={
            "component": "observability",
            "log_file": log_file,
            "log_level": log_level,
        },
    )
    return log_file


def get_system_metrics() -> dict:
    """Return host and process usage metrics for lightweight health checks."""
    vm = psutil.virtual_memory()
    process = psutil.Process()

    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "cpu": {
            "percent": psutil.cpu_percent(interval=None),
            "count": psutil.cpu_count(),
        },
        "memory": {
            "total_bytes": vm.total,
            "available_bytes": vm.available,
            "used_bytes": vm.used,
            "percent": vm.percent,
        },
        "process": {
            "pid": process.pid,
            "rss_bytes": process.memory_info().rss,
            "threads": process.num_threads(),
        },
    }


def read_recent_logs(log_file: str, limit: int = 100) -> list:
    """Read and parse the latest JSON log lines from the configured log file."""
    if not os.path.exists(log_file):
        return []

    with open(log_file, "r", encoding="utf-8") as file_obj:
        tail_lines = deque(file_obj, maxlen=limit)

    records = []
    for line in tail_lines:
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            records.append({"raw": line})
    return records