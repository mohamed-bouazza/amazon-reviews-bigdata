"""Pipeline subprocess controller for Kafka producer + Spark consumer."""
from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from django.conf import settings
from django.contrib.auth.models import User
from django.utils import timezone

from .models import AuditLog, PipelineState

logger = logging.getLogger(__name__)


class PipelineManager:
    """Manage producer + spark-consumer subprocesses."""

    PROJECT_ROOT: Path = Path(settings.PROJECT_ROOT)
    LOG_DIR: Path = Path(settings.BASE_DIR) / "logs"

    # -u = unbuffered stdout/stderr so logs appear immediately in the log file
    PRODUCER_CMD = [sys.executable, "-u", "kafka_producer/producer.py"]
    CONSUMER_CMD = [
        "spark-submit",
        "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "spark_streaming/consumer.py",
    ]

    @classmethod
    def _ensure_log_dir(cls) -> None:
        """Ensure log directory exists."""
        cls.LOG_DIR.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _is_alive(pid: int | None) -> bool:
        """Return True if a PID is alive."""
        if not pid:
            return False
        try:
            os.kill(int(pid), 0)
            return True
        except OSError:
            return False

    @classmethod
    def _spawn(cls, cmd: list[str], log_path: Path) -> int:
        """Spawn a subprocess detached with stdout/stderr redirected."""
        cls._ensure_log_dir()
        log_file = open(log_path, "ab", buffering=0)
        env = os.environ.copy()
        mongo_uri = getattr(settings, "MONGO_URI", None) or "mongodb://localhost:27018"
        if not mongo_uri or "27017" in mongo_uri or "mongo:" in mongo_uri:
            mongo_uri = "mongodb://localhost:27018"
        env["MONGO_URI"] = mongo_uri
        env["KAFKA_BROKER"] = getattr(settings, "KAFKA_BROKER", None) or "localhost:29092"
        logger.info("Spawning subprocess with MONGO_URI=%s", mongo_uri)
        proc = subprocess.Popen(
            cmd,
            cwd=str(cls.PROJECT_ROOT),
            stdout=log_file,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
            env=env,
        )
        return proc.pid

    @classmethod
    def start_producer(cls, user: User) -> dict[str, Any]:
        """Start the Kafka producer subprocess."""
        state = PipelineState.get_solo()
        if cls._is_alive(state.producer_pid):
            return {"status": "already_running", "pid": state.producer_pid}
        pid = cls._spawn(cls.PRODUCER_CMD, cls.LOG_DIR / "producer.log")
        state.producer_pid = pid
        state.status = "running"
        state.started_at = timezone.now()
        state.started_by = user if user.is_authenticated else None
        state.last_log = f"Producer started PID={pid} at {datetime.utcnow().isoformat()}"
        state.save()
        AuditLog.objects.create(
            user=user if user.is_authenticated else None,
            action="pipeline.start_producer",
            details={"pid": pid},
        )
        logger.info("Producer started PID=%s", pid)
        return {"status": "started", "pid": pid}

    @classmethod
    def start_consumer(cls, user: User) -> dict[str, Any]:
        """Start the Spark streaming consumer subprocess."""
        state = PipelineState.get_solo()
        if cls._is_alive(state.consumer_pid):
            return {"status": "already_running", "pid": state.consumer_pid}
        pid = cls._spawn(cls.CONSUMER_CMD, cls.LOG_DIR / "spark.log")
        state.consumer_pid = pid
        state.status = "running"
        state.started_at = state.started_at or timezone.now()
        state.started_by = user if user.is_authenticated else None
        state.last_log = f"Consumer started PID={pid}"
        state.save()
        AuditLog.objects.create(
            user=user if user.is_authenticated else None,
            action="pipeline.start_consumer",
            details={"pid": pid},
        )
        logger.info("Consumer started PID=%s", pid)
        return {"status": "started", "pid": pid}

    @classmethod
    def start_all(cls, user: User) -> dict[str, Any]:
        """Start both producer and consumer."""
        return {
            "producer": cls.start_producer(user),
            "consumer": cls.start_consumer(user),
        }

    @classmethod
    def _kill(cls, pid: int | None) -> bool:
        """Send SIGTERM to a PID and its process group."""
        if not pid:
            return False
        try:
            os.killpg(os.getpgid(int(pid)), signal.SIGTERM)
            return True
        except OSError:
            try:
                os.kill(int(pid), signal.SIGTERM)
                return True
            except OSError:
                return False

    @classmethod
    def stop_all(cls, user: User) -> dict[str, Any]:
        """Stop both producer and consumer processes."""
        state = PipelineState.get_solo()
        killed = {
            "producer": cls._kill(state.producer_pid),
            "consumer": cls._kill(state.consumer_pid),
        }
        state.producer_pid = None
        state.consumer_pid = None
        state.status = "stopped"
        state.last_log = f"Stopped at {datetime.utcnow().isoformat()}"
        state.save()
        AuditLog.objects.create(
            user=user if user.is_authenticated else None,
            action="pipeline.stop_all",
            details=killed,
        )
        logger.info("Pipeline stopped: %s", killed)
        return {"status": "stopped", "killed": killed}

    @classmethod
    def get_status(cls) -> dict[str, Any]:
        """Return current pipeline status info."""
        state = PipelineState.get_solo()
        producer_alive = cls._is_alive(state.producer_pid)
        consumer_alive = cls._is_alive(state.consumer_pid)
        if not producer_alive and not consumer_alive and state.status == "running":
            state.status = "stopped"
            state.save(update_fields=["status"])
        return {
            "status": state.status,
            "producer": {
                "pid": state.producer_pid,
                "alive": producer_alive,
            },
            "consumer": {
                "pid": state.consumer_pid,
                "alive": consumer_alive,
            },
            "started_at": state.started_at.isoformat() if state.started_at else None,
            "started_by": state.started_by.username if state.started_by else None,
            "last_log": state.last_log,
        }

    @classmethod
    def _tail(cls, path: Path, lines: int) -> list[str]:
        """Return up to N tail lines from a log file."""
        if not path.exists():
            return []
        try:
            with open(path, "rb") as fh:
                fh.seek(0, os.SEEK_END)
                size = fh.tell()
                read_size = min(size, 64 * 1024)
                fh.seek(size - read_size)
                data = fh.read().decode("utf-8", errors="replace")
            return data.splitlines()[-lines:]
        except OSError as exc:
            logger.warning("Cannot read log %s: %s", path, exc)
            return []

    @classmethod
    def get_recent_logs(cls, lines: int = 50) -> dict[str, list[str]]:
        """Return last N log lines from producer and spark."""
        return {
            "producer": cls._tail(cls.LOG_DIR / "producer.log", lines),
            "spark": cls._tail(cls.LOG_DIR / "spark.log", lines),
        }
