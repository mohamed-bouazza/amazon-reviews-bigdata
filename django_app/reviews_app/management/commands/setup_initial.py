"""Initial setup command — migrations, groups, superuser, model metadata."""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

from django.conf import settings
from django.contrib.auth.models import Group, Permission, User
from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.utils import timezone

from reviews_app.models import ModelMetadata, PipelineState

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """Run initial setup: migrate, groups, superuser, metadata."""

    help = "Initialize database, groups, superuser, model metadata, and pipeline state."

    def handle(self, *args, **options) -> None:
        """Execute the setup steps."""
        self.stdout.write("→ Applying migrations...")
        call_command("migrate", interactive=False, verbosity=1)

        self.stdout.write("→ Creating user groups...")
        admin_group, _ = Group.objects.get_or_create(name="Admin")
        analyst_group, _ = Group.objects.get_or_create(name="Analyst")
        viewer_group, _ = Group.objects.get_or_create(name="Viewer")

        # Admin group: every permission
        admin_group.permissions.set(Permission.objects.all())
        # Analyst: app permissions
        analyst_perms = Permission.objects.filter(
            content_type__app_label="reviews_app"
        )
        analyst_group.permissions.set(analyst_perms)
        # Viewer: only "view" permissions
        viewer_perms = analyst_perms.filter(codename__startswith="view_")
        viewer_group.permissions.set(viewer_perms)

        self.stdout.write("→ Ensuring superuser admin...")
        if not User.objects.filter(username="admin").exists():
            user = User.objects.create_superuser("admin", "admin@example.com", "admin123")
            user.groups.add(admin_group)
            self.stdout.write(self.style.SUCCESS("  created admin/admin123"))
        else:
            user = User.objects.get(username="admin")
            user.groups.add(admin_group)
            self.stdout.write("  admin already exists")

        self.stdout.write("→ Loading model metadata...")
        self._load_model_metadata()

        self.stdout.write("→ Initializing pipeline state...")
        state = PipelineState.get_solo()
        state.status = "stopped"
        state.producer_pid = None
        state.consumer_pid = None
        state.save()

        self.stdout.write(self.style.SUCCESS(
            "\nSetup complete. Login: admin / admin123 at http://localhost:8000/login/\n"
        ))

    def _load_model_metadata(self) -> None:
        """Load model metadata from project root metadata.json if available."""
        candidates = [
            Path(settings.PROJECT_ROOT) / "models" / "metadata.json",
            Path(settings.BASE_DIR).parent / "models" / "metadata.json",
        ]
        meta_path = next((p for p in candidates if p.exists()), None)
        if not meta_path:
            ModelMetadata.objects.update_or_create(
                name="LogReg-TFIDF",
                defaults={
                    "model_type": "Spark MLlib LogisticRegression",
                    "f1_score": 0.8274,
                    "accuracy": 0.85,
                    "trained_at": timezone.now(),
                    "is_active": True,
                    "notes": "Default metadata (metadata.json not found).",
                },
            )
            self.stdout.write("  metadata.json not found — wrote default record")
            return
        try:
            data = json.loads(meta_path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            self.stdout.write(self.style.WARNING(f"  cannot parse {meta_path}: {exc}"))
            return
        records = data if isinstance(data, list) else [data]
        for rec in records:
            trained_at = rec.get("trained_at")
            if isinstance(trained_at, str):
                try:
                    trained_at = datetime.fromisoformat(trained_at)
                except ValueError:
                    trained_at = timezone.now()
            else:
                trained_at = timezone.now()
            ModelMetadata.objects.update_or_create(
                name=rec.get("name", "LogReg-TFIDF"),
                defaults={
                    "model_type": rec.get("model_type", "LogisticRegression"),
                    "f1_score": float(rec.get("f1_score", 0.8274)),
                    "accuracy": float(rec.get("accuracy", 0.85)),
                    "trained_at": trained_at,
                    "is_active": bool(rec.get("is_active", True)),
                    "notes": rec.get("notes", ""),
                },
            )
        self.stdout.write(f"  loaded {len(records)} record(s) from {meta_path}")
