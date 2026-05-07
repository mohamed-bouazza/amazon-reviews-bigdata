"""PostgreSQL models for the reviews app."""
from django.contrib.auth.models import User
from django.db import models


class ModelMetadata(models.Model):
    """Metadata about trained ML models."""

    name = models.CharField(max_length=100)
    model_type = models.CharField(max_length=50)
    f1_score = models.FloatField()
    accuracy = models.FloatField()
    trained_at = models.DateTimeField()
    is_active = models.BooleanField(default=False)
    notes = models.TextField(blank=True)

    def __str__(self) -> str:
        """Return display name."""
        return f"{self.name} (F1={self.f1_score:.4f})"


class SavedDashboard(models.Model):
    """User-saved dashboard configuration."""

    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name="dashboards")
    name = models.CharField(max_length=100)
    filter_year = models.IntegerField(null=True, blank=True)
    filter_sentiment = models.CharField(max_length=20, blank=True)
    filter_product = models.CharField(max_length=50, blank=True)
    predictions_count = models.IntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    last_viewed = models.DateTimeField(auto_now=True)

    class Meta:
        """Model meta."""

        ordering = ["-last_viewed"]

    def __str__(self) -> str:
        """Return display name."""
        return f"{self.name} ({self.user.username})"


class AuditLog(models.Model):
    """Audit trail of user-triggered actions."""

    user = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    action = models.CharField(max_length=100)
    details = models.JSONField(default=dict, blank=True)
    timestamp = models.DateTimeField(auto_now_add=True)

    class Meta:
        """Model meta."""

        ordering = ["-timestamp"]

    def __str__(self) -> str:
        """Return display name."""
        return f"{self.action} @ {self.timestamp:%Y-%m-%d %H:%M}"


class PipelineState(models.Model):
    """Singleton tracking the streaming pipeline state."""

    status = models.CharField(max_length=20, default="stopped")
    producer_pid = models.IntegerField(null=True, blank=True)
    consumer_pid = models.IntegerField(null=True, blank=True)
    started_at = models.DateTimeField(null=True, blank=True)
    started_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    last_log = models.TextField(blank=True)

    @classmethod
    def get_solo(cls) -> "PipelineState":
        """Return the singleton instance, creating it if missing."""
        obj, _ = cls.objects.get_or_create(pk=1, defaults={"status": "stopped"})
        return obj

    def __str__(self) -> str:
        """Return display name."""
        return f"PipelineState({self.status})"
