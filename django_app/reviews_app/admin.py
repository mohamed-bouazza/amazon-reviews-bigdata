"""Django admin registrations."""
from django.contrib import admin

from .models import AuditLog, ModelMetadata, PipelineState, SavedDashboard


@admin.register(ModelMetadata)
class ModelMetadataAdmin(admin.ModelAdmin):
    """Admin view for ModelMetadata."""

    list_display = ("name", "model_type", "f1_score", "accuracy", "is_active", "trained_at")
    list_filter = ("is_active", "model_type")


@admin.register(SavedDashboard)
class SavedDashboardAdmin(admin.ModelAdmin):
    """Admin view for SavedDashboard."""

    list_display = ("name", "user", "filter_year", "filter_sentiment", "last_viewed")
    list_filter = ("filter_sentiment", "filter_year")


@admin.register(AuditLog)
class AuditLogAdmin(admin.ModelAdmin):
    """Admin view for AuditLog."""

    list_display = ("action", "user", "timestamp")
    list_filter = ("action",)
    readonly_fields = ("user", "action", "details", "timestamp")


@admin.register(PipelineState)
class PipelineStateAdmin(admin.ModelAdmin):
    """Admin view for PipelineState."""

    list_display = ("status", "producer_pid", "consumer_pid", "started_at", "started_by")
