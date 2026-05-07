"""API URL configuration."""
from django.urls import path

from . import api_views

urlpatterns = [
    path("auth/login", api_views.auth_login),
    path("auth/register", api_views.auth_register),
    path("auth/status", api_views.auth_status),
    path("kpi", api_views.kpi),
    path("distribution", api_views.distribution),
    path("trend", api_views.trend),
    path("top-products", api_views.top_products),
    path("products", api_views.all_products),
    path("words", api_views.word_frequencies),
    path("confusion-matrix", api_views.confusion_matrix),
    path("confidence-distribution", api_views.confidence_distribution),
    path("recent", api_views.recent),
    path("search", api_views.search),
    path("product/<str:product_id>", api_views.product_detail),
    path("product/<str:product_id>/reviews", api_views.product_reviews),
    path("pipeline/start", api_views.pipeline_start),
    path("pipeline/stop", api_views.pipeline_stop),
    path("pipeline/status", api_views.pipeline_status),
    path("pipeline/logs", api_views.pipeline_logs),
    path("dashboards", api_views.dashboards),
    path("dashboards/<int:dashboard_id>", api_views.dashboard_delete),
    path("internal/live-push", api_views.live_push),
    path("health", api_views.health),
]
