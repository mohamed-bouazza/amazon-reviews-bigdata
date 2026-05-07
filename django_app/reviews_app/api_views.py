"""REST API views — JSON endpoints backed by MongoDB and PostgreSQL."""
from __future__ import annotations

import json
import logging

from django.contrib.auth import authenticate, login
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import Group, User
from django.http import HttpRequest, JsonResponse
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_http_methods, require_POST

from . import mongo_client as mongo
from .models import AuditLog, SavedDashboard
from .pipeline_manager import PipelineManager

logger = logging.getLogger(__name__)


def _mongo_error(exc: Exception) -> JsonResponse:
    """Return a 503 response for MongoDB issues."""
    logger.warning("Mongo unavailable: %s", exc)
    return JsonResponse(
        {"error": "MongoDB unavailable", "detail": str(exc)},
        status=503,
    )


def _is_analyst(user) -> bool:
    """Return True if user has analyst-or-higher access."""
    if not user.is_authenticated:
        return False
    if user.is_superuser or user.is_staff:
        return True
    return user.groups.filter(name__in=["Admin", "Analyst"]).exists()


@require_POST
def auth_login(request: HttpRequest) -> JsonResponse:
    """Authenticate user and start session."""
    try:
        data = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    username = data.get("username", "").strip()
    password = data.get("password", "")
    if not username or not password:
        return JsonResponse({"error": "Username and password required"}, status=400)
    user = authenticate(request, username=username, password=password)
    if user is None:
        return JsonResponse({"error": "Invalid credentials"}, status=401)
    login(request, user)
    return JsonResponse({
        "username": user.username,
        "email": user.email,
        "groups": list(user.groups.values_list("name", flat=True)),
    })


@require_POST
def auth_register(request: HttpRequest) -> JsonResponse:
    """Create account and start session."""
    try:
        data = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    username = data.get("username", "").strip()
    email = data.get("email", "").strip()
    password = data.get("password", "")
    confirm = data.get("confirm", "")
    if not username or not password:
        return JsonResponse({"error": "Username and password required"}, status=400)
    if len(password) < 8:
        return JsonResponse({"error": "Password must be at least 8 characters"}, status=400)
    if password != confirm:
        return JsonResponse({"error": "Passwords do not match"}, status=400)
    if User.objects.filter(username=username).exists():
        return JsonResponse({"error": "Username already taken"}, status=409)
    user = User.objects.create_user(username=username, email=email, password=password)
    viewer, _ = Group.objects.get_or_create(name="Viewer")
    user.groups.add(viewer)
    login(request, user)
    return JsonResponse({
        "username": user.username,
        "email": user.email,
        "groups": ["Viewer"],
    }, status=201)


@require_GET
def auth_status(request: HttpRequest) -> JsonResponse:
    """Return current auth state."""
    if request.user.is_authenticated:
        return JsonResponse({
            "authenticated": True,
            "username": request.user.username,
            "groups": list(request.user.groups.values_list("name", flat=True)),
        })
    return JsonResponse({"authenticated": False})


@require_GET
def kpi(request: HttpRequest) -> JsonResponse:
    """Return KPI cards data."""
    try:
        return JsonResponse(mongo.get_kpi_stats())
    except mongo.MongoUnavailable as exc:
        return _mongo_error(exc)


@require_GET
def distribution(request: HttpRequest) -> JsonResponse:
    """Return global sentiment distribution."""
    try:
        return JsonResponse(mongo.get_global_distribution())
    except mongo.MongoUnavailable as exc:
        return _mongo_error(exc)


@require_GET
def trend(request: HttpRequest) -> JsonResponse:
    """Return yearly trend or monthly trend if year specified."""
    year = request.GET.get("year", "All")
    try:
        if year and year != "All":
            data = mongo.get_monthly_trend(int(year))
            return JsonResponse({"granularity": "monthly", "year": int(year), "data": data})
        return JsonResponse({"granularity": "yearly", "data": mongo.get_yearly_trend()})
    except (ValueError, mongo.MongoUnavailable) as exc:
        if isinstance(exc, mongo.MongoUnavailable):
            return _mongo_error(exc)
        return JsonResponse({"error": "Invalid year"}, status=400)


@require_GET
def top_products(request: HttpRequest) -> JsonResponse:
    """Return top products by count, optionally filtered by sentiment."""
    sentiment = request.GET.get("sentiment", "all")
    limit = int(request.GET.get("limit", 10))
    try:
        return JsonResponse({"data": mongo.get_top_products(sentiment, limit)})
    except mongo.MongoUnavailable as exc:
        return _mongo_error(exc)


@require_GET
def all_products(request: HttpRequest) -> JsonResponse:
    """Return all distinct products with review counts."""
    limit = int(request.GET.get("limit", 2000))
    try:
        return JsonResponse({"data": mongo.get_all_products(limit)})
    except mongo.MongoUnavailable as exc:
        return _mongo_error(exc)


@require_GET
def word_frequencies(request: HttpRequest) -> JsonResponse:
    """Return top frequent words by sentiment."""
    limit = int(request.GET.get("limit", 25))
    try:
        return JsonResponse({"data": mongo.get_word_frequencies(limit)})
    except mongo.MongoUnavailable as exc:
        return _mongo_error(exc)


@require_GET
def confusion_matrix(request: HttpRequest) -> JsonResponse:
    """Return 3x3 confusion matrix."""
    try:
        return JsonResponse({"matrix": mongo.get_confusion_matrix()})
    except mongo.MongoUnavailable as exc:
        return _mongo_error(exc)


@require_GET
def confidence_distribution(request: HttpRequest) -> JsonResponse:
    """Return confidence histogram bins per sentiment."""
    try:
        return JsonResponse(mongo.get_confidence_distribution())
    except mongo.MongoUnavailable as exc:
        return _mongo_error(exc)


@require_GET
def recent(request: HttpRequest) -> JsonResponse:
    """Return recent predictions."""
    limit = int(request.GET.get("limit", 50))
    sentiment = request.GET.get("sentiment")
    try:
        return JsonResponse({"data": mongo.get_recent_predictions(limit, sentiment)})
    except mongo.MongoUnavailable as exc:
        return _mongo_error(exc)


@require_GET
def product_detail(request: HttpRequest, product_id: str) -> JsonResponse:
    """Return product detail aggregation."""
    try:
        data = mongo.get_product_detail(product_id)
        if not data:
            return JsonResponse({"error": "Product not found"}, status=404)
        return JsonResponse(data)
    except mongo.MongoUnavailable as exc:
        return _mongo_error(exc)


@require_GET
def product_reviews(request: HttpRequest, product_id: str) -> JsonResponse:
    """Return paginated reviews for a product."""
    page = int(request.GET.get("page", 1))
    sentiment = request.GET.get("sentiment", "all")
    try:
        return JsonResponse(mongo.get_product_reviews(product_id, sentiment, page))
    except mongo.MongoUnavailable as exc:
        return _mongo_error(exc)


@require_GET
def search(request: HttpRequest) -> JsonResponse:
    """Search predictions by Summary/Text content."""
    q = request.GET.get("q", "").strip()
    if not q:
        return JsonResponse({"data": []})
    try:
        return JsonResponse({"data": mongo.search_predictions(q, 20)})
    except mongo.MongoUnavailable as exc:
        return _mongo_error(exc)


# ───── Pipeline ─────

@login_required
@require_http_methods(["POST"])
def pipeline_start(request: HttpRequest) -> JsonResponse:
    """Start the streaming pipeline."""
    if not _is_analyst(request.user):
        return JsonResponse({"error": "Forbidden"}, status=403)
    result = PipelineManager.start_all(request.user)
    return JsonResponse(result)


@login_required
@require_http_methods(["POST"])
def pipeline_stop(request: HttpRequest) -> JsonResponse:
    """Stop the streaming pipeline."""
    if not _is_analyst(request.user):
        return JsonResponse({"error": "Forbidden"}, status=403)
    return JsonResponse(PipelineManager.stop_all(request.user))


@require_GET
def pipeline_status(request: HttpRequest) -> JsonResponse:
    """Return pipeline status."""
    return JsonResponse(PipelineManager.get_status())


@require_GET
def pipeline_logs(request: HttpRequest) -> JsonResponse:
    """Return tail of producer + spark logs."""
    lines = int(request.GET.get("lines", 50))
    return JsonResponse(PipelineManager.get_recent_logs(lines))


# ───── Saved dashboards ─────

@login_required
@require_http_methods(["GET", "POST"])
def dashboards(request: HttpRequest) -> JsonResponse:
    """List or create saved dashboards for the current user."""
    if request.method == "GET":
        items = [
            {
                "id": d.id,
                "title": d.name,
                "filters": {
                    "year": d.filter_year,
                    "sentiment": d.filter_sentiment,
                    "productId": d.filter_product,
                },
                "count": d.predictions_count,
                "lastViewed": d.last_viewed.strftime("%Y-%m-%d"),
            }
            for d in SavedDashboard.objects.filter(user=request.user)
        ]
        return JsonResponse({"data": items})

    import json

    try:
        payload = json.loads(request.body or b"{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    name = (payload.get("name") or "").strip()
    if not name:
        return JsonResponse({"error": "Name required"}, status=400)
    obj = SavedDashboard.objects.create(
        user=request.user,
        name=name,
        filter_year=payload.get("year") or None,
        filter_sentiment=payload.get("sentiment", "") or "",
        filter_product=payload.get("productId", "") or "",
        predictions_count=int(payload.get("count") or 0),
    )
    AuditLog.objects.create(
        user=request.user,
        action="dashboard.create",
        details={"id": obj.id, "name": name},
    )
    return JsonResponse({"id": obj.id, "name": obj.name}, status=201)


@login_required
@require_http_methods(["DELETE"])
def dashboard_delete(request: HttpRequest, dashboard_id: int) -> JsonResponse:
    """Delete a user's saved dashboard."""
    deleted, _ = SavedDashboard.objects.filter(id=dashboard_id, user=request.user).delete()
    if not deleted:
        return JsonResponse({"error": "Not found"}, status=404)
    AuditLog.objects.create(
        user=request.user,
        action="dashboard.delete",
        details={"id": dashboard_id},
    )
    return JsonResponse({"status": "deleted"})


@require_http_methods(["POST"])
def live_push(request: HttpRequest) -> JsonResponse:
    """
    Reçoit un batch de prédictions depuis Spark et le broadcast
    à tous les clients WebSocket connectés via Channel Layer.
    C'est le chemin direct 'Visualisation en continu' du schéma.
    """
    try:
        data = json.loads(request.body)
        predictions = data.get("predictions", [])
        batch_id    = data.get("batch_id", -1)
    except (json.JSONDecodeError, Exception):
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    from channels.layers import get_channel_layer
    from asgiref.sync import async_to_sync

    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        "live_feed",
        {
            "type":        "live.batch",   # → consumers.py::live_batch()
            "predictions": predictions,
            "batch_id":    batch_id,
        },
    )
    logger.info("live_push: broadcast %d predictions from Spark batch %s", len(predictions), batch_id)
    return JsonResponse({"pushed": len(predictions), "batch_id": batch_id})


@require_GET
def health(request: HttpRequest) -> JsonResponse:
    """Health probe endpoint."""
    mongo_ok = False
    try:
        mongo_ok = mongo.ping()
    except Exception:
        mongo_ok = False
    return JsonResponse({
        "status": "ok",
        "mongo": mongo_ok,
        "time": timezone.now().isoformat(),
    })
