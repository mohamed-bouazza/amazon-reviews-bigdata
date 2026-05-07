"""Page views (templates)."""
from __future__ import annotations

import json
import logging

from django.contrib.auth.models import Group, User
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render

logger = logging.getLogger(__name__)


def app_view(request: HttpRequest) -> HttpResponse:
    """Render the SPA shell — React handles auth and internal routing."""
    authenticated = request.user.is_authenticated
    return render(
        request,
        "app.html",
        {
            "initial_page": "dashboard" if authenticated else "login",
            "user_groups_json": json.dumps(
                list(request.user.groups.values_list("name", flat=True))
                if authenticated else []
            ),
        },
    )


def not_found(request: HttpRequest, exception=None) -> HttpResponse:
    """Render 404 page."""
    return render(request, "404.html", status=404)
