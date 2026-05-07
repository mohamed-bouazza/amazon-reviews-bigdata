"""Root URL configuration."""
from django.contrib import admin
from django.contrib.auth.views import LogoutView
from django.urls import include, path

from reviews_app import views as app_views

urlpatterns = [
    path("admin/", admin.site.urls),
    path("", app_views.app_view, name="root"),
    path("logout/", LogoutView.as_view(next_page="/"), name="logout"),
    path("api/", include("reviews_app.urls")),
]

handler404 = "reviews_app.views.not_found"
