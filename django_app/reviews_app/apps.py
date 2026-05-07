"""App configuration for reviews_app."""
from django.apps import AppConfig


class ReviewsAppConfig(AppConfig):
    """Reviews app config."""

    default_auto_field = "django.db.models.BigAutoField"
    name = "reviews_app"

    def ready(self) -> None:
        """Démarrer le consumer Kafka live au boot Django/Daphne."""
        import sys
        # Ne pas lancer pendant migrate, collectstatic, shell, etc.
        excluded = {"migrate", "makemigrations", "collectstatic", "shell", "createsuperuser"}
        if len(sys.argv) > 1 and sys.argv[1] in excluded:
            return
        from .kafka_live_consumer import start_kafka_live_consumer
        start_kafka_live_consumer()
