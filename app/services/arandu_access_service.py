"""Control de acceso explícito y cerrado para el MVP de Arandú."""

from flask import current_app


def allowed_usernames() -> set[str]:
    configured = current_app.config.get("ARANDU_ALLOWED_USERS", "")
    if isinstance(configured, str):
        values = configured.split(",")
    else:
        values = configured or []
    return {str(value).strip().casefold() for value in values if str(value).strip()}


def user_can_access_arandu(user) -> bool:
    username = getattr(user, "username", "")
    return bool(username) and username.strip().casefold() in allowed_usernames()
