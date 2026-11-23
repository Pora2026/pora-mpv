"""Persistencia mínima y auditable del módulo Arandú.

Estas tablas almacenan conversación, reglas aprobadas y eventos de auditoría.
No otorgan al agente capacidad para modificar datos operativos de PORÁ.
"""

from datetime import datetime, timezone

from app.extensions import db


def _utcnow():
    return datetime.now(timezone.utc)


class AranduConversation(db.Model):
    __tablename__ = "arandu_conversations"

    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(160), nullable=False, default="Conversación compartida")
    created_by_user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    status = db.Column(db.String(20), nullable=False, default="active")
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=_utcnow)
    updated_at = db.Column(db.DateTime(timezone=True), nullable=False, default=_utcnow)


class AranduMessage(db.Model):
    __tablename__ = "arandu_messages"

    id = db.Column(db.Integer, primary_key=True)
    conversation_id = db.Column(
        db.Integer, db.ForeignKey("arandu_conversations.id"), nullable=False
    )
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    role = db.Column(db.String(20), nullable=False)
    content = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=_utcnow)


class AranduRule(db.Model):
    __tablename__ = "arandu_rules"

    id = db.Column(db.Integer, primary_key=True)
    rule_key = db.Column(db.String(120), nullable=False, unique=True)
    title = db.Column(db.String(200), nullable=False)
    content = db.Column(db.Text, nullable=False)
    status = db.Column(db.String(20), nullable=False, default="proposed")
    version = db.Column(db.Integer, nullable=False, default=1)
    proposed_by_user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    approved_by_user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=_utcnow)
    approved_at = db.Column(db.DateTime(timezone=True), nullable=True)


class AranduAuditEvent(db.Model):
    __tablename__ = "arandu_audit_events"

    id = db.Column(db.Integer, primary_key=True)
    conversation_id = db.Column(
        db.Integer, db.ForeignKey("arandu_conversations.id"), nullable=True
    )
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    event_type = db.Column(db.String(80), nullable=False)
    payload_json = db.Column(db.Text, nullable=False, default="{}")
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=_utcnow)


class AranduDocument(db.Model):
    __tablename__ = "arandu_documents"

    id = db.Column(db.Integer, primary_key=True)
    original_name = db.Column(db.String(255), nullable=False)
    stored_name = db.Column(db.String(255), nullable=False, unique=True)
    mime_type = db.Column(db.String(160), nullable=True)
    size_bytes = db.Column(db.Integer, nullable=False)
    sha256 = db.Column(db.String(64), nullable=False)
    uploaded_by_user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    status = db.Column(db.String(30), nullable=False, default="ready")
    extracted_text = db.Column(db.Text, nullable=False, default="")
    file_content = db.Column(db.LargeBinary, nullable=False)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=_utcnow)
