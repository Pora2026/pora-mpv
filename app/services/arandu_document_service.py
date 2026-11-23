"""Carga segura y extracción acotada de documentos para Arandú."""

import hashlib
import io
import json
import zipfile
from pathlib import Path
from uuid import uuid4

from werkzeug.utils import secure_filename
from sqlalchemy import func

from app.extensions import db
from app.models.arandu import AranduDocument


MAX_DOCUMENT_BYTES = 15 * 1024 * 1024
MAX_TOTAL_DOCUMENT_BYTES = 60 * 1024 * 1024
MAX_EXTRACTED_CHARS = 120_000
ALLOWED_EXTENSIONS = {
    ".txt", ".md", ".csv", ".json", ".pdf", ".docx", ".xlsx",
    ".png", ".jpg", ".jpeg", ".webp",
}
BLOCKED_EXTENSIONS = {
    ".exe", ".com", ".bat", ".cmd", ".ps1", ".sh", ".py", ".js",
    ".jar", ".msi", ".dll", ".scr", ".vbs", ".xlsm", ".docm",
}
IMAGE_MIME_TYPES = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
}


class DocumentValidationError(ValueError):
    pass


def _validate_office_archive(content):
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            members = archive.infolist()
            if len(members) > 5000 or sum(item.file_size for item in members) > 50 * 1024 * 1024:
                raise DocumentValidationError("El documento comprimido excede los límites seguros.")
    except zipfile.BadZipFile as exc:
        raise DocumentValidationError("El documento Office no es válido.") from exc


def _extract_text(extension, content):
    if extension in {".txt", ".md", ".csv"}:
        return content.decode("utf-8-sig", errors="replace")
    if extension == ".json":
        parsed = json.loads(content.decode("utf-8-sig"))
        return json.dumps(parsed, ensure_ascii=False, indent=2)
    if extension == ".pdf":
        from pypdf import PdfReader

        reader = PdfReader(io.BytesIO(content))
        if len(reader.pages) > 300:
            raise DocumentValidationError("El PDF supera el máximo de 300 páginas.")
        return "\n\n".join(page.extract_text() or "" for page in reader.pages)
    if extension == ".docx":
        from docx import Document

        _validate_office_archive(content)
        document = Document(io.BytesIO(content))
        return "\n".join(paragraph.text for paragraph in document.paragraphs)
    if extension == ".xlsx":
        import openpyxl

        _validate_office_archive(content)
        workbook = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
        lines = []
        for sheet in workbook.worksheets:
            lines.append(f"[Hoja: {sheet.title}]")
            for row in sheet.iter_rows(values_only=True):
                lines.append(" | ".join("" if value is None else str(value) for value in row))
                if sum(len(item) for item in lines) >= MAX_EXTRACTED_CHARS:
                    break
        workbook.close()
        return "\n".join(lines)
    # Las imágenes se conservan, pero no se ejecuta OCR implícito.
    return ""


def save_document(file_storage, user_id):
    if file_storage is None:
        raise DocumentValidationError("No se recibió un archivo.")
    original_name = secure_filename(file_storage.filename or "")
    if not original_name:
        raise DocumentValidationError("El archivo no tiene un nombre válido.")
    extension = Path(original_name).suffix.lower()
    if extension in BLOCKED_EXTENSIONS or extension not in ALLOWED_EXTENSIONS:
        raise DocumentValidationError("El tipo de archivo no está permitido para el MVP.")

    content = file_storage.read(MAX_DOCUMENT_BYTES + 1)
    if not content:
        raise DocumentValidationError("El archivo está vacío.")
    if len(content) > MAX_DOCUMENT_BYTES:
        raise DocumentValidationError("El archivo supera el máximo de 15 MB.")
    if extension in {".png", ".jpg", ".jpeg", ".webp"} and len(content) > 5 * 1024 * 1024:
        raise DocumentValidationError("Las imágenes no pueden superar 5 MB.")
    if extension == ".png" and not content.startswith(b"\x89PNG\r\n\x1a\n"):
        raise DocumentValidationError("El contenido no corresponde a una imagen PNG válida.")
    if extension in {".jpg", ".jpeg"} and not content.startswith(b"\xff\xd8\xff"):
        raise DocumentValidationError("El contenido no corresponde a una imagen JPEG válida.")
    if extension == ".webp" and not (
        content.startswith(b"RIFF") and content[8:12] == b"WEBP"
    ):
        raise DocumentValidationError("El contenido no corresponde a una imagen WEBP válida.")
    current_total = int(
        db.session.query(func.coalesce(func.sum(AranduDocument.size_bytes), 0)).scalar()
        or 0
    )
    if current_total + len(content) > MAX_TOTAL_DOCUMENT_BYTES:
        raise DocumentValidationError(
            "Los documentos de Arandú alcanzaron el límite total de 60 MB."
        )

    try:
        extracted = _extract_text(extension, content)[:MAX_EXTRACTED_CHARS]
    except Exception as exc:
        raise DocumentValidationError(f"No se pudo leer el documento: {exc}") from exc

    stored_name = f"{uuid4().hex}{extension}"
    document = AranduDocument(
        original_name=original_name,
        stored_name=stored_name,
        mime_type=IMAGE_MIME_TYPES.get(extension, file_storage.mimetype),
        size_bytes=len(content),
        sha256=hashlib.sha256(content).hexdigest(),
        uploaded_by_user_id=user_id,
        status="ready" if extracted else "stored_without_text",
        extracted_text=extracted,
        file_content=content,
    )
    try:
        db.session.add(document)
        db.session.commit()
    except Exception:
        db.session.rollback()
        raise
    return document
