"""SMTP delivery for the Info page contact form. Configure via environment variables."""

from __future__ import annotations

import os
import smtplib
import ssl
from email.message import EmailMessage


def contact_smtp_configured() -> bool:
    """Return True when all required SMTP and destination settings are present."""
    req = (
        (os.environ.get("FOOTBALL_CONTACT_TO_EMAIL") or "").strip(),
        (os.environ.get("FOOTBALL_SMTP_HOST") or "").strip(),
        (os.environ.get("FOOTBALL_SMTP_USER") or "").strip(),
        (os.environ.get("FOOTBALL_SMTP_PASSWORD") or "").strip(),
    )
    return all(req)


def send_contact_submission(*, name: str, reply_email: str, message: str) -> None:
    """
    Send an email to FOOTBALL_CONTACT_TO_EMAIL with Reply-To set to the visitor.
    Uses STARTTLS on port 587 by default, or SMTP_SSL when FOOTBALL_SMTP_SSL=1.
    """
    to_addr = os.environ["FOOTBALL_CONTACT_TO_EMAIL"].strip()
    smtp_host = os.environ["FOOTBALL_SMTP_HOST"].strip()
    smtp_port = int(os.environ.get("FOOTBALL_SMTP_PORT", "587"))
    user = os.environ["FOOTBALL_SMTP_USER"].strip()
    password = os.environ["FOOTBALL_SMTP_PASSWORD"]
    from_addr = (os.environ.get("FOOTBALL_SMTP_FROM") or user).strip()
    use_ssl = (os.environ.get("FOOTBALL_SMTP_SSL") or "").strip().lower() in (
        "1",
        "true",
        "yes",
    )

    subject_prefix = (os.environ.get("FOOTBALL_CONTACT_SUBJECT_PREFIX") or "[Football rankings]").strip()
    msg = EmailMessage()
    msg["Subject"] = f"{subject_prefix} Message from {name}"
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg["Reply-To"] = reply_email
    msg.set_content(
        f"Name: {name}\n"
        f"Visitor email (Reply-To): {reply_email}\n\n"
        f"{message}\n"
    )

    context = ssl.create_default_context()
    if use_ssl:
        with smtplib.SMTP_SSL(smtp_host, smtp_port, context=context, timeout=45) as smtp:
            smtp.login(user, password)
            smtp.send_message(msg)
        return

    with smtplib.SMTP(smtp_host, smtp_port, timeout=45) as smtp:
        smtp.ehlo()
        smtp.starttls(context=context)
        smtp.ehlo()
        smtp.login(user, password)
        smtp.send_message(msg)
