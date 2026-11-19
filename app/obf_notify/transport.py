"""Transport los van detectie. Nu: e-mail. Later: ntfy, Pushover, WhatsApp, ... via dezelfde ``send``."""

from __future__ import annotations

import smtplib
import ssl
from email.message import EmailMessage
from typing import Protocol

from .message import Message, redact
from .settings import NotifyConfig, read_password

SMTP_TIMEOUT = 20.0


def tls_context() -> ssl.SSLContext:
    """Volledig geverifieerde TLS-context (certificaat- én hostnamecontrole), met een expliciete CA-bundel.

    De python.org-Python van de venv heeft geen eigen wortelcertificaten (``cafile=None`` en de
    ``etc/openssl/cert.pem`` ontbreekt); dan faalt elke verificatie met "unable to get local issuer
    certificate". ``certifi`` staat in requirements.txt en wordt hier expliciet als bron gebruikt, zodat het
    resultaat niet afhangt van ``SSL_CERT_FILE`` of een shell -- de LaunchAgent heeft die niet. Zonder
    certifi valt dit terug op de standaardcontext, die dan net zo streng faalt; er is nooit een bypass.
    """
    try:
        import certifi  # noqa: PLC0415

        context = ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        context = ssl.create_default_context()
    return context


class TransportError(Exception):
    """Een mislukte verzending. De tekst is al gezuiverd en veilig om te loggen."""


class Transport(Protocol):
    name: str

    def send(self, message: Message) -> None: ...


class EmailTransport:
    name = "email"

    def __init__(self, config: NotifyConfig, *, password: str | None = None, smtp_factory=None):
        self.config = config
        self._password = password
        self._factory = smtp_factory

    def _build(self, message: Message, recipient: str | None) -> EmailMessage:
        mail = EmailMessage()
        # Kop-injectie onmogelijk: onderwerp en adressen zonder regeleinden.
        mail["Subject"] = " ".join(message.subject.split())
        mail["From"] = " ".join(self.config.sender.split())
        mail["To"] = " ".join((recipient or self.config.recipient).split())
        mail.set_content(message.body)
        return mail

    def voorbereid(self):
        """Controleer lokaal vóór een duurzame verzendintentie; opent geen SMTP-verbinding."""
        config = self.config
        if not config.transport_ready:
            detail = "; ".join(config.problems) or "smtp_host, sender en recipient zijn verplicht"
            raise TransportError(f"e-mail niet geconfigureerd ({detail})")
        password = self._password if self._password is not None else read_password(config)
        if config.smtp_user and not password:
            raise TransportError("geen SMTP-wachtwoord gevonden (sleutelhanger of omgeving)")
        def verzend(message, *, recipient=None):
            return self._send_met_wachtwoord(message, recipient=recipient, password=password)
        return verzend

    def send(self, message: Message, *, recipient: str | None = None) -> None:
        return self.voorbereid()(message, recipient=recipient)

    def _send_met_wachtwoord(self, message: Message, *, recipient: str | None, password: str | None) -> None:
        config = self.config
        mail = self._build(message, recipient)
        try:
            if self._factory is not None:
                client = self._factory(config.smtp_host, config.smtp_port, SMTP_TIMEOUT)
            elif config.smtp_security == "ssl":
                client = smtplib.SMTP_SSL(config.smtp_host, config.smtp_port, timeout=SMTP_TIMEOUT,
                                          context=tls_context())
            else:
                client = smtplib.SMTP(config.smtp_host, config.smtp_port, timeout=SMTP_TIMEOUT)
            with client:
                if config.smtp_security == "starttls":
                    client.starttls(context=tls_context())
                if config.smtp_user:
                    client.login(config.smtp_user, password)
                client.send_message(mail)
        except TransportError:
            raise
        except Exception as exc:  # noqa: BLE001 -- alles wordt een veilige, gezuiverde fout
            detail = redact(f"{type(exc).__name__}: {exc}", 160)
            for secret in (password, config.smtp_user):
                if secret:
                    detail = detail.replace(secret, "[verborgen]")
            raise TransportError(f"verzenden mislukt ({detail})") from None
