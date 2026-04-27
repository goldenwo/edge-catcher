"""SMTP adapter — sends email via smtplib."""
from __future__ import annotations

import json
import smtplib
import time
from email.message import EmailMessage

from edge_catcher.notifications.envelope import DeliveryResult, Notification


class SMTPChannel:
	"""Email delivery via SMTP with STARTTLS + password auth."""

	def __init__(
		self,
		name: str,
		host: str,
		port: int,
		user: str,
		password: str,
		from_addr: str,
		to: list[str],
		use_tls: bool = True,
		timeout_seconds: float = 10.0,
	) -> None:
		self.name = name
		self.host = host
		self.port = port
		self.user = user
		self.password = password
		self.from_addr = from_addr
		self.to = list(to)
		self.use_tls = use_tls
		self.timeout_seconds = timeout_seconds

	def send(self, notification: Notification) -> DeliveryResult:
		t0 = time.perf_counter()
		smtp_conn = None
		try:
			# 1. Build the body string.
			text = notification.body
			if notification.payload is not None:
				text = f"{text}\n\n{json.dumps(notification.payload, indent=2)}"

			# 2-6. Build EmailMessage in locked sequence (per spec §5.4).
			msg = EmailMessage()
			msg["Subject"] = f"[{notification.severity}] {notification.title}"
			msg["From"] = self.from_addr
			msg["To"] = ", ".join(self.to)
			msg.set_content(text)

			smtp_conn = smtplib.SMTP(self.host, self.port, timeout=self.timeout_seconds)
			if self.use_tls:
				smtp_conn.starttls()
			smtp_conn.login(self.user, self.password)
			smtp_conn.send_message(msg, from_addr=self.from_addr, to_addrs=self.to)
		except (smtplib.SMTPException, OSError, ValueError) as exc:
			return DeliveryResult(
				channel_name=self.name,
				success=False,
				error=repr(exc),
				latency_ms=(time.perf_counter() - t0) * 1000,
			)
		finally:
			if smtp_conn is not None:
				try:
					smtp_conn.quit()
				except Exception:
					pass  # don't override the original error

		return DeliveryResult(
			channel_name=self.name,
			success=True,
			latency_ms=(time.perf_counter() - t0) * 1000,
		)
