"""Persistent Telegram admin monitoring utilities for ZEN TOPUP.

This module is intentionally self-contained so it can be dropped into an
existing python-telegram-bot (v20+) async project without breaking current
payment/order handlers.

Integration pattern:
1) Create one `OrderMonitor` instance at startup.
2) Call `create_pending_order()` when user reaches payment step.
3) Call `update_order_status(..., status=OrderStatus.VERIFYING_PAYMENT)` when
   the user submits a transaction reference.
4) Call `update_order_status(..., status=OrderStatus.COMPLETED/FAILED)` from
   your existing payment verification result handlers.
5) Run `timeout_pending_orders()` in a background JobQueue task to
   automatically mark abandoned orders as TIMEOUT.

Timeout is configured to **6 hours** (was 30 minutes).
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Optional

from telegram import Bot
from telegram.constants import ParseMode

logger = logging.getLogger(__name__)

# Payment transaction timeout updated from 30 minutes to 6 hours.
PAYMENT_TX_TIMEOUT_SECONDS = 6 * 60 * 60

# Logs account: receives ALL order lifecycle events.
LOGS_ADMIN_CHAT_ID = 7317467437

# Admin moderation command rename: use /unban (not /unrestrict).
ADMIN_UNBAN_COMMAND = "/unban"


class OrderStatus(str, Enum):
    """Allowed monitoring states for an order lifecycle."""

    WAITING_PAYMENT = "WAITING PAYMENT"
    VERIFYING_PAYMENT = "VERIFYING PAYMENT"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"


@dataclass(slots=True)
class OrderRecord:
    order_id: str
    user_id: int
    username: str
    game: str
    package: str
    amount: str
    status: OrderStatus
    timestamp: str
    admin_message_id: Optional[int] = None
    transaction_number: Optional[str] = None
    failure_reason: Optional[str] = None
    completion_time: Optional[str] = None
    metadata_json: str = "{}"


class OrderMonitor:
    """Persists order monitoring data and keeps a single admin message in sync.

    Routing policy implemented for your two-admin setup:
    - logs_chat_id: receives all lifecycle updates (pending/verifying/failed/timeout/completed)
    - main_chat_id: receives only completed orders as a separate notification
    """

    def __init__(
        self,
        db_path: str | Path,
        logs_chat_id: int,
        bot: Bot,
        main_chat_id: Optional[int] = None,
    ):
        self.db_path = str(db_path)
        self.logs_chat_id = logs_chat_id
        self.main_chat_id = main_chat_id
        self.bot = bot
        self._db_lock = asyncio.Lock()
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS monitored_orders (
                    order_id TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    username TEXT NOT NULL,
                    game TEXT NOT NULL,
                    package TEXT NOT NULL,
                    amount TEXT NOT NULL,
                    status TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    admin_message_id INTEGER,
                    transaction_number TEXT,
                    failure_reason TEXT,
                    completion_time TEXT,
                    metadata_json TEXT NOT NULL DEFAULT '{}'
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_orders_status ON monitored_orders(status)"
            )

    async def create_pending_order(
        self,
        *,
        order_id: str,
        user_id: int,
        username: str,
        game: str,
        package: str,
        amount: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> OrderRecord:
        """Create persistent pending order + send first admin monitoring message.

        Call this from the existing payment-step handler right after creating the
        order in your current order system.
        """
        now = _utc_now_iso()
        username_fmt = username if username.startswith("@") else f"@{username}"

        order = OrderRecord(
            order_id=order_id,
            user_id=user_id,
            username=username_fmt,
            game=game,
            package=package,
            amount=amount,
            status=OrderStatus.WAITING_PAYMENT,
            timestamp=now,
            metadata_json=json.dumps(metadata or {}, ensure_ascii=False),
        )

        # Send admin message first; store message_id for same-message edits later.
        sent = await self.bot.send_message(
            chat_id=self.logs_chat_id,
            text=self._render_admin_message(order),
            parse_mode=ParseMode.HTML,
        )
        order.admin_message_id = sent.message_id

        async with self._db_lock:
            with self._conn() as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO monitored_orders (
                        order_id, user_id, username, game, package, amount,
                        status, timestamp, admin_message_id, transaction_number,
                        failure_reason, completion_time, metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        order.order_id,
                        order.user_id,
                        order.username,
                        order.game,
                        order.package,
                        order.amount,
                        order.status.value,
                        order.timestamp,
                        order.admin_message_id,
                        order.transaction_number,
                        order.failure_reason,
                        order.completion_time,
                        order.metadata_json,
                    ),
                )

        return order

    async def update_order_status(
        self,
        order_id: str,
        *,
        status: OrderStatus,
        transaction_number: Optional[str] = None,
        failure_reason: Optional[str] = None,
    ) -> Optional[OrderRecord]:
        """Update DB status and edit the same admin message for that order.

        Call points:
        - on tx submission -> VERIFYING_PAYMENT
        - on verification success -> COMPLETED
        - on verification failure -> FAILED
        """
        async with self._db_lock:
            order = self._get_order(order_id)
            if not order:
                logger.warning("update_order_status: order not found: %s", order_id)
                return None

            order.status = status
            if transaction_number:
                order.transaction_number = transaction_number
            if failure_reason:
                order.failure_reason = failure_reason
            if status in (OrderStatus.COMPLETED, OrderStatus.FAILED, OrderStatus.TIMEOUT):
                order.completion_time = _utc_now_hhmm()

            with self._conn() as conn:
                conn.execute(
                    """
                    UPDATE monitored_orders
                    SET status=?, transaction_number=?, failure_reason=?, completion_time=?
                    WHERE order_id=?
                    """,
                    (
                        order.status.value,
                        order.transaction_number,
                        order.failure_reason,
                        order.completion_time,
                        order.order_id,
                    ),
                )

        if order.admin_message_id:
            await self.bot.edit_message_text(
                chat_id=self.logs_chat_id,
                message_id=order.admin_message_id,
                text=self._render_admin_message(order),
                parse_mode=ParseMode.HTML,
            )


        # Send completed orders to the main account only when completion happens.
        if status == OrderStatus.COMPLETED and self.main_chat_id:
            await self.bot.send_message(
                chat_id=self.main_chat_id,
                text=self._render_main_completed_message(order),
                parse_mode=ParseMode.HTML,
            )

        return order

    async def timeout_pending_orders(self) -> int:
        """Mark stale WAITING PAYMENT orders as TIMEOUT and edit admin messages.

        Schedule this in a periodic JobQueue task, for example every 5 minutes.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=PAYMENT_TX_TIMEOUT_SECONDS)
        async with self._db_lock:
            with self._conn() as conn:
                rows = conn.execute(
                    """
                    SELECT * FROM monitored_orders
                    WHERE status=? AND timestamp < ?
                    """,
                    (OrderStatus.WAITING_PAYMENT.value, cutoff.isoformat()),
                ).fetchall()

        count = 0
        for row in rows:
            await self.update_order_status(
                row["order_id"],
                status=OrderStatus.TIMEOUT,
                failure_reason="No transaction number submitted within 6 hours.",
            )
            count += 1

        return count

    async def export_orders_backup(self, backup_path: str | Path) -> Path:
        """Export all monitored orders to JSON backup.

        JSON format is intentionally explicit and versioned so future imports stay
        stable across bot updates.
        """
        out_path = Path(backup_path)
        async with self._db_lock:
            with self._conn() as conn:
                rows = conn.execute("SELECT * FROM monitored_orders ORDER BY timestamp ASC").fetchall()

        payload = {
            "schema": "zen_topup_order_monitor_backup",
            "version": 1,
            "exported_at": _utc_now_iso(),
            "orders": [dict(row) for row in rows],
        }

        out_path.parent.mkdir(parents=True, exist_ok=True)
        with NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=str(out_path.parent)) as tmp:
            json.dump(payload, tmp, ensure_ascii=False, indent=2)
            tmp_path = Path(tmp.name)
        tmp_path.replace(out_path)
        return out_path

    async def import_orders_backup(self, backup_path: str | Path) -> int:
        """Import monitored orders from JSON backup.

        Supports both legacy and current export structures:
        - {"orders": [...]} (preferred)
        - {"monitored_orders": [...]} (legacy compatibility)
        - [...] (raw array fallback)
        """
        in_path = Path(backup_path)
        data = json.loads(in_path.read_text(encoding="utf-8"))

        if isinstance(data, dict):
            raw_orders = data.get("orders")
            if raw_orders is None:
                raw_orders = data.get("monitored_orders", [])
        elif isinstance(data, list):
            raw_orders = data
        else:
            raise ValueError("Unsupported backup structure; expected object or array.")

        count = 0
        async with self._db_lock:
            with self._conn() as conn:
                for raw in raw_orders:
                    if not isinstance(raw, dict):
                        continue
                    order = self._normalize_import_order(raw)
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO monitored_orders (
                            order_id, user_id, username, game, package, amount,
                            status, timestamp, admin_message_id, transaction_number,
                            failure_reason, completion_time, metadata_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            order["order_id"],
                            order["user_id"],
                            order["username"],
                            order["game"],
                            order["package"],
                            order["amount"],
                            order["status"],
                            order["timestamp"],
                            order["admin_message_id"],
                            order["transaction_number"],
                            order["failure_reason"],
                            order["completion_time"],
                            order["metadata_json"],
                        ),
                    )
                    count += 1
        return count

    def _normalize_import_order(self, raw: dict[str, Any]) -> dict[str, Any]:
        """Normalize backup order fields to DB schema for robust imports."""
        status = str(raw.get("status", OrderStatus.WAITING_PAYMENT.value))
        if status not in {s.value for s in OrderStatus}:
            status = OrderStatus.WAITING_PAYMENT.value

        return {
            "order_id": str(raw.get("order_id", "")),
            "user_id": int(raw.get("user_id", 0)),
            "username": str(raw.get("username", "@unknown")),
            "game": str(raw.get("game", "Unknown")),
            "package": str(raw.get("package", "Unknown")),
            "amount": str(raw.get("amount", "0")),
            "status": status,
            "timestamp": str(raw.get("timestamp", _utc_now_iso())),
            "admin_message_id": raw.get("admin_message_id"),
            "transaction_number": raw.get("transaction_number"),
            "failure_reason": raw.get("failure_reason"),
            "completion_time": raw.get("completion_time"),
            "metadata_json": raw.get("metadata_json")
            if isinstance(raw.get("metadata_json"), str)
            else json.dumps(raw.get("metadata", {}), ensure_ascii=False),
        }

    def _get_order(self, order_id: str) -> Optional[OrderRecord]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM monitored_orders WHERE order_id=?", (order_id,)
            ).fetchone()
        if not row:
            return None
        return OrderRecord(
            order_id=row["order_id"],
            user_id=row["user_id"],
            username=row["username"],
            game=row["game"],
            package=row["package"],
            amount=row["amount"],
            status=OrderStatus(row["status"]),
            timestamp=row["timestamp"],
            admin_message_id=row["admin_message_id"],
            transaction_number=row["transaction_number"],
            failure_reason=row["failure_reason"],
            completion_time=row["completion_time"],
            metadata_json=row["metadata_json"],
        )

    def _render_admin_message(self, order: OrderRecord) -> str:
        title = {
            OrderStatus.WAITING_PAYMENT: "🟡 <b>PENDING ORDER</b>",
            OrderStatus.VERIFYING_PAYMENT: "🔄 <b>VERIFYING PAYMENT</b>",
            OrderStatus.COMPLETED: "✅ <b>COMPLETED</b>",
            OrderStatus.FAILED: "❌ <b>FAILED</b>",
            OrderStatus.TIMEOUT: "⌛ <b>TIMEOUT</b>",
        }[order.status]

        lines = [
            title,
            "",
            f"<b>Order ID:</b> {order.order_id}",
            f"<b>User:</b> {order.username}",
            f"<b>User ID:</b> {order.user_id}",
            "",
            f"<b>Game:</b> {order.game}",
            f"<b>Package:</b> {order.package}",
            f"<b>Amount:</b> {order.amount}",
            "",
            f"<b>Status:</b> {order.status.value}",
            f"<b>Time:</b> {_hhmm_from_iso(order.timestamp)}",
        ]

        if order.transaction_number:
            lines += ["", f"<b>Transaction:</b> {order.transaction_number}"]
        if order.completion_time:
            lines += [f"<b>Completed At:</b> {order.completion_time}"]
        if order.failure_reason:
            lines += [f"<b>Reason:</b> {order.failure_reason}"]

        return "\n".join(lines)



    def _render_main_completed_message(self, order: OrderRecord) -> str:
        """Compact completion-only alert for the main admin account."""
        return "\n".join(
            [
                "✅ <b>COMPLETED ORDER</b>",
                "",
                f"<b>Order ID:</b> {order.order_id}",
                f"<b>User:</b> {order.username}",
                f"<b>Game:</b> {order.game}",
                f"<b>Package:</b> {order.package}",
                f"<b>Amount:</b> {order.amount}",
                f"<b>Transaction:</b> {order.transaction_number or '-'}",
                f"<b>Completed At:</b> {order.completion_time or _utc_now_hhmm()}",
            ]
        )

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_now_hhmm() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M")


def _hhmm_from_iso(ts: str) -> str:
    return datetime.fromisoformat(ts).strftime("%H:%M")
