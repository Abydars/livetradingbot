"""
telegram_listener.py — Listens to Telegram channels for trading signals.

Uses Telethon with StringSession so no session file is written to disk.
The session string is persisted back to the DB via on_session_save callback.
"""
import logging
import re
import time
from typing import Callable, Awaitable

from telethon import TelegramClient, events
from telethon.errors import AuthKeyError, AuthKeyUnregisteredError, UserDeactivatedBanError
from telethon.sessions import StringSession

logger = logging.getLogger("telegram_listener")

# Priority-1: pair with separator  e.g. "KAT USDT", "BTC/USDT", "ETH-USDT"
_PAIR_SEP_RE = re.compile(
    r'\b([A-Z]{2,10})\s*[/\-_ ]\s*(USDT|BTC|ETH|BNB|BUSD)\b',
    re.IGNORECASE,
)
# Priority-2: direct pair already joined  e.g. "KATUSDT", "BTCUSDT"
_PAIR_DIRECT_RE = re.compile(
    r'\b([A-Z]{2,10}(USDT|BTC|ETH|BNB|BUSD))\b',
    re.IGNORECASE,
)
_DIRECTION_RE = re.compile(r'\b(LONG|SHORT|BUY|SELL)\b', re.IGNORECASE)
_STABLECOIN_BASE = {"USDT", "BUSD"}   # bases that are obviously not a real coin

_MAX_SIGNALS = 20


class TelegramListener:
    def __init__(
        self,
        api_id: int,
        api_hash: str,
        session_str: str,
        channel_ids: list[str],
        on_signal_update: Callable[[list], Awaitable[None]],
        on_session_save: Callable[[str], Awaitable[None]],
    ):
        self._api_id = api_id
        self._api_hash = api_hash
        self._session_str = session_str
        self._channel_ids = channel_ids
        self._on_signal_update = on_signal_update
        self._on_session_save = on_session_save
        self._signals: list[dict] = []
        self._client: TelegramClient | None = None

    @property
    def signals(self) -> list[dict]:
        return list(self._signals)

    async def start(self) -> None:
        self._client = TelegramClient(
            StringSession(self._session_str),
            self._api_id,
            self._api_hash,
        )

        try:
            await self._client.connect()
        except (AuthKeyError, AuthKeyUnregisteredError, UserDeactivatedBanError) as exc:
            logger.warning(
                "Telegram not authenticated — use /api/telegram/send_code to authenticate (%s)", exc
            )
            self._client = None
            return

        if not await self._client.is_user_authorized():
            logger.warning(
                "Telegram not authenticated — use /api/telegram/send_code to authenticate"
            )
            await self._client.disconnect()
            self._client = None
            return

        # Persist the (potentially refreshed) session string
        await self._on_session_save(self._client.session.save())

        # Resolve channel identifiers to entity objects so Telethon never
        # tries to look up "@me" (which does not exist as a public username).
        me = await self._client.get_me()
        me_username = (me.username or "").lower() if me else ""

        resolved = []
        for entry in self._channel_ids:
            entry = entry.strip()
            if not entry:
                continue
            try:
                if entry.lower() == "me":
                    resolved.append(me)
                else:
                    resolved.append(await self._client.get_entity(entry))
            except Exception as exc:
                logger.warning("Could not resolve Telegram channel %r: %s", entry, exc)

        @self._client.on(events.NewMessage(chats=resolved if resolved else None))
        async def _on_new_message(event: events.NewMessage.Event) -> None:
            try:
                chat = await event.get_chat()
                uname = getattr(chat, "username", None)
                if me_username and (uname or "").lower() == me_username:
                    channel_name = "Saved Messages"
                else:
                    channel_name = uname or getattr(chat, "title", None) or "me"
            except Exception:
                channel_name = "unknown"

            signal = self._parse_signal(event.raw_text or "", channel_name)
            if signal is None:
                return

            self._upsert_signal(signal)
            await self._on_signal_update(self.signals)

        try:
            await self._client.run_until_disconnected()
        except (AuthKeyError, AuthKeyUnregisteredError) as exc:
            logger.warning("Telegram session invalidated: %s", exc)

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.disconnect()
            self._client = None

    # ------------------------------------------------------------------
    # Signal parsing
    # ------------------------------------------------------------------

    def _parse_signal(self, text: str, channel: str) -> dict | None:
        # --- symbol extraction (priority 1: separated pair) ---
        symbol: str | None = None
        m = _PAIR_SEP_RE.search(text)
        if m:
            base  = m.group(1).upper()
            quote = m.group(2).upper()
            symbol = base + quote
        else:
            m = _PAIR_DIRECT_RE.search(text)
            if m:
                symbol = m.group(1).upper()

        if not symbol:
            logger.debug("Telegram parse failed | channel=%s | text=%s", channel, text[:100])
            return None

        # strip quote currency then re-attach cleanly (handles double-quote edge cases)
        for q in ("USDT", "BTC", "ETH", "BNB", "BUSD"):
            if symbol.endswith(q) and symbol != q:
                base_part = symbol[: -len(q)]
                symbol = base_part + q
                break

        # validation: 4-12 chars total, base must not be empty or quote-only
        base_only = symbol
        for q in ("USDT", "BTC", "ETH", "BNB", "BUSD"):
            if base_only.endswith(q):
                base_only = base_only[: -len(q)]
                break
        if not (4 <= len(symbol) <= 12) or base_only in _STABLECOIN_BASE or not base_only:
            logger.debug("Telegram parse failed | channel=%s | text=%s", channel, text[:100])
            return None

        # --- direction extraction ---
        direction_match = _DIRECTION_RE.search(text)
        if not direction_match:
            logger.debug("Telegram parse failed | channel=%s | text=%s", channel, text[:100])
            return None

        raw_dir = direction_match.group(1).upper()
        bias = "LONG" if raw_dir in ("LONG", "BUY") else "SHORT"

        return {
            "symbol": symbol,
            "score": 1.0,
            "change": 0.0,
            "volume": 0,
            "price": 0.0,
            "bias": bias,
            "vol_surge": 1.0,
            "momentum": 0.0,
            "atr_pct": 0.0,
            "htf_bias": "",
            "scanner_type": channel,
            "ts": int(time.time()),
        }

    # ------------------------------------------------------------------
    # Signal list management
    # ------------------------------------------------------------------

    def _upsert_signal(self, signal: dict) -> None:
        symbol = signal["symbol"]
        for i, existing in enumerate(self._signals):
            if existing["symbol"] == symbol:
                self._signals[i] = signal
                break
        else:
            self._signals.insert(0, signal)

        # Keep newest-first order and trim to max
        self._signals.sort(key=lambda s: s["ts"], reverse=True)
        self._signals = self._signals[:_MAX_SIGNALS]
