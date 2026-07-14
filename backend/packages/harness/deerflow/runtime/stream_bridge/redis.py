"""Redis Streams bridge with Consumer Group support.

Provides durable, cross-process message routing using Redis Streams.
Each run maps to a Redis Stream + Consumer Group. XREADGROUP ensures
exactly-one-worker delivery per consumer group. XACK confirms
micro-step completion.

Usage::

    from deerflow.runtime.stream_bridge.redis import RedisStreamBridge

    bridge = RedisStreamBridge(redis_url="redis://localhost:6379/0")
    async with bridge:
        await bridge.publish(run_id, "metadata", {"key": "value"})
        async for event in bridge.subscribe(run_id):
            ...
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import AsyncIterator
from typing import Any

from .base import END_SENTINEL, HEARTBEAT_SENTINEL, StreamBridge, StreamEvent

logger = logging.getLogger(__name__)

# Attempt to import redis.asyncio; fail gracefully if not installed.
try:
    import redis.asyncio as aioredis

    _REDIS_AVAILABLE = True
except ImportError:
    aioredis = None  # type: ignore[assignment]
    _REDIS_AVAILABLE = False


class RedisStreamBridge(StreamBridge):
    """Redis Streams bridge with Consumer Group support.

    Architecture:
        - Each ``run_id`` maps to a Redis Stream key ``sworm:stream:{run_id}``.
        - Each consumer group maps to ``sworm:group:{run_id}``.
        - ``XREADGROUP`` ensures exactly-one-worker delivery within a group.
        - ``XACK`` is called automatically after yielding each event, and
          also exposed as an explicit method for manual acknowledgment.

    Args:
        redis_url: Redis connection URL (e.g. ``redis://localhost:6379/0``).
        consumer_name: Unique name for this consumer within the group.
            Defaults to a process-unique identifier.
        block_ms: Block timeout in milliseconds for ``XREADGROUP``.
        claim_idle_ms: Minimum idle time (ms) before ``XAUTOCLAIM`` picks
            up abandoned messages from crashed consumers.
        stream_maxlen: Maximum entries to retain per stream (approximate).
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        *,
        consumer_name: str | None = None,
        block_ms: int = 5000,
        claim_idle_ms: int = 30000,
        queue_maxsize: int = 256,
        max_connections: int | None = None,
        stream_ttl_seconds: int = 86400,
    ) -> None:
        if not _REDIS_AVAILABLE:
            raise ImportError("redis[asyncio] is required for RedisStreamBridge. Install it with: pip install redis")

        self._redis_url = redis_url
        self._redis: aioredis.Redis | None = None
        self._consumer_name = consumer_name or f"sworm-consumer-{id(self):x}"
        self._block_ms = block_ms
        self._claim_idle_ms = claim_idle_ms
        self._stream_maxlen = queue_maxsize
        self._max_connections = max_connections
        self._stream_ttl_seconds = stream_ttl_seconds

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def _ensure_redis(self) -> aioredis.Redis:
        """Lazily create the Redis connection pool."""
        if self._redis is None:
            pool_kwargs: dict[str, Any] = {
                "decode_responses": True,
                "socket_connect_timeout": 5,
                "socket_keepalive": True,
                "retry_on_timeout": True,
            }
            if self._max_connections is not None:
                pool_kwargs["max_connections"] = self._max_connections
            self._redis = aioredis.from_url(self._redis_url, **pool_kwargs)
        return self._redis

    async def close(self) -> None:
        """Close the Redis connection."""
        if self._redis is not None:
            try:
                await self._redis.aclose()
            except Exception:
                logger.debug("Error closing Redis connection", exc_info=True)
            self._redis = None

    # ── StreamBridge API ──────────────────────────────────────────────────

    def _stream_key(self, run_id: str) -> str:
        return f"sworm:stream:{run_id}"

    def _group_name(self, run_id: str) -> str:
        return f"sworm:group:{run_id}"

    async def _ensure_group(self, run_id: str) -> None:
        """Create the consumer group if it does not exist (idempotent)."""
        redis = await self._ensure_redis()
        try:
            await redis.xgroup_create(
                self._stream_key(run_id),
                self._group_name(run_id),
                id="0",
                mkstream=True,
            )
            logger.debug("Created consumer group for run %s", run_id)
        except aioredis.ResponseError as exc:
            # "BUSYGROUP Consumer Group name already exists" — expected
            if "BUSYGROUP" not in str(exc):
                raise

    async def publish(self, run_id: str, event: str, data: Any) -> None:
        """XADD an event to the run's stream.

        Refreshes the stream key TTL so retained SSE replay buffers are
        eventually reclaimed even if ``cleanup()`` never runs.

        Args:
            run_id: The run identifier (becomes the stream key).
            event: SSE event name (e.g. ``"metadata"``, ``"updates"``).
            data: JSON-serialisable payload.
        """
        redis = await self._ensure_redis()
        stream_key = self._stream_key(run_id)
        payload = {
            "event": event,
            "data": json.dumps(data, default=str),
            "ts": str(time.time()),
        }
        msg_id = await redis.xadd(
            stream_key,
            payload,
            maxlen=self._stream_maxlen,
            approximate=True,
        )
        # Refresh TTL so the stream lives long enough for late subscribers
        if self._stream_ttl_seconds > 0:
            await redis.expire(stream_key, self._stream_ttl_seconds)
        logger.debug("Published event to run %s: id=%s event=%s", run_id, msg_id, event)

    async def publish_end(self, run_id: str) -> None:
        """Signal that no more events will be produced for *run_id*.

        Inserts a sentinel ``__end__`` marker into the stream and
        refreshes the TTL so late subscribers can drain it.
        """
        redis = await self._ensure_redis()
        stream_key = self._stream_key(run_id)
        await redis.xadd(
            stream_key,
            {"event": "__end__", "data": "", "ts": str(time.time())},
            maxlen=self._stream_maxlen,
            approximate=True,
        )
        if self._stream_ttl_seconds > 0:
            await redis.expire(stream_key, self._stream_ttl_seconds)
        logger.debug("Published END_SENTINEL for run %s", run_id)

    async def subscribe(
        self,
        run_id: str,
        *,
        last_event_id: str | None = None,
        heartbeat_interval: float = 15.0,
    ) -> AsyncIterator[StreamEvent]:
        """XREADGROUP consumer that yields events for a run.

        Args:
            run_id: The run identifier to subscribe to.
            last_event_id: Resume from this event ID (for reconnection).
                If ``None``, starts from the latest (``>``).
            heartbeat_interval: Seconds between heartbeat yields when no
                events arrive.

        Yields:
            StreamEvent instances. Yields ``HEARTBEAT_SENTINEL`` when no
            event arrives within *heartbeat_interval*. Yields
            ``END_SENTINEL`` when the producer signals stream end.
        """
        redis = await self._ensure_redis()
        await self._ensure_group(run_id)

        stream_key = self._stream_key(run_id)
        group_name = self._group_name(run_id)
        consumer = self._consumer_name

        # Starting ID: resume from last_event_id or read new messages only.
        if last_event_id:
            # Read from after the last known event
            cursor_id = last_event_id
        else:
            # ">" means only new messages not yet delivered to this consumer
            cursor_id = ">"

        while True:
            try:
                entries = await redis.xreadgroup(
                    groupname=group_name,
                    consumername=consumer,
                    streams={stream_key: cursor_id},
                    count=1,
                    block=self._block_ms,
                )
            except aioredis.ConnectionError:
                logger.warning("Redis connection lost for run %s; retrying...", run_id)
                self._redis = None  # Force reconnection
                await self._ensure_redis()
                continue
            except Exception:
                logger.error("XREADGROUP error for run %s", run_id, exc_info=True)
                yield HEARTBEAT_SENTINEL
                continue

            if not entries:
                # No messages within block timeout — yield heartbeat
                yield HEARTBEAT_SENTINEL
                continue

            for _stream, messages in entries:
                for msg_id, fields in messages:
                    cursor_id = msg_id

                    event_name = fields.get("event", "")
                    data_raw = fields.get("data", "")

                    if event_name == "__end__":
                        # Auto-ACK the end sentinel
                        await redis.xack(stream_key, group_name, msg_id)
                        yield END_SENTINEL
                        return

                    # Parse data
                    try:
                        data = json.loads(data_raw) if data_raw else None
                    except (json.JSONDecodeError, TypeError):
                        data = data_raw

                    # Auto-ACK after successful read
                    await redis.xack(stream_key, group_name, msg_id)

                    yield StreamEvent(id=str(msg_id), event=event_name, data=data)

    async def ack(self, run_id: str, event_id: str) -> None:
        """Explicit XACK for manual micro-step completion tracking.

        Use this when you need to acknowledge processing *after* yielding
        the event, rather than relying on auto-ACK in ``subscribe()``.
        """
        redis = await self._ensure_redis()
        await redis.xack(
            self._stream_key(run_id),
            self._group_name(run_id),
            event_id,
        )
        logger.debug("ACK'd event %s for run %s", event_id, run_id)

    async def cleanup(self, run_id: str, *, delay: float = 0) -> None:
        """Delete the stream and consumer group for *run_id*.

        Args:
            run_id: The run to clean up.
            delay: Seconds to wait before cleanup (gives late subscribers
                time to drain).
        """
        import asyncio

        if delay > 0:
            await asyncio.sleep(delay)

        redis = await self._ensure_redis()
        stream_key = self._stream_key(run_id)
        group_name = self._group_name(run_id)

        try:
            await redis.xgroup_destroy(stream_key, group_name)
        except Exception:
            logger.debug("Error destroying group for run %s", run_id, exc_info=True)

        try:
            await redis.delete(stream_key)
        except Exception:
            logger.debug("Error deleting stream for run %s", run_id, exc_info=True)

        logger.debug("Cleaned up stream and group for run %s", run_id)

    # ── Utility methods ───────────────────────────────────────────────────

    async def get_stream_info(self, run_id: str) -> dict[str, Any]:
        """Get diagnostic info about a run's stream (for monitoring)."""
        redis = await self._ensure_redis()
        stream_key = self._stream_key(run_id)
        try:
            info = await redis.xinfo_stream(stream_key)
            return {
                "run_id": run_id,
                "stream_key": stream_key,
                "length": info.get("length", 0),
                "first_entry": info.get("first-entry"),
                "last_entry": info.get("last-entry"),
            }
        except Exception:
            return {"run_id": run_id, "error": "stream not found"}

    async def get_pending_info(self, run_id: str) -> dict[str, Any]:
        """Get pending messages for the consumer group (for monitoring)."""
        redis = await self._ensure_redis()
        try:
            pending = await redis.xpending_range(
                self._stream_key(run_id),
                self._group_name(run_id),
                min="-",
                max="+",
                count=100,
            )
            return {
                "run_id": run_id,
                "pending_count": len(pending),
                "pending_messages": pending,
            }
        except Exception:
            return {"run_id": run_id, "error": "no pending info available"}

    async def reclaim_idle_messages(
        self,
        run_id: str,
        count: int = 10,
    ) -> list[StreamEvent]:
        """XAUTOCLAIM messages that have been idle too long.

        Useful for recovering from crashed consumers that left messages
        unacknowledged.
        """
        redis = await self._ensure_redis()
        stream_key = self._stream_key(run_id)
        group_name = self._group_name(run_id)

        try:
            _start, claimed, _deleted = await redis.xautoclaim(
                stream_key,
                group_name,
                self._consumer_name,
                self._claim_idle_ms,
                "0-0",
                count=count,
            )
            events = []
            for msg_id, fields in claimed:
                event_name = fields.get("event", "")
                data_raw = fields.get("data", "")
                try:
                    data = json.loads(data_raw) if data_raw else None
                except (json.JSONDecodeError, TypeError):
                    data = data_raw
                events.append(StreamEvent(id=str(msg_id), event=event_name, data=data))
            return events
        except Exception:
            logger.warning("XAUTOCLAIM failed for run %s", run_id, exc_info=True)
            return []
