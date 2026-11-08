"""Kafka 测试数据生产器：确定性数据 + 期望值，供 B/C 环节验证。"""
from __future__ import annotations

import json
import time
import uuid
from collections import Counter, defaultdict
from typing import Any

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP = "node1:9092"
BASE_TS = 1700000000000  # 固定事件时间，避免每次跑真值变化


def ensure_topic(topic: str, *, partitions: int = 1, replication: int = 1) -> None:
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, request_timeout_ms=15000)
    try:
        try:
            admin.create_topics([NewTopic(
                name=topic, num_partitions=partitions, replication_factor=replication,
            )], validate_only=False)
        except TopicAlreadyExistsError:
            pass
    finally:
        admin.close()


def reset_topic(topic: str, *, partitions: int = 1, replication: int = 1) -> None:
    """重跑时清空 topic，保证期望值是确定的，不叠加上次数据。"""
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, request_timeout_ms=15000)
    try:
        try:
            admin.delete_topics([topic], timeout_ms=15000)
        except Exception:
            pass
        for _ in range(20):
            try:
                if topic not in admin.list_topics():
                    break
            except Exception:
                break
            time.sleep(0.5)
        try:
            admin.create_topics([NewTopic(
                name=topic, num_partitions=partitions, replication_factor=replication,
            )], validate_only=False)
        except TopicAlreadyExistsError:
            pass
    finally:
        admin.close()
    time.sleep(1)


def _serialize(value: Any) -> bytes:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, str):
        return value.encode("utf-8")
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def _produce(topic: str, values: list[Any], *, key: str | None = None) -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=_serialize,
        key_serializer=lambda k: k.encode() if k else None,
        acks="all",
        request_timeout_ms=20000,
    )
    try:
        futures = [producer.send(topic, value=v, key=key) for v in values]
        for fut in futures:
            fut.get(timeout=20)
        producer.flush(timeout=20)
    finally:
        producer.close(timeout=10)


def send_events(topic: str, events: list[Any]) -> None:
    """应用 RUNNING 后再发同一批事件；不重置 topic，避免删掉应用正在订阅的 topic。"""
    _produce(topic, events)


def consume(topic: str, *, expected: int, timeout_ms: int = 20000) -> list[Any]:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=timeout_ms,
        group_id=f"eval-selftest-{uuid.uuid4().hex}",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    try:
        out = []
        for msg in consumer:
            out.append(msg.value)
            if len(out) >= expected:
                break
        return out
    finally:
        consumer.close()


def selftest(run_id: str) -> dict[str, Any]:
    topic = f"eval_{run_id}_selftest"
    ensure_topic(topic)
    payload = [{"k": "a", "v": 1}, {"k": "b", "v": 2}, {"k": "a", "v": 3}]
    _produce(topic, payload)
    got = consume(topic, expected=3)
    return {"topic": topic, "sent": payload, "received": got, "ok": got == payload}


# ---------------- B1 ----------------
def produce_words(topic: str) -> dict[str, Any]:
    reset_topic(topic)
    sentences = [
        "hello flink hello world",
        "flink is fast",
        "hello world",
        "kafka flink hello",
    ] * 5  # 20 行
    _produce(topic, sentences)
    counts: Counter[str] = Counter()
    for line in sentences:
        counts.update(line.split())
    return {"topic": topic, "lines": len(sentences), "expected_word_counts": dict(counts)}


# ---------------- B2 ----------------
def produce_events(topic: str, *, send: bool = True) -> dict[str, Any]:
    """建/清空 topic；send=False 时只准备 topic 和期望值，等应用 RUNNING 后再发。

    作业可能用 earliest 或 latest；先清空 topic、等应用启动后再发，两种 offset
    策略都能消费到同一批数据。
    """
    reset_topic(topic)
    base = BASE_TS
    events = [
        {"user_id": "u1", "event": "search", "ts": base + 1000},
        {"user_id": "u2", "event": "search", "ts": base + 2000},
        {"user_id": "u1", "event": "cart", "ts": base + 3000},
        {"user_id": "u3", "event": "search", "ts": base + 4000},
        {"user_id": "u2", "event": "cart", "ts": base + 5000},
        # 推进 watermark 的哨兵，避免窗口一直不关闭
        {"user_id": "__sentinel__", "event": "sentinel", "ts": base + 60000},
    ]
    if send:
        _produce(topic, events)
    pv: Counter[str] = Counter()
    uv: dict[str, set[str]] = defaultdict(set)
    for e in events:
        if e["event"] == "sentinel":
            continue
        pv[e["event"]] += 1
        uv[e["event"]].add(e["user_id"])
    expected = {
        "window": [base, base + 10000],
        "pv": dict(pv),
        "uv": {k: len(v) for k, v in uv.items()},
    }
    return {"topic": topic, "events": events, "expected": expected}


# ---------------- B3 ----------------
def produce_events_topn(topic: str, *, send: bool = True) -> dict[str, Any]:
    reset_topic(topic)
    base = BASE_TS
    w1 = base
    w2 = base + 60000
    events = [
        {"user_id": "u1", "event": "order", "category": "A", "price": 100, "quantity": 2, "ts": w1 + 1000},
        {"user_id": "u2", "event": "order", "category": "A", "price": 50, "quantity": 1, "ts": w1 + 2000},
        {"user_id": "u3", "event": "order", "category": "B", "price": 30, "quantity": 3, "ts": w1 + 3000},
        {"user_id": "u4", "event": "order", "category": "C", "price": 10, "quantity": 10, "ts": w1 + 4000},
        {"user_id": "u5", "event": "order", "category": "D", "price": 5, "quantity": 5, "ts": w1 + 5000},
        {"user_id": "u1", "event": "order", "category": "A", "price": 100, "quantity": 1, "ts": w2 + 1000},
        {"user_id": "u2", "event": "order", "category": "B", "price": 100, "quantity": 3, "ts": w2 + 2000},
        {"user_id": "u3", "event": "order", "category": "C", "price": 25, "quantity": 2, "ts": w2 + 3000},
        {"user_id": "u4", "event": "order", "category": "D", "price": 50, "quantity": 4, "ts": w2 + 4000},
        {"user_id": "__sentinel__", "event": "sentinel", "category": "Z", "price": 0, "quantity": 0, "ts": base + 180000},
    ]
    if send:
        _produce(topic, events)
    sales: dict[int, dict[str, int]] = {w1: defaultdict(int), w2: defaultdict(int)}
    for e in events:
        if e["event"] == "sentinel":
            continue
        window = w1 if w1 <= e["ts"] < w1 + 60000 else w2
        sales[window][e["category"]] += e["price"] * e["quantity"]
    expected = []
    for window, cats in sales.items():
        top = sorted(cats.items(), key=lambda kv: (-kv[1], kv[0]))[:3]
        expected.append({
            "window": [window, window + 60000],
            "top3": [{"category": c, "sales": v} for c, v in top],
        })
    return {"topic": topic, "events": events, "expected": expected}


# ---------------- C3 ----------------
def produce_bad_events(topic: str) -> dict[str, Any]:
    reset_topic(topic)
    base = BASE_TS
    events = [
        {"user_id": "u1", "amount": "10", "ts": base + 1000},
        {"user_id": "u2", "amount": "20", "ts": base + 2000},
        {"user_id": "bad", "amount": "N/A", "ts": base + 3000},
        {"user_id": "u3", "amount": "30", "ts": base + 4000},
    ]
    _produce(topic, events)
    return {"topic": topic, "events": events, "bad_record": events[2]}


def wait_topic_messages(topic: str, expected: int, timeout_s: float = 30) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            if len(consume(topic, expected=expected, timeout_ms=5000)) >= expected:
                return True
        except Exception:
            pass
        time.sleep(2)
    return False
