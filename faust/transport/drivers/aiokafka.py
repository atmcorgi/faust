"""Message transport using :pypi:`aiokafka`."""

import asyncio
import typing
from asyncio import Lock, QueueEmpty
from collections import deque
from functools import partial
from time import monotonic
from typing import (
    Any,
    Awaitable,
    Callable,
    ClassVar,
    Deque,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Set,
    Tuple,
    Type,
    cast,
    no_type_check,
)

import aiokafka
import aiokafka.abc
import opentracing
from aiokafka import TopicPartition
from aiokafka.consumer.group_coordinator import OffsetCommitRequest
from aiokafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from aiokafka.errors import (
    CommitFailedError,
    ConsumerStoppedError,
    IllegalStateError,
    KafkaError,
    NotControllerError,
    ProducerFenced,
    TopicAlreadyExistsError as TopicExistsError,
    for_code,
)
from aiokafka.partitioner import DefaultPartitioner, murmur2
from aiokafka.protocol.admin import CreateTopicsRequest
from aiokafka.protocol.metadata import MetadataRequest_v1
from aiokafka.structs import OffsetAndMetadata, TopicPartition as _TopicPartition
from aiokafka.util import parse_kafka_version
from mode import Service, get_logger
from mode.threads import ServiceThread, WorkerThread
from mode.utils import text
from mode.utils.futures import StampedeWrapper
from mode.utils.objects import cached_property
from mode.utils.times import Seconds, humanize_seconds_ago, want_seconds
from opentracing.ext import tags
from yarl import URL

from faust.auth import (
    GSSAPICredentials,
    OAuthCredentials,
    SASLCredentials,
    SSLCredentials,
)
from faust.exceptions import (
    ConsumerNotStarted,
    ImproperlyConfigured,
    NotReady,
    ProducerSendError,
)
from faust.transport import base
from faust.transport.consumer import (
    ConsumerThread,
    RecordMap,
    ThreadDelegateConsumer,
    ensure_TPset,
)
from faust.types import (
    TP,
    ConsumerMessage,
    FutureMessage,
    HeadersArg,
    PendingMessage,
    RecordMetadata,
)
from faust.types.auth import CredentialsT
from faust.types.transports import ConsumerT, PartitionerT, ProducerT
from faust.utils.tracing import noop_span, set_current_span, traced_from_parent_span

__all__ = ["Consumer", "Producer", "Transport"]

# if not hasattr(aiokafka, '__robinhood__'):  # pragma: no cover
#     raise RuntimeError(
#         'Please install robinhood-aiokafka, not aiokafka')

logger = get_logger(__name__)

DEFAULT_GENERATION_ID = OffsetCommitRequest.DEFAULT_GENERATION_ID

TOPIC_LENGTH_MAX = 249

SLOW_PROCESSING_CAUSE_AGENT = """
The agent processing the stream is hanging (waiting for network, I/O or \
infinite loop).
""".strip()

SLOW_PROCESSING_CAUSE_STREAM = """
The stream has stopped processing events for some reason.
""".strip()


SLOW_PROCESSING_CAUSE_COMMIT = """
The commit handler background thread has stopped working (report as bug).
""".strip()


SLOW_PROCESSING_EXPLAINED = """

There are multiple possible explanations for this:

1) The processing of a single event in the stream
   is taking too long.

    The timeout for this is defined by the %(setting)s setting,
    currently set to %(current_value)r.  If you expect the time
    required to process an event, to be greater than this then please
    increase the timeout.

"""

SLOW_PROCESSING_NO_FETCH_SINCE_START = """
Aiokafka has not sent fetch request for %r since start (started %s)
""".strip()

SLOW_PROCESSING_NO_RESPONSE_SINCE_START = """
Aiokafka has not received fetch response for %r since start (started %s)
""".strip()

SLOW_PROCESSING_NO_RECENT_FETCH = """
Aiokafka stopped fetching from %r (last done %s)
""".strip()

SLOW_PROCESSING_NO_RECENT_RESPONSE = """
Broker stopped responding to fetch requests for %r (last responded %s)
""".strip()

SLOW_PROCESSING_NO_HIGHWATER_SINCE_START = """
Highwater not yet available for %r (started %s).
""".strip()

SLOW_PROCESSING_STREAM_IDLE_SINCE_START = """
Stream has not started processing %r (started %s).
""".strip()

SLOW_PROCESSING_STREAM_IDLE = """
Stream stopped processing, or is slow for %r (last inbound %s).
""".strip()

SLOW_PROCESSING_NO_COMMIT_SINCE_START = """
Has not committed %r at all since worker start (started %s).
""".strip()

SLOW_PROCESSING_NO_RECENT_COMMIT = """
Has not committed %r (last commit %s).
""".strip()


def __canon_host(host, default):
    """Ensure host is correctly formatted for aiokafka. That means IPv6
    addresses must enclosed in squared brackets.
    """
    if not host:
        return default
    if ":" in host:
        return f"[{host}]"
    return host


def server_list(urls: List[URL], default_port: int) -> List[str]:
    """Convert list of urls to list of servers accepted by :pypi:`aiokafka`."""
    default_host = "127.0.0.1"
    # Yarl strips [] from IPv6 adresses, and aiokafka expects them.
    return [
        f"{__canon_host(u.host, default_host)}:{u.port or default_port}" for u in urls
    ]


class ConsumerRebalanceListener(aiokafka.abc.ConsumerRebalanceListener):  # type: ignore
    # kafka's ridiculous class based callback interface makes this hacky.

    def __init__(self, thread: ConsumerThread) -> None:
        self._thread: ConsumerThread = thread

    def on_partitions_revoked(self, revoked: Iterable[_TopicPartition]) -> Awaitable:
        """Call when partitions are being revoked."""
        thread = self._thread
        # XXX Must call app.on_rebalance_start as early as possible.
        # we call this in the sync method, this way when we know
        # that it will be called even if await never returns to the coroutine.
        thread.app.on_rebalance_start()

        # this way we should also get a warning if the coroutine
        # is never awaited.
        return thread.on_partitions_revoked(ensure_TPset(revoked))

    async def on_partitions_assigned(self, assigned: Iterable[_TopicPartition]) -> None:
        """Call when partitions are being assigned."""
        generation = self._thread._ensure_consumer()._coordinator.generation
        # set the generation on the app
        self._thread.app.consumer_generation_id = generation
        await self._thread.on_partitions_assigned(ensure_TPset(assigned), generation)


class Consumer(ThreadDelegateConsumer):
    """Kafka consumer using :pypi:`aiokafka`."""

    logger = logger

    RebalanceListener: ClassVar[Type[ConsumerRebalanceListener]]
    RebalanceListener = ConsumerRebalanceListener

    consumer_stopped_errors: ClassVar[Tuple[Type[BaseException], ...]] = (
        ConsumerStoppedError,
    )

    def _new_consumer_thread(self) -> ConsumerThread:
        return AIOKafkaConsumerThread(self, loop=self.loop, beacon=self.beacon)

    async def create_topic(
        self,
        topic: str,
        partitions: int,
        replication: int,
        *,
        config: Optional[Mapping[str, Any]] = None,
        timeout: Seconds = 30.0,
        retention: Optional[Seconds] = None,
        compacting: Optional[bool] = None,
        deleting: Optional[bool] = None,
        ensure_created: bool = False,
    ) -> None:
        """Create/declare topic on server."""
        if self.app.conf.topic_allow_declare:
            await self._thread.create_topic(
                topic,
                partitions,
                replication,
                config=config,
                timeout=timeout,
                retention=retention,
                compacting=compacting,
                deleting=deleting,
                ensure_created=ensure_created,
            )
        else:
            logger.warning(f"Topic creation disabled! Can't create topic {topic}")

    def _new_topicpartition(self, topic: str, partition: int) -> TP:
        return cast(TP, _TopicPartition(topic, partition))

    def _to_message(self, tp: TP, record: Any) -> ConsumerMessage:
        timestamp: Optional[int] = record.timestamp
        timestamp_s: float = cast(float, None)
        if timestamp is not None:
            timestamp_s = timestamp / 1000.0
        return ConsumerMessage(
            record.topic,
            record.partition,
            record.offset,
            timestamp_s,
            record.timestamp_type,
            record.headers,
            record.key,
            record.value,
            record.checksum,
            record.serialized_key_size,
            record.serialized_value_size,
            tp,
            generation_id=self.app.consumer_generation_id,
        )

    async def on_stop(self) -> None:
        """Call when consumer is stopping."""
        await super().on_stop()
        transport = cast(Transport, self.transport)
        transport._topic_waiters.clear()

    def verify_event_path(self, now: float, tp: TP) -> None:
        return self._thread.verify_event_path(now, tp)


class ThreadedProducer(ServiceThread):
    _producer: Optional[aiokafka.AIOKafkaProducer] = None
    event_queue: Optional[asyncio.Queue] = None
    _default_producer: Optional[aiokafka.AIOKafkaProducer] = None
    _push_events_task: Optional[asyncio.Task] = None
    app: None
    stopped: bool
    _shutdown_initiated: bool = False

    def __init__(
        self,
        default_producer,
        app,
        *,
        executor: Any = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        thread_loop: Optional[asyncio.AbstractEventLoop] = None,
        Worker: Type[WorkerThread] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            executor=executor,
            thread_loop=thread_loop,
            Worker=Worker,
            **kwargs,
        )
        self._default_producer = default_producer
        self.app = app

    def _shutdown_thread(self) -> None:
        # Ensure that the shutdown process is initiated only once
        if not self._shutdown_initiated:
            asyncio.run_coroutine_threadsafe(self.on_thread_stop(), self.thread_loop)

    async def flush(self) -> None:
        """Wait for producer to finish transmitting all buffered messages."""
        while True:
            try:
                msg = self.event_queue.get_nowait()
            except QueueEmpty:
                break
            else:
                await self.publish_message(msg)
        if self._producer is not None:
            await self._producer.flush()

    def _new_producer(
        self, transactional_id: Optional[str] = None
    ) -> aiokafka.AIOKafkaProducer:
        return aiokafka.AIOKafkaProducer(
            loop=self.thread_loop,
            **{
                **self._default_producer._settings_default(),
                **self._default_producer._settings_auth(),
                **self._default_producer._settings_extra(),
            },
            transactional_id=transactional_id,
        )

    async def on_start(self) -> None:
        self.event_queue = asyncio.Queue()
        producer = self._producer = self._new_producer()
        await producer.start()
        self.stopped = False
        self._push_events_task = self.thread_loop.create_task(self.push_events())

    async def on_thread_stop(self) -> None:
        """Call when producer thread is stopping."""
        self._shutdown_initiated = True
        logger.info("Stopping producer thread")
        await super().on_thread_stop()
        self.stopped = True
        # when method queue is stopped, we can stop the consumer
        if self._producer is not None:
            await self.flush()
            await self._producer.stop()
        if self._push_events_task is not None:
            while not self._push_events_task.done():
                await asyncio.sleep(0.1)

    async def push_events(self):
        while not self.stopped:
            try:
                event = await asyncio.wait_for(self.event_queue.get(), timeout=0.1)
            except asyncio.TimeoutError:
                continue

            self.app.sensors.on_threaded_producer_buffer_processed(
                app=self.app, size=self.event_queue.qsize()
            )
            await self.publish_message(event)

    async def publish_message(
        self, fut_other: FutureMessage, wait: bool = False
    ) -> Awaitable[RecordMetadata]:
        """Fulfill promise to publish message to topic."""
        fut = FutureMessage(fut_other.message)
        message: PendingMessage = fut.message
        topic = message.channel.get_topic_name()
        key: bytes = cast(bytes, message.key)
        value: bytes = cast(bytes, message.value)
        partition: Optional[int] = message.partition
        timestamp: float = cast(float, message.timestamp)
        headers: Optional[HeadersArg] = message.headers
        logger.debug(
            "send: topic=%r k=%r v=%r timestamp=%r partition=%r",
            topic,
            key,
            value,
            timestamp,
            partition,
        )
        producer = self._producer
        state = self.app.sensors.on_send_initiated(
            producer,
            topic,
            message=message,
            keysize=len(key) if key else 0,
            valsize=len(value) if value else 0,
        )
        timestamp_ms = int(timestamp * 1000.0) if timestamp else timestamp
        if headers is not None:
            if isinstance(headers, Mapping):
                headers = list(headers.items())
        if wait:
            ret: RecordMetadata = await producer.send_and_wait(
                topic=topic,
                key=key,
                value=value,
                partition=partition,
                timestamp_ms=timestamp_ms,
                headers=headers,
            )
            fut.message.channel._on_published(
                message=fut, state=state, producer=producer
            )
            fut.set_result(ret)
            return fut
        else:
            fut2 = cast(
                asyncio.Future,
                await producer.send(
                    topic=topic,
                    key=key,
                    value=value,
                    partition=partition,
                    timestamp_ms=timestamp_ms,
                    headers=headers,
                ),
            )
            callback = partial(
                fut.message.channel._on_published,
                message=fut,
                state=state,
                producer=producer,
            )
            fut2.add_done_callback(cast(Callable, callback))
            return fut2


class AIOKafkaConsumerThread(ConsumerThread):
    _consumer: Optional[aiokafka.AIOKafkaConsumer] = None
    _pending_rebalancing_spans: Deque[opentracing.Span]

    tp_last_committed_at: MutableMapping[TP, float]
    time_started: float

    tp_fetch_request_timeout_secs: float
    tp_fetch_response_timeout_secs: float
    tp_stream_timeout_secs: float
    tp_commit_timeout_secs: float

    def __post_init__(self) -> None:
        consumer = cast(Consumer, self.consumer)
        self._partitioner: PartitionerT = (
            self.app.conf.producer_partitioner or DefaultPartitioner()
        )
        self._rebalance_listener = consumer.RebalanceListener(self)
        self._pending_rebalancing_spans = deque()
        self.tp_last_committed_at = {}

        app = self.consumer.app

        stream_processing_timeout = app.conf.stream_processing_timeout
        self.tp_fetch_request_timeout_secs = stream_processing_timeout
        self.tp_fetch_response_timeout_secs = stream_processing_timeout
        self.tp_stream_timeout_secs = stream_processing_timeout

        commit_livelock_timeout = app.conf.broker_commit_livelock_soft_timeout
        self.tp_commit_timeout_secs = commit_livelock_timeout

    async def on_start(self) -> None:
        """Call when consumer starts."""
        self._consumer = self._create_consumer(loop=self.thread_loop)
        self.time_started = monotonic()
        await self._consumer.start()

    async def on_thread_stop(self) -> None:
        """Call when consumer thread is stopping."""
        # super stops thread method queue (QueueServiceThread.method_queue)
        await super().on_thread_stop()
        # when method queue is stopped, we can stop the consumer
        if self._consumer is not None:
            await self._consumer.stop()

    def _create_consumer(
        self, loop: asyncio.AbstractEventLoop
    ) -> aiokafka.AIOKafkaConsumer:
        transport = cast(Transport, self.transport)
        if self.app.client_only:
            return self._create_client_consumer(transport)
        else:
            return self._create_worker_consumer(transport)

    def _create_worker_consumer(
        self, transport: "Transport"
    ) -> aiokafka.AIOKafkaConsumer:
        isolation_level: str = "read_uncommitted"
        conf = self.app.conf
        if self.consumer.in_transaction:
            isolation_level = "read_committed"
        self._assignor = (
            self.app.assignor
            if self.app.conf.table_standby_replicas > 0
            else RoundRobinPartitionAssignor
        )
        auth_settings = credentials_to_aiokafka_auth(
            conf.broker_credentials, conf.ssl_context
        )
        max_poll_interval = conf.broker_max_poll_interval or 0

        request_timeout = conf.broker_request_timeout
        session_timeout = conf.broker_session_timeout
        rebalance_timeout = conf.broker_rebalance_timeout

        if session_timeout > request_timeout:
            raise ImproperlyConfigured(
                f"Setting broker_session_timeout={session_timeout} "
                f"cannot be greater than "
                f"broker_request_timeout={request_timeout}"
            )

        return aiokafka.AIOKafkaConsumer(
            api_version=conf.consumer_api_version,
            client_id=conf.broker_client_id,
            group_id=conf.id,
            group_instance_id=conf.consumer_group_instance_id,
            bootstrap_servers=server_list(transport.url, transport.default_port),
            partition_assignment_strategy=[self._assignor],
            enable_auto_commit=False,
            auto_offset_reset=conf.consumer_auto_offset_reset,
            max_poll_records=conf.broker_max_poll_records,
            max_poll_interval_ms=int(max_poll_interval * 1000.0),
            max_partition_fetch_bytes=conf.consumer_max_fetch_size,
            fetch_max_wait_ms=1500,
            request_timeout_ms=int(request_timeout * 1000.0),
            check_crcs=conf.broker_check_crcs,
            session_timeout_ms=int(session_timeout * 1000.0),
            rebalance_timeout_ms=int(rebalance_timeout * 1000.0),
            heartbeat_interval_ms=int(conf.broker_heartbeat_interval * 1000.0),
            isolation_level=isolation_level,
            metadata_max_age_ms=conf.consumer_metadata_max_age_ms,
            connections_max_idle_ms=conf.consumer_connections_max_idle_ms,
            # traced_from_parent_span=self.traced_from_parent_span,
            # start_rebalancing_span=self.start_rebalancing_span,
            # start_coordinator_span=self.start_coordinator_span,
            # on_generation_id_known=self.on_generation_id_known,
            # flush_spans=self.flush_spans,
            **auth_settings,
        )

    def _create_client_consumer(
        self, transport: "Transport"
    ) -> aiokafka.AIOKafkaConsumer:
        conf = self.app.conf
        auth_settings = credentials_to_aiokafka_auth(
            conf.broker_credentials, conf.ssl_context
        )
        max_poll_interval = conf.broker_max_poll_interval or 0
        return aiokafka.AIOKafkaConsumer(
            client_id=conf.broker_client_id,
            bootstrap_servers=server_list(transport.url, transport.default_port),
            request_timeout_ms=int(conf.broker_request_timeout * 1000.0),
            enable_auto_commit=True,
            max_poll_records=conf.broker_max_poll_records,
            max_poll_interval_ms=int(max_poll_interval * 1000.0),
            auto_offset_reset=conf.consumer_auto_offset_reset,
            check_crcs=conf.broker_check_crcs,
            **auth_settings,
        )

    @cached_property
    def trace_category(self) -> str:
        return f"{self.app.conf.name}-_aiokafka"

    def start_rebalancing_span(self) -> opentracing.Span:
        return self._start_span("rebalancing", lazy=True)

    def start_coordinator_span(self) -> opentracing.Span:
        return self._start_span("coordinator")

    def _start_span(self, name: str, *, lazy: bool = False) -> opentracing.Span:
        tracer = self.app.tracer
        if tracer is not None:
            span = tracer.get_tracer(self.trace_category).start_span(
                operation_name=name,
            )
            span.set_tag(tags.SAMPLING_PRIORITY, 1)
            self.app._span_add_default_tags(span)
            set_current_span(span)
            if lazy:
                self._transform_span_lazy(span)
            return span
        else:
            return noop_span()

    @no_type_check
    def _transform_span_lazy(self, span: opentracing.Span) -> None:
        # XXX slow
        consumer = self
        if typing.TYPE_CHECKING:
            # MyPy completely disallows the statements below
            # claiming it is an illegal dynamic baseclass.
            # We know mypy, but do it anyway :D
            pass
        else:
            cls = span.__class__

            class LazySpan(cls):
                def finish(self) -> None:
                    consumer._span_finish(span)

            span._real_finish, span.finish = span.finish, LazySpan.finish

    def _span_finish(self, span: opentracing.Span) -> None:
        assert self._consumer is not None
        if self._consumer._coordinator.generation == DEFAULT_GENERATION_ID:
            self._on_span_generation_pending(span)
        else:
            self._on_span_generation_known(span)

    def _on_span_generation_pending(self, span: opentracing.Span) -> None:
        self._pending_rebalancing_spans.append(span)

    def _on_span_generation_known(self, span: opentracing.Span) -> None:
        if self._consumer:
            coordinator = self._consumer._coordinator
            coordinator_id = coordinator.coordinator_id
            app_id = self.app.conf.id
            generation = coordinator.generation
            member_id = coordinator.member_id

            try:
                op_name = span.operation_name
                set_tag = span.set_tag
            except AttributeError:  # pragma: no cover
                pass  # not a real span
            else:
                trace_id_str = f"reb-{app_id}-{generation}"
                trace_id = murmur2(trace_id_str.encode())

                span.context.trace_id = trace_id
                if op_name.endswith(".REPLACE_WITH_MEMBER_ID"):
                    span.set_operation_name(f"rebalancing node {member_id}")
                set_tag("kafka_generation", generation)
                set_tag("kafka_member_id", member_id)
                set_tag("kafka_coordinator_id", coordinator_id)
                self.app._span_add_default_tags(span)
                span._real_finish()

    def _on_span_cancelled_early(self, span: opentracing.Span) -> None:
        try:
            op_name = span.operation_name
        except AttributeError:
            return
        else:
            span.set_operation_name(f"{op_name} (CANCELLED)")
            span._real_finish()

    def traced_from_parent_span(
        self, parent_span: opentracing.Span, lazy: bool = False, **extra_context: Any
    ) -> Callable:
        return traced_from_parent_span(
            parent_span,
            callback=self._transform_span_lazy if lazy else None,
            **extra_context,
        )

    def flush_spans(self) -> None:
        while self._pending_rebalancing_spans:
            span = self._pending_rebalancing_spans.popleft()
            self._on_span_cancelled_early(span)

    def on_generation_id_known(self) -> None:
        while self._pending_rebalancing_spans:
            span = self._pending_rebalancing_spans.popleft()
            self._on_span_generation_known(span)

    def close(self) -> None:
        """Close consumer for graceful shutdown."""
        if self._consumer is not None:
            self._consumer._closed = True
            asyncio.run_coroutine_threadsafe(
                self._consumer._client.close(), self.app.loop
            )
            asyncio.run_coroutine_threadsafe(
                self._consumer._coordinator.close(), self.app.loop
            )

    async def subscribe(self, topics: Iterable[str]) -> None:
        """Reset subscription (requires rebalance)."""
        # XXX pattern does not work :/
        await self.call_thread(
            self._ensure_consumer().subscribe,
            topics=set(topics),
            listener=self._rebalance_listener,
        )

    async def seek_to_committed(self) -> Mapping[TP, int]:
        """Seek partitions to the last committed offset."""
        return await self.call_thread(self._ensure_consumer().seek_to_committed)

    async def commit(self, offsets: Mapping[TP, int]) -> bool:
        """Commit topic offsets."""
        return await self.call_thread(self._commit, offsets)

    async def _commit(self, offsets: Mapping[TP, int]) -> bool:
        consumer = self._ensure_consumer()
        now = monotonic()
        commitable_offsets = {
            tp: offset for tp, offset in offsets.items() if tp in self.assignment()
        }
        try:
            aiokafka_offsets = {
                ensure_aiokafka_TP(tp): OffsetAndMetadata(offset, "")
                for tp, offset in commitable_offsets.items()
            }
            self.tp_last_committed_at.update({tp: now for tp in commitable_offsets})
            await consumer.commit(aiokafka_offsets)
        except CommitFailedError as exc:
            if "already rebalanced" in str(exc):
                return False
            self.log.exception("Committing raised exception: %r", exc)
            await self.crash(exc)
            return False
        except IllegalStateError as exc:
            self.log.exception(
                "Got exception: %r\nCurrent assignment: %r", exc, self.assignment()
            )
            await self.crash(exc)
            return False
        except Exception as exc:
            self.log.exception(
                "Got exception: %r\nCurrent assignment: %r", exc, self.assignment()
            )
            await self.crash(exc)
            return False
        return True

    def verify_event_path(self, now: float, tp: TP) -> None:
        # long function ahead, but not difficult to test
        # as it always returns as soon as some condition is met.
        if self._verify_aiokafka_event_path(now, tp):
            # already logged error.
            return None
        parent = cast(Consumer, self.consumer)
        app = parent.app
        monitor = app.monitor
        acks_enabled_for = app.topics.acks_enabled_for
        secs_since_started = now - self.time_started

        if monitor is not None:  # need for .stream_inbound_time
            aiotp = TopicPartition(tp.topic, tp.partition)
            tp_state = self._ensure_consumer()._fetcher._subscriptions.subscription.assignment.state_value(  # noqa: E501
                aiotp
            )
            highwater = tp_state.highwater
            committed_offset = tp_state.position
            has_acks = acks_enabled_for(tp.topic)
            if highwater is None:
                if secs_since_started >= self.tp_stream_timeout_secs:
                    # AIOKAFKA HAS NOT UPDATED HIGHWATER SINCE STARTING
                    self.log.error(
                        SLOW_PROCESSING_NO_HIGHWATER_SINCE_START,
                        tp,
                        humanize_seconds_ago(secs_since_started),
                    )
                return None

            if has_acks and committed_offset is not None:
                if highwater > committed_offset:
                    inbound = monitor.stream_inbound_time.get(tp)
                    if inbound is None:
                        if secs_since_started >= self.tp_stream_timeout_secs:
                            # AIOKAFKA IS FETCHING BUT STREAM IS NOT
                            # PROCESSING EVENTS (no events at all since
                            # start).
                            self._log_slow_processing_stream(
                                SLOW_PROCESSING_STREAM_IDLE_SINCE_START,
                                tp,
                                humanize_seconds_ago(secs_since_started),
                            )
                        return None

                    secs_since_stream = now - inbound
                    if secs_since_stream >= self.tp_stream_timeout_secs:
                        # AIOKAFKA IS FETCHING, AND STREAM WAS WORKING
                        # BEFORE BUT NOW HAS STOPPED PROCESSING
                        # (or processing of an event in the stream takes
                        #  longer than tp_stream_timeout_secs).
                        self._log_slow_processing_stream(
                            SLOW_PROCESSING_STREAM_IDLE,
                            tp,
                            humanize_seconds_ago(secs_since_stream),
                        )
                        return None

                    last_commit = self.tp_last_committed_at.get(tp)
                    if last_commit is None:
                        if secs_since_started >= self.tp_commit_timeout_secs:
                            # AIOKAFKA IS FETCHING AND STREAM IS PROCESSING
                            # BUT WE HAVE NOT COMMITTED ANYTHING SINCE WORKER
                            # START.
                            self._log_slow_processing_commit(
                                SLOW_PROCESSING_NO_COMMIT_SINCE_START,
                                tp,
                                humanize_seconds_ago(secs_since_started),
                            )
                            return None
                    else:
                        secs_since_commit = now - last_commit
                        if secs_since_commit >= self.tp_commit_timeout_secs:
                            # AIOKAFKA IS FETCHING AND STREAM IS PROCESSING
                            # BUT WE HAVE NOT COMITTED ANYTHING IN A WHILE
                            # (commit offset is not advancing).
                            self._log_slow_processing_commit(
                                SLOW_PROCESSING_NO_RECENT_COMMIT,
                                tp,
                                humanize_seconds_ago(secs_since_commit),
                            )
                            return None

    def verify_recovery_event_path(self, now: float, tp: TP) -> None:
        self._verify_aiokafka_event_path(now, tp)

    def _verify_aiokafka_event_path(self, now: float, tp: TP) -> bool:
        """Verify that :pypi:`aiokafka` event path is working.

        Returns :const:`True` if any error was logged.
        """
        consumer = self._ensure_consumer()
        secs_since_started = now - self.time_started
        aiotp = TopicPartition(tp.topic, tp.partition)
        assignment = consumer._fetcher._subscriptions.subscription.assignment
        if not assignment or not assignment.active:
            self.log.error(f"No active partitions for {tp}")
            return True
        poll_at = None
        aiotp_state = assignment.state_value(aiotp)
        if aiotp_state and aiotp_state.timestamp:
            poll_at = aiotp_state.timestamp / 1000  # milliseconds
        if poll_at is None:
            if secs_since_started >= self.tp_fetch_request_timeout_secs:
                # NO FETCH REQUEST SENT AT ALL SINCE WORKER START
                self.log.error(
                    SLOW_PROCESSING_NO_FETCH_SINCE_START,
                    tp,
                    humanize_seconds_ago(secs_since_started),
                )
            return True

        secs_since_request = now - poll_at
        if secs_since_request >= self.tp_fetch_request_timeout_secs:
            # NO REQUEST SENT BY AIOKAFKA IN THE LAST n SECONDS
            self.log.error(
                SLOW_PROCESSING_NO_RECENT_FETCH,
                tp,
                humanize_seconds_ago(secs_since_request),
            )
            return True

        return False

    def _log_slow_processing_stream(self, msg: str, *args: Any) -> None:
        app = self.consumer.app
        self._log_slow_processing(
            msg,
            *args,
            causes=[
                SLOW_PROCESSING_CAUSE_STREAM,
                SLOW_PROCESSING_CAUSE_AGENT,
            ],
            setting="stream_processing_timeout",
            current_value=app.conf.stream_processing_timeout,
        )

    def _log_slow_processing_commit(self, msg: str, *args: Any) -> None:
        app = self.consumer.app
        self._log_slow_processing(
            msg,
            *args,
            causes=[SLOW_PROCESSING_CAUSE_COMMIT],
            setting="broker_commit_livelock_soft_timeout",
            current_value=app.conf.broker_commit_livelock_soft_timeout,
        )

    def _make_slow_processing_error(
        self, msg: str, causes: Iterable[str], setting: str, current_value: float
    ) -> str:
        return " ".join(
            [
                msg,
                SLOW_PROCESSING_EXPLAINED
                % {"setting": setting, "current_value": current_value},
                text.enumeration(causes, start=2, sep="\n\n"),
            ]
        )

    def _log_slow_processing(
        self,
        msg: str,
        *args: Any,
        causes: Iterable[str],
        setting: str,
        current_value: float,
    ) -> None:
        return self.log.error(
            self._make_slow_processing_error(msg, causes, setting, current_value),
            *args,
        )

    async def position(self, tp: TP) -> Optional[int]:
        """Return the current position for topic partition."""
        return await self.call_thread(self._ensure_consumer().position, tp)

    async def seek_to_beginning(self, *partitions: _TopicPartition) -> None:
        """Seek list of offsets to the first available offset."""
        await self.call_thread(self._ensure_consumer().seek_to_beginning, *partitions)

    async def seek_wait(self, partitions: Mapping[TP, int]) -> None:
        """Seek partitions to specific offset and wait for operation."""
        consumer = self._ensure_consumer()
        await self.call_thread(self._seek_wait, consumer, partitions)

    async def _seek_wait(
        self, consumer: Consumer, partitions: Mapping[TP, int]
    ) -> None:
        for tp, offset in partitions.items():
            self.log.dev("SEEK %r -> %r", tp, offset)
            consumer.seek(tp, offset)
            if offset > 0:
                self.consumer._read_offset[tp] = offset
            elif tp in self.consumer._read_offset.keys():
                del self.consumer._read_offset[tp]
        await asyncio.wait_for(
            asyncio.gather(*[consumer.position(tp) for tp in partitions]),
            timeout=self.app.conf.broker_request_timeout,
        )

    def seek(self, partition: TP, offset: int) -> None:
        """Seek partition to specific offset."""
        self._ensure_consumer().seek(partition, offset)

    def assignment(self) -> Set[TP]:
        """Return the current assignment."""
        return ensure_TPset(self._ensure_consumer().assignment())

    def highwater(self, tp: TP) -> int:
        """Return the last offset in a specific partition."""
        if self.consumer.in_transaction:
            return self._ensure_consumer().last_stable_offset(tp)
        else:
            return self._ensure_consumer().highwater(tp)

    def topic_partitions(self, topic: str) -> Optional[int]:
        """Return the number of partitions configured for topic by name."""
        if self._consumer is not None:
            return self._consumer._coordinator._metadata_snapshot.get(topic)
        return None

    async def earliest_offsets(self, *partitions: TP) -> Mapping[TP, int]:
        """Return the earliest offsets for a list of partitions."""
        return await self.call_thread(
            self._ensure_consumer().beginning_offsets, partitions
        )

    async def highwaters(self, *partitions: TP) -> Mapping[TP, int]:
        """Return the last offsets for a list of partitions."""
        return await self.call_thread(self._highwaters, partitions)

    async def _highwaters(self, partitions: List[TP]) -> Mapping[TP, int]:
        consumer = self._ensure_consumer()
        if self.consumer.in_transaction:
            return {tp: consumer.last_stable_offset(tp) for tp in partitions}
        else:
            return cast(Mapping[TP, int], await consumer.end_offsets(partitions))

    def _ensure_consumer(self) -> aiokafka.AIOKafkaConsumer:
        if self._consumer is None:
            raise ConsumerNotStarted("Consumer thread not yet started")
        return self._consumer

    async def getmany(
        self, active_partitions: Optional[Set[TP]], timeout: float
    ) -> RecordMap:
        """Fetch batch of messages from server."""
        # Implementation for the Fetcher service.
        _consumer = self._ensure_consumer()
        # NOTE: Since we are enqueing the fetch request,
        # we need to check when dequeued that we are not in a rebalancing
        # state at that point to return early, or we
        # will create a deadlock (fetch request starts after flow stopped)
        return await self.call_thread(
            self._fetch_records,
            _consumer,
            active_partitions,
            timeout=timeout,
            max_records=_consumer._max_poll_records,
        )

    async def _fetch_records(
        self,
        consumer: aiokafka.AIOKafkaConsumer,
        active_partitions: Set[TP],
        timeout: Optional[float] = None,
        max_records: Optional[int] = None,
    ) -> RecordMap:
        if not self.consumer.flow_active:
            return {}
        fetcher = consumer._fetcher
        if consumer._closed or fetcher._closed:
            raise ConsumerStoppedError()
        with fetcher._subscriptions.fetch_context():
            try:
                return await fetcher.fetched_records(
                    active_partitions,
                    timeout=timeout,
                    max_records=max_records,
                )
            finally:
                pass

    async def create_topic(
        self,
        topic: str,
        partitions: int,
        replication: int,
        *,
        config: Optional[Mapping[str, Any]] = None,
        timeout: Seconds = 30.0,
        retention: Optional[Seconds] = None,
        compacting: Optional[bool] = None,
        deleting: Optional[bool] = None,
        ensure_created: bool = False,
    ) -> None:
        """Create/declare topic on server."""
        if self.app.conf.topic_allow_declare:
            transport = cast(Transport, self.consumer.transport)
            _consumer = self._ensure_consumer()
            _retention = int(want_seconds(retention) * 1000.0) if retention else None
            if len(topic) > TOPIC_LENGTH_MAX:
                raise ValueError(
                    f"Topic name {topic!r} is too long (max={TOPIC_LENGTH_MAX})"
                )
            await self.call_thread(
                transport._create_topic,
                self,
                _consumer._client,
                topic,
                partitions,
                replication,
                config=config,
                timeout=int(want_seconds(timeout) * 1000.0),
                retention=_retention,
                compacting=compacting,
                deleting=deleting,
                ensure_created=ensure_created,
            )
        else:
            logger.warning(f"Topic creation disabled! Can't create topic {topic}")

    def key_partition(
        self, topic: str, key: Optional[bytes], partition: Optional[int] = None
    ) -> Optional[int]:
        """Hash key to determine partition destination."""
        consumer = self._ensure_consumer()
        metadata = consumer._client.cluster
        partitions_for_topic = metadata.partitions_for_topic(topic)
        if partitions_for_topic is None:
            return None
        if partition is not None:
            assert partition >= 0
            assert partition in partitions_for_topic, "Unrecognized partition"
            return partition

        all_partitions = list(partitions_for_topic)
        available = list(metadata.available_partitions_for_topic(topic))
        return self._partitioner(key, all_partitions, available)


class Producer(base.Producer):
    """Kafka producer using :pypi:`aiokafka`."""

    logger = logger

    allow_headers: bool = True
    _producer: Optional[aiokafka.AIOKafkaProducer] = None
    _transaction_producers: typing.Dict[str, aiokafka.AIOKafkaProducer] = {}
    _trn_locks: typing.Dict[str, Lock] = {}

    def create_threaded_producer(self):
        return ThreadedProducer(default_producer=self, app=self.app)

    def __post_init__(self) -> None:
        self._send_on_produce_message = self.app.on_produce_message.send
        if self.partitioner is None:
            self.partitioner = DefaultPartitioner()
        if self._api_version != "auto":
            wanted_api_version = parse_kafka_version(self._api_version)
            if wanted_api_version < (0, 11):
                self.allow_headers = False

    def _settings_default(self) -> Mapping[str, Any]:
        transport = cast(Transport, self.transport)
        return {
            "bootstrap_servers": server_list(transport.url, transport.default_port),
            "client_id": self.client_id,
            "acks": self.acks,
            "linger_ms": self.linger_ms,
            "max_batch_size": self.max_batch_size,
            "max_request_size": self.max_request_size,
            "compression_type": self.compression_type,
            "security_protocol": "SSL" if self.ssl_context else "PLAINTEXT",
            "partitioner": self.partitioner,
            "request_timeout_ms": int(self.request_timeout * 1000),
            "api_version": self._api_version,
            "metadata_max_age_ms": self.app.conf.producer_metadata_max_age_ms,
            "connections_max_idle_ms": self.app.conf.producer_connections_max_idle_ms,
        }

    def _settings_auth(self) -> Mapping[str, Any]:
        return credentials_to_aiokafka_auth(self.credentials, self.ssl_context)

    async def begin_transaction(self, transactional_id: str) -> None:
        """Begin transaction by id."""
        try:
            transaction_producer = self._transaction_producers.get(transactional_id)
            if transaction_producer is None:
                transaction_producer = self._new_producer(
                    transactional_id=transactional_id
                )
                await transaction_producer.start()
                self._transaction_producers[transactional_id] = transaction_producer
                self._trn_locks[transactional_id] = asyncio.Lock()
            async with self._trn_locks[transactional_id]:
                self._ensure_producer()
                await self._transaction_producers[transactional_id].begin_transaction()
        except ProducerFenced:
            logger.info(f"Current Producer for transaction {transactional_id} fenced")
            tp = self._transaction_producers.pop(transactional_id)
            await tp.stop()
        except Exception as ex:
            logger.warning(
                "Exception in begin_transaction for transaction "
                f"{transactional_id} exception {ex}"
            )

    async def commit_transaction(self, transactional_id: str) -> None:
        """Commit transaction by id."""
        try:
            async with self._trn_locks[transactional_id]:
                transaction_producer = self._transaction_producers.get(transactional_id)
                if transaction_producer:
                    await transaction_producer.commit_transaction()
                    logger.debug(f"Done committing transaction {transactional_id}")
                else:
                    logger.warning(
                        f"Commit invoked for unknown transaction {transactional_id}"
                    )
        except ProducerFenced:
            logger.info(f"Current Producer for transaction {transactional_id} fenced")
            tp = self._transaction_producers.pop(transactional_id)
            await tp.stop()

        except Exception as ex:
            logger.warning(
                "Exception in commit_transaction for transaction "
                f"{transactional_id} exception {ex}"
            )

    async def abort_transaction(self, transactional_id: str) -> None:
        """Abort and rollback transaction by id."""
        try:
            async with self._trn_locks[transactional_id]:
                transaction_producer = self._transaction_producers.get(transactional_id)
                if transaction_producer:
                    await transaction_producer.abort_transaction()
                else:
                    logger.warning(
                        f"Abort invoked for unknown transaction {transactional_id}"
                    )
        except ProducerFenced:
            logger.info(f"Current Producer for transaction {transactional_id} fenced")
            tp = self._transaction_producers.pop(transactional_id)
            await tp.stop()
        except Exception as ex:
            logger.warning(
                "Exception in abort_transaction for transaction "
                f"{transactional_id} exception {ex}"
            )

    async def stop_transaction(self, transactional_id: str) -> None:
        """Stop transaction by id."""
        # Let Kafka Manage the transactions. On rebalance a new producer with the
        # same transaction id will fence off this producer
        tp = self._transaction_producers.pop(transactional_id, None)
        if tp:
            await tp.stop()

    async def maybe_begin_transaction(self, transactional_id: str) -> None:
        """Begin transaction (if one does not already exist)."""
        await self.begin_transaction(transactional_id)

    async def commit_transactions(
        self,
        tid_to_offset_map: Mapping[str, Mapping[TP, int]],
        group_id: str,
        start_new_transaction: bool = True,
    ) -> None:
        """Commit transactions."""
        for transactional_id, offsets in tid_to_offset_map.items():
            # get the producer
            async with self._trn_locks[transactional_id]:
                transaction_producer = self._transaction_producers.get(transactional_id)
                if transaction_producer:
                    logger.debug(
                        f"Sending offsets {offsets} to transaction {transactional_id}"
                    )
                    await transaction_producer.send_offsets_to_transaction(
                        offsets, group_id
                    )
                    await transaction_producer.commit_transaction()
                    logger.debug(f"Done committing transaction {transactional_id}")
                    if start_new_transaction:
                        logger.debug(f"Starting transaction {transactional_id}")
                        await transaction_producer.begin_transaction()
                        logger.debug(f"Started transaction {transactional_id}")

                else:
                    logger.warning(
                        f"Commit invoked for unknown transaction {transactional_id}"
                    )

    def _settings_extra(self) -> Mapping[str, Any]:
        if self.app.in_transaction:
            return {"acks": "all", "enable_idempotence": True}
        return {}

    def _new_producer(
        self, transactional_id: Optional[str] = None
    ) -> aiokafka.AIOKafkaProducer:
        return self._producer_type(
            **{
                **self._settings_default(),
                **self._settings_auth(),
                **self._settings_extra(),
            },
            transactional_id=transactional_id,
        )

    @property
    def _producer_type(self) -> Type[aiokafka.AIOKafkaProducer]:
        return aiokafka.AIOKafkaProducer

    async def create_topic(
        self,
        topic: str,
        partitions: int,
        replication: int,
        *,
        config: Optional[Mapping[str, Any]] = None,
        timeout: Seconds = 20.0,
        retention: Optional[Seconds] = None,
        compacting: Optional[bool] = None,
        deleting: Optional[bool] = None,
        ensure_created: bool = False,
    ) -> None:
        """Create/declare topic on server."""
        if self.app.conf.topic_allow_declare:
            _retention = int(want_seconds(retention) * 1000.0) if retention else None
            producer = self._ensure_producer()
            await cast(Transport, self.transport)._create_topic(
                self,
                producer.client,
                topic,
                partitions,
                replication,
                config=config,
                timeout=int(want_seconds(timeout) * 1000.0),
                retention=_retention,
                compacting=compacting,
                deleting=deleting,
                ensure_created=ensure_created,
            )
            await producer.client.force_metadata_update()  # Fixes #499
        else:
            logger.warning(f"Topic creation disabled! Can't create topic {topic}")

    def _ensure_producer(self) -> aiokafka.AIOKafkaProducer:
        if self._producer is None:
            raise NotReady("Producer service not yet started")
        return self._producer

    async def on_start(self) -> None:
        """Call when producer starts."""
        await super().on_start()
        producer = self._producer = self._new_producer()
        self.beacon.add(producer)
        await producer.start()

    async def on_stop(self) -> None:
        """Call when producer stops."""
        await super().on_stop()
        cast(Transport, self.transport)._topic_waiters.clear()
        producer, self._producer = self._producer, None
        if producer is not None:
            await producer.stop()
            for transaction_producer in self._transaction_producers.values():
                await transaction_producer.stop()
            self._transaction_producers.clear()

    async def send(
        self,
        topic: str,
        key: Optional[bytes],
        value: Optional[bytes],
        partition: Optional[int],
        timestamp: Optional[float],
        headers: Optional[HeadersArg],
        *,
        transactional_id: Optional[str] = None,
    ) -> Awaitable[RecordMetadata]:
        """Schedule message to be transmitted by producer."""

        transaction_producer = self._ensure_producer()
        if transactional_id:
            transaction_producer = self._transaction_producers.get(transactional_id)
            if transaction_producer is None:
                raise ProducerSendError(
                    f"No transaction producer found for : {transactional_id}"
                )
        if headers is not None:
            if isinstance(headers, Mapping):
                headers = list(headers.items())
        self._send_on_produce_message(
            key=key,
            value=value,
            partition=partition,
            timestamp=timestamp,
            headers=headers,
        )
        if headers is not None and not self.allow_headers:
            headers = None
        timestamp_ms = int(timestamp * 1000.0) if timestamp else timestamp
        try:
            if transactional_id:
                async with self._trn_locks[transactional_id]:
                    return cast(
                        Awaitable[RecordMetadata],
                        await transaction_producer.send(
                            topic,
                            value,
                            key=key,
                            partition=partition,
                            timestamp_ms=timestamp_ms,
                            headers=headers,
                        ),
                    )
            else:
                return cast(
                    Awaitable[RecordMetadata],
                    await transaction_producer.send(
                        topic,
                        value,
                        key=key,
                        partition=partition,
                        timestamp_ms=timestamp_ms,
                        headers=headers,
                    ),
                )

        except KafkaError as exc:
            raise ProducerSendError(f"Error while sending: {exc!r}") from exc

    async def send_and_wait(
        self,
        topic: str,
        key: Optional[bytes],
        value: Optional[bytes],
        partition: Optional[int],
        timestamp: Optional[float],
        headers: Optional[HeadersArg],
        *,
        transactional_id: Optional[str] = None,
    ) -> RecordMetadata:
        """Send message and wait for it to be transmitted."""
        fut = await self.send(
            topic,
            key=key,
            value=value,
            partition=partition,
            timestamp=timestamp,
            headers=headers,
            transactional_id=transactional_id,
        )
        return await fut

    async def flush(self) -> None:
        """Wait for producer to finish transmitting all buffered messages."""
        await self.buffer.flush()
        if self._producer is not None:
            await self._producer.flush()
        for transaction_producer in self._transaction_producers.values():
            await transaction_producer.flush()

    def key_partition(self, topic: str, key: bytes) -> TP:
        """Hash key to determine partition destination."""
        producer = self._ensure_producer()
        partition = producer._partition(
            topic,
            partition=None,
            key=None,
            value=None,
            serialized_key=key,
            serialized_value=None,
        )
        return TP(topic, partition)

    def supports_headers(self) -> bool:
        """Return :const:`True` if message headers are supported."""
        producer = self._ensure_producer()
        client = producer.client
        if client is None:
            raise NotReady("Producer client not yet connected")
        return client.api_version >= (0, 11)


class Transport(base.Transport):
    """Kafka transport using :pypi:`aiokafka`."""

    Consumer: ClassVar[Type[ConsumerT]]
    Consumer = Consumer
    Producer: ClassVar[Type[ProducerT]]
    Producer = Producer

    default_port = 9092
    driver_version = f"aiokafka={aiokafka.__version__}"

    _topic_waiters: MutableMapping[str, StampedeWrapper]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._topic_waiters = {}

    def _topic_config(
        self,
        retention: Optional[int] = None,
        compacting: Optional[bool] = None,
        deleting: Optional[bool] = None,
    ) -> MutableMapping[str, Any]:
        config: MutableMapping[str, Any] = {}
        cleanup_flags: Set[str] = set()
        if compacting:
            cleanup_flags |= {"compact"}
        if deleting:
            cleanup_flags |= {"delete"}
        if cleanup_flags:
            config["cleanup.policy"] = ",".join(sorted(cleanup_flags))
        if retention:
            config["retention.ms"] = retention
        return config

    async def _create_topic(
        self,
        owner: Service,
        client: aiokafka.AIOKafkaClient,
        topic: str,
        partitions: int,
        replication: int,
        **kwargs: Any,
    ) -> None:
        assert topic is not None
        try:
            wrap = self._topic_waiters[topic]
        except KeyError:
            wrap = self._topic_waiters[topic] = StampedeWrapper(
                self._really_create_topic,
                owner,
                client,
                topic,
                partitions,
                replication,
                loop=self.loop,
                **kwargs,
            )
        try:
            await wrap()
        except Exception:
            self._topic_waiters.pop(topic, None)
            raise

    async def _get_controller_node(
        self, owner: Service, client: aiokafka.AIOKafkaClient, timeout: int = 30000
    ) -> Optional[int]:  # pragma: no cover
        nodes = [broker.nodeId for broker in client.cluster.brokers()]
        for node_id in nodes:
            if node_id is None:
                raise NotReady("Not connected to Kafka Broker")
            request = MetadataRequest_v1([])
            wait_result = await owner.wait(
                client.send(node_id, request),
                timeout=timeout,
            )
            if wait_result.stopped:
                owner.log.info("Shutting down - skipping creation.")
                return None
            response = wait_result.result
            return response.controller_id
        raise Exception("Controller node not found")

    async def _really_create_topic(
        self,
        owner: Service,
        client: aiokafka.AIOKafkaClient,
        topic: str,
        partitions: int,
        replication: int,
        *,
        config: Optional[Mapping[str, Any]] = None,
        timeout: int = 30000,
        retention: Optional[int] = None,
        compacting: Optional[bool] = None,
        deleting: Optional[bool] = None,
        ensure_created: bool = False,
    ) -> None:  # pragma: no cover
        owner.log.info("Creating topic %r", topic)

        if topic in client.cluster.topics():
            owner.log.debug("Topic %r exists, skipping creation.", topic)
            return

        protocol_version = 1
        extra_configs = config or {}
        config = self._topic_config(retention, compacting, deleting)
        config.update(extra_configs)

        controller_node = await self._get_controller_node(
            owner, client, timeout=timeout
        )
        owner.log.debug("Found controller: %r", controller_node)

        if controller_node is None:
            if owner.should_stop:
                owner.log.info("Shutting down hence controller not found")
                return
            else:
                raise Exception("Controller node is None")

        request = CreateTopicsRequest[protocol_version](
            [(topic, partitions, replication, [], list(config.items()))],
            timeout,
            False,
        )
        wait_result = await owner.wait(
            client.send(controller_node, request),
            timeout=timeout,
        )
        if wait_result.stopped:
            owner.log.debug("Shutting down - skipping creation.")
            return
        response = wait_result.result

        assert len(response.topic_errors), "single topic"

        _, code, reason = response.topic_errors[0]

        if code != 0:
            if not ensure_created and code == TopicExistsError.errno:
                owner.log.debug("Topic %r exists, skipping creation.", topic)
                return
            elif code == NotControllerError.errno:
                raise RuntimeError(f"Invalid controller: {controller_node}")
            else:
                raise for_code(code)(f"Cannot create topic: {topic} ({code}): {reason}")
        else:
            owner.log.info("Topic %r created.", topic)
            return


def credentials_to_aiokafka_auth(
    credentials: Optional[CredentialsT] = None, ssl_context: Any = None
) -> Mapping:
    if credentials is not None:
        if isinstance(credentials, SSLCredentials):
            return {
                "security_protocol": credentials.protocol.value,
                "ssl_context": credentials.context,
            }
        elif isinstance(credentials, OAuthCredentials):
            return {
                "security_protocol": credentials.protocol.value,
                "sasl_mechanism": credentials.mechanism.value,
                "sasl_oauth_token_provider": credentials.oauth_cb,
                "ssl_context": credentials.ssl_context,
            }
        elif isinstance(credentials, SASLCredentials):
            return {
                "security_protocol": credentials.protocol.value,
                "sasl_mechanism": credentials.mechanism.value,
                "sasl_plain_username": credentials.username,
                "sasl_plain_password": credentials.password,
                "ssl_context": credentials.ssl_context,
            }
        elif isinstance(credentials, GSSAPICredentials):
            return {
                "security_protocol": credentials.protocol.value,
                "sasl_mechanism": credentials.mechanism.value,
                "sasl_kerberos_service_name": credentials.kerberos_service_name,
                "sasl_kerberos_domain_name": credentials.kerberos_domain_name,
                "ssl_context": credentials.ssl_context,
            }
        else:
            raise ImproperlyConfigured(f"aiokafka does not support {credentials}")
    elif ssl_context is not None:
        return {
            "security_protocol": "SSL",
            "ssl_context": ssl_context,
        }
    else:
        return {"security_protocol": "PLAINTEXT"}


def ensure_aiokafka_TP(tp: TP) -> _TopicPartition:
    """Convert Faust ``TP`` to aiokafka ``TopicPartition``."""
    return (
        tp
        if isinstance(tp, _TopicPartition)
        else _TopicPartition(tp.topic, tp.partition)
    )


def ensure_aiokafka_TPset(tps: Iterable[TP]) -> Set[_TopicPartition]:
    """Convert set of Faust ``TP`` to aiokafka ``TopicPartition``."""
    return {ensure_aiokafka_TP(tp) for tp in tps}
