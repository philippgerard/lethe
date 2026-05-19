"""Integration layer — connects actors to the existing Agent/LLM system.

The cortex (principal) runs in intentional hybrid mode:
- Handle quick local tasks directly (CLI/file/memory/telegram)
- Delegate long or parallel work to subagents

The DMN (Default Mode Network) is a persistent background subagent that
replaces heartbeats. It scans goals, reorganizes memory, self-improves,
and notifies the cortex when something needs user attention.
"""

import asyncio
import logging
from typing import Awaitable, Callable, Dict, List, Optional

from lethe.actor import Actor, ActorConfig, ActorMessage, ActorRegistry, ActorState, MessageIntent, ModelTier
from lethe.actor.tools import create_actor_tools
from lethe.actor.runner import ActorRunner
from lethe.actor.dmn import DefaultModeNetwork
# Amygdala merged into hippocampus — salience tagging now runs per-message
from lethe.actor.brainstem import Brainstem
from lethe.memory.curator import run_curator
from lethe.config import Settings, get_settings
from lethe.memory.llm import AsyncLLMClient, LLMConfig
from lethe.notification_gate import GateAction, NotificationGate
from lethe.notification_router import NotificationRouter
from lethe.notification_scoring import NotificationScoring
from lethe.notification_signals import UserNotificationSignal
from lethe.tools.policy import CORTEX_TOOL_NAMES, SUBAGENT_DEFAULT_TOOL_NAMES, SUBAGENT_EXCLUDED_TOOL_NAMES

logger = logging.getLogger(__name__)

# Backward-compatible exported names. The canonical policy lives in
# lethe.tools.policy.
SUBAGENT_DEFAULT_TOOLS = set(SUBAGENT_DEFAULT_TOOL_NAMES)


class ActorSystem:
    """Manages the actor system, wiring it into the existing Agent.
    
    The cortex (principal) is hybrid: quick local tasks directly, long work delegated.
    Subagents still get the broad tool surface for deeper/parallel tasks.
    """

    def __init__(self, agent, settings: Optional[Settings] = None):
        self.agent = agent
        self.settings = settings or get_settings()
        self.registry = ActorRegistry()
        self.principal: Optional[Actor] = None
        self.brainstem: Optional[Brainstem] = None
        self.dmn: Optional[DefaultModeNetwork] = None
        self._background_tasks: Dict[str, asyncio.Task] = {}
        self._principal_monitor_task: Optional[asyncio.Task] = None
        self._processed_principal_message_ids: set[str] = set()
        self._last_principal_message_idx = 0
        
        # Tools from the agent that subagents can use (not the cortex)
        self._available_tools: Dict[str, tuple] = {}
        
        # Callbacks set by main.py
        self._send_to_user: Optional[Callable] = None
        self._get_reminders: Optional[Callable] = None
        self._run_cortex_turn: Optional[Callable[[str], Awaitable[None]]] = None
        self._review_user_notification: Optional[Callable[[UserNotificationSignal], Awaitable[None]]] = None
        self._pending_user_notifications: List[UserNotificationSignal] = []
        self._notification_router: Optional[NotificationRouter] = None
        self._notification_scoring = NotificationScoring()
        self._notification_gate = NotificationGate()

    def _get_principal_context(self) -> str:
        """Build principal context for DMN from live memory blocks."""
        try:
            blocks = getattr(self.agent, "memory", None).blocks
            identity = blocks.get("identity") or {}
            human = blocks.get("human") or {}
            project = blocks.get("project") or {}

            def _extract(value: str, max_lines: int = 40) -> str:
                text = (value or "").strip()
                if not text:
                    return ""
                lines = text.splitlines()
                if len(lines) <= max_lines:
                    return text
                return "\n".join(lines[:max_lines]) + "\n...[truncated by lines]"

            parts = []
            if identity.get("value"):
                parts.append(f"Identity snapshot:\n{_extract(identity.get('value', ''))}")
            if human.get("value"):
                parts.append(f"Human context:\n{_extract(human.get('value', ''))}")
            if project.get("value"):
                parts.append(f"Project context:\n{_extract(project.get('value', ''))}")
            if not parts:
                return (
                    "Advance your principal's goals with current memory context. "
                    "If context is missing, prioritize building fresh actionable context."
                )
            return "\n\n".join(parts)
        except Exception as e:
            logger.warning(f"Failed to build principal context for DMN: {e}")
            return "Advance your principal's goals based on current memory and recent activity."

    async def setup(self):
        """Set up the actor system.
        
        1. Collect agent's tools for subagent use
        2. Strip non-actor tools from the agent's LLM (cortex doesn't use them)
        3. Create principal actor
        4. Register actor tools with the agent
        """
        # Collect all agent tools BEFORE stripping them (for subagent use)
        self._collect_available_tools()

        # Create principal actor
        self.principal = self.registry.spawn(
            ActorConfig(
                name="cortex",
                group="main",
                goals="Serve the user. Handle quick tasks directly. Delegate long or complex tasks to subagents.",
            ),
            is_principal=True,
        )
        self._notification_router = NotificationRouter(self.registry, self.principal.id)
        self.registry.events.subscribe(self._on_actor_event)
        
        # Set up LLM factory
        self.registry.set_llm_factory(self._create_llm_for_actor)
        
        # Register actor tools with the cortex's LLM
        actor_tools = create_actor_tools(self.principal, self.registry)
        for func, _ in actor_tools:
            self.agent.add_tool(func)

        # Strip tools down to CORTEX_TOOL_NAMES (keep under 15 for Gemma 4).
        # Stash stripped tools in _EXTENDED_TOOLS so request_tool() can activate them.
        if hasattr(self.agent, 'llm') and hasattr(self.agent.llm, 'restrict_tools'):
            from lethe.tools import _EXTENDED_TOOLS
            removed = self.agent.llm.restrict_tools(CORTEX_TOOL_NAMES)
            for name, (func, _schema) in removed.items():
                if name not in _EXTENDED_TOOLS:
                    _EXTENDED_TOOLS[name] = (func, None)
            logger.info(
                f"Cortex tools: {len(self.agent.llm.tools)} "
                f"(stripped {len(removed)}: {sorted(removed)})"
            )

        # Wire principal actor into the agent so it can drain inbox and see actor context.
        self.agent._principal_actor = self.principal
        def _principal_actor_context() -> str:
            if self.principal:
                return self.principal.build_system_prompt()
            return ""
        self.agent._actor_context_provider = _principal_actor_context

        # Auto-start ordinary child actors via an explicit registry hook.
        self.registry.subscribe_spawn(self._on_actor_spawned)
        
        # Rebuild tool reference in system prompt (was built before stripping)
        if hasattr(self.agent, 'llm'):
            assembler = getattr(self.agent, 'assembler', None)
            if assembler and assembler.should_embed_tool_reference():
                self.agent.llm.context._tool_reference = self.agent.llm.context._build_tool_reference(self.agent.llm.tools)
                logger.info(f"Rebuilt tool reference ({len(self.agent.llm.context._tool_reference)} chars)")
            else:
                self.agent.llm.context._tool_reference = ""
            self.agent.llm._update_tool_budget()
        
        # Initialize Brainstem FIRST. It supervises boot and runtime health.
        self.brainstem = Brainstem(
            registry=self.registry,
            settings=self.settings,
            cortex_id=self.principal.id,
        )
        await self.brainstem.startup()

        # Initialize DMN (Default Mode Network) — persistent background thinker
        self.dmn = DefaultModeNetwork(
            registry=self.registry,
            llm_factory=self._create_llm_for_actor,
            available_tools=self._available_tools,
            cortex_id=self.principal.id,
            send_to_user=self._send_to_user or (lambda msg: asyncio.sleep(0)),
            get_reminders=self._get_reminders,
            principal_context_provider=self._get_principal_context,
            model_override=self.settings.llm_model_dmn,
        )
        tool_count = len(self.agent.llm.tools)
        available_count = len(self._available_tools)
        logger.info(
            f"Actor system initialized. Principal: {self.principal.id}, "
            f"cortex tools: {tool_count}, subagent tools available: {available_count}, "
            "Brainstem online, DMN ready, Memory curator active"
        )
        self._start_principal_monitor()

    # Tools subagents must NOT have — they communicate via actors only
    SUBAGENT_EXCLUDED_TOOLS = set(SUBAGENT_EXCLUDED_TOOL_NAMES)

    def _collect_available_tools(self):
        """Collect tools from the agent for subagent use.
        
        This runs BEFORE stripping cortex tools, so it captures everything.
        Subagents can request any tool EXCEPT telegram (they message actors, not users).
        """
        if hasattr(self.agent, 'llm') and hasattr(self.agent.llm, 'iter_tool_entries'):
            for name, func, schema in self.agent.llm.iter_tool_entries():
                if name not in self.SUBAGENT_EXCLUDED_TOOLS:
                    self._available_tools[name] = (func, schema)

    def get_available_tool_names(self) -> List[str]:
        """List tool names available for subagents."""
        return sorted(self._available_tools.keys())

    def _should_autostart_actor(self, actor: Actor) -> bool:
        return (
            not actor.is_principal
            and actor.config.name not in {"brainstem", "dmn"}
            and actor.id not in self._background_tasks
        )

    def _on_actor_spawned(self, actor: Actor):
        """Start ordinary spawned actors without monkey-patching the registry."""
        if self._should_autostart_actor(actor):
            self._start_actor(actor)

    async def _create_llm_for_actor(self, actor: Actor) -> AsyncLLMClient:
        """Create an LLM client for a subagent actor."""
        config = LLMConfig()
        if actor.config.model is ModelTier.MAIN:
            pass  # config.model is already the main model
        else:
            config.model = config.model_aux
        
        config.context_limit = min(config.context_limit, 64000)
        config.max_output_tokens = min(config.max_output_tokens, 4096)
        
        client = AsyncLLMClient(
            config=config,
            system_prompt=actor.build_system_prompt(),
            usage_scope=f"actor:{actor.config.name}",
        )
        if hasattr(self.agent, 'assembler'):
            client.context._assembler = self.agent.assembler

        return client

    def _start_actor(self, actor: Actor):
        """Start a non-principal actor running in the background."""
        async def _run():
            try:
                runner = ActorRunner(
                    actor=actor,
                    registry=self.registry,
                    llm_factory=self._create_llm_for_actor,
                    available_tools=self._available_tools,
                )
                result = await runner.run()
                logger.info(f"Actor {actor.config.name} (id={actor.id}) finished: {result[:80]}...")
            except Exception as e:
                logger.error(f"Actor {actor.config.name} (id={actor.id}) error: {e}", exc_info=True)
                if actor.state != ActorState.TERMINATED:
                    actor.terminate(f"Error: {e}")
            finally:
                self._background_tasks.pop(actor.id, None)
        
        task = asyncio.create_task(_run(), name=f"actor-{actor.id}-{actor.config.name}")
        self._background_tasks[actor.id] = task
        actor.set_task_handle(task)
        logger.info(f"Started background actor: {actor.config.name} (id={actor.id})")

    def _start_principal_monitor(self):
        """Monitor principal inbox/messages even when cortex is not in an active LLM loop."""
        if self._principal_monitor_task and not self._principal_monitor_task.done():
            return

        async def _monitor():
            while True:
                try:
                    await asyncio.sleep(1.0)
                    if not self.principal or self.principal.state == ActorState.TERMINATED:
                        continue
                    all_messages = self.principal.messages
                    if self._last_principal_message_idx >= len(all_messages):
                        continue
                    new_messages = all_messages[self._last_principal_message_idx:]
                    self._last_principal_message_idx = len(all_messages)
                    for msg in new_messages:
                        if msg.id in self._processed_principal_message_ids:
                            continue
                        if msg.recipient != self.principal.id:
                            continue
                        if msg.sender == self.principal.id:
                            continue
                        self._processed_principal_message_ids.add(msg.id)
                        await self._handle_principal_message(msg)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning(f"Principal monitor error: {e}")

        self._principal_monitor_task = asyncio.create_task(_monitor(), name="actor-principal-monitor")

    async def _on_actor_event(self, event):
        """Route background user notifications through scoring, gating, and review."""
        if not self.principal or not self._notification_router:
            return

        signal = self._notification_router.from_actor_event(event)
        if not signal:
            return

        self.registry.emit_event(
            "notification_signal_received",
            self.principal,
            {
                "signal_id": signal.event_id,
                "source": signal.source.value,
                "origin": signal.origin.value,
                "category": signal.category.value,
                "urgency": signal.urgency.value,
                "kind": signal.kind,
                "content_preview": signal.content[:240],
            },
        )

        assessment = self._notification_scoring.assess(signal)
        decision = self._notification_gate.decide(signal, assessment)
        self.registry.emit_event(
            "notification_gate_decision",
            self.principal,
            {
                "signal_id": signal.event_id,
                "action": decision.action.value,
                "reason": decision.reason,
                "urgency_score": assessment.urgency,
                "user_relevance": assessment.user_relevance,
                "interruptibility": assessment.interruptibility,
            },
        )
        if decision.action == GateAction.DROP:
            logger.info(
                "Notification gate dropped %s %s (%s): %s",
                signal.source.value,
                signal.category.value,
                decision.reason,
                signal.content[:120],
            )
            return

        if self._review_user_notification:
            self.registry.emit_event(
                "notification_review_requested",
                self.principal,
                {
                    "signal_id": signal.event_id,
                    "source": signal.source.value,
                    "category": signal.category.value,
                    "urgency": signal.urgency.value,
                },
            )
            try:
                await self._review_user_notification(signal)
            except Exception as e:
                logger.warning("Notification review failed for %s: %s", signal.source.value, e)
        else:
            self._pending_user_notifications.append(signal)
            self.registry.emit_event(
                "notification_signal_queued",
                self.principal,
                {
                    "signal_id": signal.event_id,
                    "source": signal.source.value,
                    "category": signal.category.value,
                    "urgency": signal.urgency.value,
                },
            )
            logger.info(
                "Queued user-facing signal from %s until notification review is ready",
                signal.source.value,
            )

    async def _handle_principal_message(self, message: ActorMessage):
        """Route child→principal messages using MessageIntent for dispatch."""
        content = (message.content or "").strip()
        sender = self.registry.get(message.sender)
        sender_name = sender.config.name if sender else message.sender
        intent = message.intent
        if intent == MessageIntent.MESSAGE and message.metadata:
            intent = MessageIntent.from_strings(
                message.metadata.get("channel", ""),
                message.metadata.get("kind", ""),
            )

        _BACKGROUND_ACTORS = {"brainstem", "dmn"}
        is_background = sender_name in _BACKGROUND_ACTORS
        if is_background:
            return

        if self.principal:
            self.registry.emit_event(
                "principal_update_received",
                self.principal,
                {
                    "from_actor_id": message.sender,
                    "from_actor_name": sender_name,
                    "message_id": message.id,
                    "content_preview": content[:240],
                    "intent": intent.value,
                },
            )
        logger.info(
            "Principal message from %s: intent=%s content=%s",
            sender_name, intent.value, content[:120],
        )

        # --- Subagent task lifecycle ---
        if intent.channel == "task_update" and intent.wakes_cortex:
            if intent.is_terminal:
                synthetic = (
                    f"[System: subagent '{sender_name}' finished ({intent.value}). "
                    f"Its result is in your inbox. Review it and respond to the user.]"
                )
            else:
                synthetic = (
                    f"[System: subagent '{sender_name}' sent a progress update. "
                    f"The subagent is still running; do not kill, restart, ping, or otherwise manage it "
                    f"unless the user explicitly asked you to intervene or the update says it is blocked. "
                    f"You may send the user a brief status update if it adds value; otherwise return no text. "
                    f"Progress: {content[:500]}]"
                )
            if self._run_cortex_turn:
                logger.info("Triggering cortex turn for subagent '%s' %s", sender_name, intent.value)
                try:
                    await self._run_cortex_turn(synthetic)
                except Exception as e:
                    logger.warning("Cortex turn for subagent %s failed: %s", intent.value, e)
            else:
                logger.warning("No run_cortex_turn callback; subagent '%s' %s stuck in inbox", sender_name, intent.value)
            return

        return

    def set_callbacks(
        self,
        send_to_user: Callable,
        get_reminders: Optional[Callable] = None,
        run_cortex_turn: Optional[Callable[[str], Awaitable[None]]] = None,
        review_user_notification: Optional[Callable[[UserNotificationSignal], Awaitable[None]]] = None,
    ):
        """Set callbacks for DMN and actor system.

        Args:
            send_to_user: async Callable(message: str) -> None
            get_reminders: async Callable() -> str
            run_cortex_turn: async Callable(synthetic_message: str) -> None — triggers a full cortex LLM turn for subagent updates
            review_user_notification: async Callable(signal: UserNotificationSignal) -> None — review for background user-facing signals
        """
        self._send_to_user = send_to_user
        self._get_reminders = get_reminders
        self._run_cortex_turn = run_cortex_turn
        self._review_user_notification = review_user_notification
        if self.dmn:
            self.dmn.send_to_user = send_to_user
            self.dmn.get_reminders = get_reminders
        if review_user_notification and self._pending_user_notifications:
            asyncio.create_task(self._flush_pending_user_notifications())

    async def _flush_pending_user_notifications(self):
        if not self._review_user_notification:
            return
        pending = list(self._pending_user_notifications)
        self._pending_user_notifications.clear()
        for signal in pending:
            try:
                await self._review_user_notification(signal)
            except Exception as e:
                logger.warning("Failed to flush queued signal %s: %s", signal.event_id, e)

    async def dmn_round(self) -> Optional[str]:
        """Run a DMN round. Called by heartbeat timer.

        Returns:
            Message to send to user, or None
        """
        if self.dmn is None:
            return None
        return await self.dmn.run_round()

    async def curator_round(self):
        """Run memory curator if cadence elapsed (6h). Called by heartbeat."""
        try:
            stats = await run_curator(
                self.agent.notes,
                self.agent.memory.archival,
                self.agent.memory.messages,
            )
            if not stats.get("skipped"):
                logger.info("Curator round: %s", stats)
        except Exception as e:
            logger.error("Curator round failed: %s", e)

    async def brainstem_heartbeat(self, heartbeat_message: str = "") -> Optional[str]:
        """Run Brainstem supervisory checks (main heartbeat cadence)."""
        if self.brainstem is None:
            return None
        await self.brainstem.heartbeat(heartbeat_message=heartbeat_message)
        return None

    async def background_round(self) -> Optional[str]:
        """Run background cognition rounds (DMN + Consolidation).

        Salience tagging now runs per-message in hippocampus.
        """
        dmn_result = await self.dmn_round()
        await self.curator_round()  # self-gating via 6h cadence
        return dmn_result

    async def shutdown(self):
        """Shut down all actors gracefully."""
        logger.info(f"Shutting down actor system ({self.registry.active_count} active actors)")
        if self.brainstem:
            try:
                self.brainstem.record_shutdown()
            except Exception as e:
                logger.debug("Brainstem shutdown marker failed: %s", e)
        if self._principal_monitor_task and not self._principal_monitor_task.done():
            self._principal_monitor_task.cancel()
            try:
                await self._principal_monitor_task
            except asyncio.CancelledError:
                pass
        
        for actor in list(self.registry._actors.values()):
            if not actor.is_principal and actor.state != ActorState.TERMINATED:
                actor.terminate("System shutdown")
        
        tasks = list(self._background_tasks.values())
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        if self.principal and self.principal.state != ActorState.TERMINATED:
            self.principal.terminate("System shutdown")
        
        self.registry.cleanup_terminated(force=True)
        logger.info("Actor system shut down")

    @property
    def status(self) -> dict:
        all_events = self.registry.events.query(limit=500)
        recent_events = all_events[-10:]
        lifecycle_events = [
            e for e in all_events
            if e.event_type in {"actor_spawned", "actor_terminated"}
        ][-30:]
        dmn_status = self.dmn.status if self.dmn else {}
        brainstem_status = self.brainstem.status if self.brainstem else {}
        actor_last_event_at: dict[str, str] = {}
        for e in all_events:
            actor_last_event_at[e.actor_id] = e.created_at.isoformat()
        return {
            "active_actors": self.registry.active_count,
            "background_tasks": len(self._background_tasks),
            "principal_monitor_running": bool(
                self._principal_monitor_task and not self._principal_monitor_task.done()
            ),
            "actors": [
                {
                    "id": a.id,
                    "name": a.name,
                    "group": a.group,
                    "state": a.state.value,
                    "task_state": a.task_state.value,
                    "goals": a.goals[:80],
                }
                for a in self.registry.all_actors
            ],
            "actor_last_event_at": actor_last_event_at,
            "recent_events": [
                {
                    "type": e.event_type,
                    "actor_id": e.actor_id,
                    "actor_name": (self.registry.get(e.actor_id).config.name if self.registry.get(e.actor_id) else ""),
                    "group": e.group,
                    "payload": e.payload,
                    "created_at": e.created_at.isoformat(),
                }
                for e in recent_events
            ],
            "lifecycle_events": [
                {
                    "type": e.event_type,
                    "actor_id": e.actor_id,
                    "actor_name": (
                        (e.payload.get("name") if isinstance(e.payload, dict) else "")
                        or (self.registry.get(e.actor_id).config.name if self.registry.get(e.actor_id) else "")
                    ),
                    "created_at": e.created_at.isoformat(),
                }
                for e in lifecycle_events
            ],
            "brainstem": brainstem_status,
            "dmn": dmn_status,
        }
