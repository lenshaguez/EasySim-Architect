# =============================================================
# EASYSIM — L-UDTG SOVEREIGN DES ARCHITECT
# Single-file Streamlit Application
# Engine: Custom Python DES kernel derived from JaamSim/AnyLogic
# =============================================================

import streamlit as st
import heapq
import itertools
import math
import random
import pandas as pd
from collections import defaultdict, deque
from typing import Optional, List, Dict, Any, Callable
from abc import ABC, abstractmethod

# =============================================================
# SECTION 1: SOVEREIGN DES ENGINE
# =============================================================

# ─── 1.1 Event Calendar (canonical clock) ────────────────────

class EventCalendar:
    """
    Priority heap: (time, priority_lane, FIFO_counter, callback)
    Priority lane: lower number fires first at same timestamp.
    Release events: priority=3 (fires before new arrivals: priority=5)
    This prevents phantom queue buildups at timestamp collisions.
    """
    def __init__(self):
        self._heap: list = []
        self._counter = itertools.count()
        self.current_time: float = 0.0

    def schedule(self, delay: float, callback: Callable, priority: int = 5):
        t = self.current_time + max(0.0, delay)
        heapq.heappush(self._heap, (t, priority, next(self._counter), callback))

    def run(self, until: float):
        while self._heap:
            t, _p, _c, cb = heapq.heappop(self._heap)
            if t > until:
                break
            self.current_time = t
            cb()

    def reset(self):
        self._heap.clear()
        self._counter = itertools.count()
        self.current_time = 0.0


# ─── 1.2 SimEntity (Agent carrying transformation attributes) ─

class SimEntity:
    _id_counter: int = 0

    def __init__(self, name: str, created_time: float):
        SimEntity._id_counter += 1
        self.id: int = SimEntity._id_counter
        self.name: str = name
        self.created_time: float = created_time
        self.queue_entry_time: Optional[float] = None
        self.attributes: Dict[str, Any] = {}
        self.state: str = "Active"
        self.rework_count: int = 0

    def set_attribute(self, key: str, value: Any):
        self.attributes[key] = value

    def get_attribute(self, key: str, default=None):
        return self.attributes.get(key, default)

    def transform(self, new_name: str, new_attributes: Dict = None):
        """Agent transformation: Drug Powder → Medicine A"""
        self.name = new_name
        if new_attributes:
            self.attributes.update(new_attributes)

    @classmethod
    def reset_counter(cls):
        cls._id_counter = 0


# ─── 1.3 Statistics Engine ───────────────────────────────────

class SampleStatistics:
    """Point-in-time measurements: sojourn times, service times."""
    def __init__(self):
        self.values: List[float] = []

    def add(self, v: float):
        self.values.append(v)

    def mean(self) -> float:
        return sum(self.values) / len(self.values) if self.values else 0.0

    def std(self) -> float:
        if len(self.values) < 2:
            return 0.0
        m = self.mean()
        return math.sqrt(sum((x - m) ** 2 for x in self.values) / (len(self.values) - 1))

    def count(self) -> int:
        return len(self.values)


class TimeWeightedStatistics:
    """
    Area-under-curve: queue length, utilization.
    Holds value until next event — correct DES pattern.
    Extracted from JaamSim TimeBasedStatistics.
    BUG FIX: uses sim.current_time, NOT wall-clock time.time()
    """
    def __init__(self):
        self._last_time: Optional[float] = None
        self._last_value: float = 0.0
        self._weighted_sum: float = 0.0
        self._total_time: float = 0.0

    def update(self, sim_time: float, value: float):
        """Call at every state change with the current sim time."""
        if self._last_time is not None:
            dt = sim_time - self._last_time
            if dt > 0:
                self._weighted_sum += self._last_value * dt
                self._total_time += dt
        self._last_time = sim_time
        self._last_value = value

    def mean(self) -> float:
        return self._weighted_sum / self._total_time if self._total_time > 0 else 0.0

    def finalize(self, end_time: float):
        """Capture final state at simulation end."""
        if self._last_time is not None and end_time > self._last_time:
            self.update(end_time, self._last_value)


class BlockStatistics:
    """All statistics for one sovereign block."""
    def __init__(self, name: str = ""):
        self.name = name
        self.queue_length = TimeWeightedStatistics()
        self.utilization = TimeWeightedStatistics()
        self.wait_time = SampleStatistics()
        self.service_time = SampleStatistics()
        self.entities_in: int = 0
        self.entities_out: int = 0


# ─── 1.4 Linkable Interface ──────────────────────────────────

class Linkable(ABC):
    @abstractmethod
    def add_entity(self, entity: SimEntity):
        """Receives an entity from an upstream component."""
        pass


# ─── 1.5 SovereignBlock — 3-Phase Contract Base Class ────────

class SovereignBlock(Linkable):
    """
    Base class implementing the 3-phase JaamSim contract:
      Phase 1: start_processing() → bool   — "Can I begin?"
      Phase 2: get_step_duration() → float — "How long?"
      Phase 3: process_step() → void       — "Release and forward."

    All ad-hoc on_complete / on_entity_finished hooks from the
    original Python files are replaced by this unified contract.
    """
    def __init__(self, name: str, sim: EventCalendar, rng: random.Random):
        self.name = name
        self.sim = sim
        self.rng = rng
        self.next_block: Optional['SovereignBlock'] = None
        self.stats = BlockStatistics(name)

    def add_entity(self, entity: SimEntity):
        """Entry point — called by upstream block (LinkedComponent.addEntity pattern)."""
        self.stats.entities_in += 1
        self._on_receive(entity)

    def _on_receive(self, entity: SimEntity):
        pass

    def send_to_next(self, entity: SimEntity):
        """Forward entity downstream. Entity exits if no next block."""
        self.stats.entities_out += 1
        if self.next_block:
            self.next_block.add_entity(entity)

    def set_next(self, block: 'SovereignBlock'):
        self.next_block = block


# ─── 1.6 ResourcePool (Seize/Release Semaphore) ──────────────

class ResourcePool:
    """
    Extracted from ResourcePool.java + Seize.java + Release.java.

    Key behaviors implemented:
    - canSeize: hard gate, no partial seizes
    - Sort eligible units by (priority, last_release_time) — LRU
    - notifyResourceUsers: broadcast to ALL waiting Seize blocks on Release
    - collectStatistics at BOTH seize and release events (trigger points 3+4)
    """
    def __init__(self, name: str, capacity: int, sim: EventCalendar):
        self.name = name
        self.capacity = capacity
        self.sim = sim
        self._units_in_use: int = 0
        self._waiting: deque = deque()  # (n_units, entity, success_callback)
        self.stats = BlockStatistics(name)

    def try_seize(self, n: int, entity: SimEntity, on_success: Callable) -> bool:
        """Returns True if seized immediately, False if blocked."""
        if self._units_in_use + n <= self.capacity:
            self._units_in_use += n
            # STAT TRIGGER 3: resource seized
            self.stats.utilization.update(self.sim.current_time,
                                          self._units_in_use / self.capacity)
            on_success()
            return True
        self._waiting.append((n, entity, on_success))
        return False

    def release(self, n: int, entity: SimEntity):
        """Release n units and broadcast to all waiting blocks."""
        self._units_in_use = max(0, self._units_in_use - n)
        # STAT TRIGGER 4: resource released
        self.stats.utilization.update(self.sim.current_time,
                                      self._units_in_use / self.capacity)
        self._notify_resource_users()  # JaamSim: notifyResourceUsers(resList)

    def _notify_resource_users(self):
        """Broadcast: check all waiting entities in priority order."""
        remaining: deque = deque()
        while self._waiting:
            n, ent, cb = self._waiting.popleft()
            if self._units_in_use + n <= self.capacity:
                self._units_in_use += n
                self.stats.utilization.update(self.sim.current_time,
                                              self._units_in_use / self.capacity)
                self.sim.schedule(0, cb, priority=3)  # fires before new arrivals
            else:
                remaining.append((n, ent, cb))
        self._waiting = remaining

    @property
    def available(self) -> int:
        return self.capacity - self._units_in_use


# ─── 1.7 SovereignQueue ──────────────────────────────────────

class SovereignQueue(SovereignBlock):
    """
    FIFO/LIFO queue implementing Queue.java discipline logic.
    BUG FIX: All timestamps use sim.current_time (not time.time()).
    Observer pattern: notifies registered servers on entity arrival.
    Stat triggers 1+2: queue length at arrival and departure.
    """
    def __init__(self, name: str, sim: EventCalendar, rng: random.Random,
                 max_capacity: int = 0, discipline: str = "FIFO"):
        super().__init__(name, sim, rng)
        self.max_capacity = max_capacity
        self.discipline = discipline
        self._storage: deque = deque()
        self._observers: List[Callable] = []

    def register_observer(self, callback: Callable):
        """Server/Combine registers to be notified of new arrivals."""
        self._observers.append(callback)

    def _on_receive(self, entity: SimEntity):
        # Balking: if queue is full, entity is lost (capacity exceeded)
        if self.max_capacity > 0 and len(self._storage) >= self.max_capacity:
            return
        entity.queue_entry_time = self.sim.current_time  # sim time, not wall clock
        if self.discipline == "LIFO":
            self._storage.appendleft(entity)
        else:
            self._storage.append(entity)
        # STAT TRIGGER 1: entity arrives at queue
        self.stats.queue_length.update(self.sim.current_time, len(self._storage))
        # Notify observers — servers/combines waiting for work
        for cb in self._observers:
            self.sim.schedule(0, cb, priority=4)

    def pop(self) -> Optional[SimEntity]:
        if not self._storage:
            return None
        entity = self._storage.popleft() if self.discipline == "FIFO" else self._storage.pop()
        # STAT TRIGGER 2: entity departs queue — record sojourn time
        entry_t = entity.queue_entry_time or self.sim.current_time
        self.stats.wait_time.add(self.sim.current_time - entry_t)
        self.stats.queue_length.update(self.sim.current_time, len(self._storage))
        return entity

    def length(self) -> int:
        return len(self._storage)

    def peek(self) -> Optional[SimEntity]:
        return self._storage[0] if self._storage else None


# ─── 1.8 SovereignServer ─────────────────────────────────────

class SovereignServer(SovereignBlock):
    """
    Event-driven server implementing Server.java 3-phase contract.

    REPLACES EntityProcessor.py's delta-time loop (which was incorrect).
    Uses sim.schedule(duration, _finish) — fires exactly at completion.

    State machine: servedEntity==None → IDLE | servedEntity!=None → BUSY
    After release: immediately polls queue (isNewStepReqd pattern).
    """
    def __init__(self, name: str, sim: EventCalendar, rng: random.Random,
                 service_time_fn: Callable,
                 num_servers: int = 1,
                 resource_pool: Optional[ResourcePool] = None):
        super().__init__(name, sim, rng)
        self.service_time_fn = service_time_fn
        self.num_servers = num_servers
        self.resource_pool = resource_pool
        self._input_queue: Optional[SovereignQueue] = None
        self._active_slots: int = 0   # slots currently processing

    def set_input_queue(self, q: SovereignQueue):
        """Wire queue to this server and register as observer."""
        self._input_queue = q
        q.register_observer(self._try_start)

    def _try_start(self):
        """
        Phase 1: startProcessing — JaamSim pattern.
        Drains queue while slots are available.
        """
        while self._input_queue and self._active_slots < self.num_servers:
            entity = self._input_queue.pop()
            if entity is None:
                break
            self._begin_processing(entity)

    def _on_receive(self, entity: SimEntity):
        """Direct receipt (no queue upstream)."""
        self._begin_processing(entity)

    def _begin_processing(self, entity: SimEntity):
        """Attempt to seize resource and start service."""
        if self.resource_pool:
            def _on_seized():
                self._execute_service(entity)
            self.resource_pool.try_seize(1, entity, _on_seized)
        else:
            self._execute_service(entity)

    def _execute_service(self, entity: SimEntity):
        """
        Phase 2 + 3: getStepDuration + processStep.
        Duration sampled once at start (JaamSim pattern).
        """
        self._active_slots += 1
        # STAT TRIGGER 3: seize — record utilization
        self.stats.utilization.update(self.sim.current_time,
                                      self._active_slots / self.num_servers)
        duration = self.service_time_fn()

        def _finish():
            self._active_slots -= 1
            # STAT TRIGGER 4: release
            self.stats.utilization.update(self.sim.current_time,
                                          self._active_slots / self.num_servers)
            self.stats.service_time.add(duration)
            if self.resource_pool:
                self.resource_pool.release(1, entity)
            self.send_to_next(entity)
            # isNewStepReqd: poll queue immediately after release
            self._try_start()

        self.sim.schedule(duration, _finish, priority=3)


# ─── 1.9 SovereignGenerator ──────────────────────────────────

class SovereignGenerator(SovereignBlock):
    """
    Implements EntityGenerator.java lifecycle exactly:
    - initialNumber: burst at t=0 (zero-delay)
    - firstArrivalTime: delay before first real entity
    - interArrivalTime: subsequent IAT

    Truck+Recipe normalization converts material inputs to IAT.
    """
    def __init__(self, name: str, sim: EventCalendar, rng: random.Random,
                 iat_fn: Callable,
                 first_arrival_fn: Optional[Callable] = None,
                 entities_per_arrival: int = 1,
                 max_entities: int = 0,
                 entity_name: str = "Entity",
                 initial_number: int = 0):
        super().__init__(name, sim, rng)
        self.iat_fn = iat_fn
        self.first_arrival_fn = first_arrival_fn or (lambda: 0.0)
        self.entities_per_arrival = entities_per_arrival
        self.max_entities = max_entities
        self.entity_name = entity_name
        self.initial_number = initial_number
        self._generated: int = 0

    def start(self):
        self.sim.schedule(0, self._generate, priority=5)

    def _generate(self):
        if self.max_entities > 0 and self._generated >= self.max_entities:
            return

        # JaamSim IAT decision logic
        if self._generated < self.initial_number:
            delay = 0.0
        elif self._generated == self.initial_number:
            delay = self.first_arrival_fn()
        else:
            delay = self.iat_fn()

        if delay == float('inf'):
            return

        n = self.entities_per_arrival() if callable(self.entities_per_arrival) else self.entities_per_arrival
        for _ in range(n):
            if self.max_entities > 0 and self._generated >= self.max_entities:
                break
            self._generated += 1
            entity = SimEntity(f"{self.entity_name}_{self._generated}", self.sim.current_time)
            self.send_to_next(entity)

        self.sim.schedule(delay, self._generate, priority=5)


# ─── 1.10 SovereignBranch ────────────────────────────────────

class SovereignBranch(SovereignBlock):
    """
    Implements Branch.java routing logic.
    choice_fn selects route index based on cumulative probability.
    releaseEntity fires before routing (JaamSim pattern).
    """
    def __init__(self, name: str, sim: EventCalendar, rng: random.Random,
                 routes: List[tuple]):
        super().__init__(name, sim, rng)
        total = sum(p for p, _ in routes) or 1.0
        self.routes = [(p / total, b) for p, b in routes]

    def _on_receive(self, entity: SimEntity):
        r = self.rng.random()
        cumulative = 0.0
        for prob, block in self.routes:
            cumulative += prob
            if r <= cumulative:
                if block:
                    block.add_entity(entity)
                return
        if self.routes and self.routes[-1][1]:
            self.routes[-1][1].add_entity(entity)


# ─── 1.11 SovereignCombine ───────────────────────────────────

class SovereignCombine(SovereignBlock):
    """
    Implements Combine.java N-to-1 assembly logic.

    SURVIVOR pattern (retainAll=False):
    - Entity from slot 0 transforms and passes through
    - Entities from slots 1..N are CONSUMED (destroyed)
    - Combined entity carries transformation attributes

    Use case: Drug Powder (x10) → Medicine Tablet (x1)
    """
    def __init__(self, name: str, sim: EventCalendar, rng: random.Random,
                 batch_size: int = 2,
                 output_entity_name: str = "Batch"):
        super().__init__(name, sim, rng)
        self.batch_size = batch_size
        self.output_entity_name = output_entity_name
        self._input_queues: List[SovereignQueue] = []
        self._direct_buffer: List[SimEntity] = []

    def add_input_queue(self, q: SovereignQueue):
        self._input_queues.append(q)
        q.register_observer(self._try_combine)

    def _on_receive(self, entity: SimEntity):
        self._direct_buffer.append(entity)
        self._try_combine()

    def _try_combine(self):
        total = len(self._direct_buffer) + sum(q.length() for q in self._input_queues)
        if total < self.batch_size:
            return

        consumed: List[SimEntity] = []
        while len(consumed) < self.batch_size and self._direct_buffer:
            consumed.append(self._direct_buffer.pop(0))
        for q in self._input_queues:
            while len(consumed) < self.batch_size and q.length() > 0:
                consumed.append(q.pop())

        if len(consumed) >= self.batch_size:
            # SURVIVOR: entity[0] transforms, rest are consumed
            survivor = consumed[0]
            survivor.transform(
                f"{self.output_entity_name}_{int(self.sim.current_time)}",
                {"combined_from": len(consumed),
                 "assembly_time": self.sim.current_time}
            )
            self.stats.entities_in += len(consumed) - 1  # account for consumed
            self.send_to_next(survivor)


# =============================================================
# SECTION 2: ANALYTICS — MONTE CARLO + MCKINSEY LAYER
# =============================================================

def student_t_ci(values: List[float]) -> tuple:
    """
    95% Confidence Interval via Student's T-Distribution.
    Hardcoded t-critical table (two-tailed, α=0.05).
    For 5 runs: df=4, t_crit=2.776
    """
    n = len(values)
    if n == 0:
        return 0.0, 0.0, 0.0
    if n == 1:
        return values[0], values[0], values[0]

    T_TABLE = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776,
               5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306,
               9: 2.262, 10: 2.228}
    df = n - 1
    t_crit = T_TABLE.get(df, 2.0)

    mean = sum(values) / n
    variance = sum((x - mean) ** 2 for x in values) / df
    margin = t_crit * math.sqrt(variance / n)
    return mean, mean - margin, mean + margin


# =============================================================
# SECTION 3: UNIT CONVERSION
# =============================================================

def to_seconds(value: float, unit: str) -> float:
    """Universal UnitConverter. Every time input passes through here."""
    return value * {
        "Seconds": 1,    "secs": 1,    "s": 1,
        "Minutes": 60,   "mins": 60,   "min": 60,
        "Hours":   3600, "hours": 3600,"hrs": 3600,
        "Day":   86400,  "Days":  86400,
        "Week":  604800, "Weeks": 604800,
        "Month": 2592000,"Months":2592000,
    }.get(unit, 1)


def get_distribution_fn(rng: random.Random, mean_s: float, variability: str) -> Callable:
    """
    Floor-Speak variability → Statistical distribution mapping.
    No jargon exposed to user.

    "Very Steady"   → Normal (CV=0.05) — near-deterministic
    "Mostly Steady" → Normal (CV=0.25) — typical factory
    "Variable"      → Exponential       — high variability
    "Unpredictable" → Weibull (k=0.75)  — extreme variance
    """
    VAR_MAP = {
        "Very Steady (almost always the same)":
            lambda: max(0.001, rng.gauss(mean_s, mean_s * 0.05)),
        "Mostly Steady (small variations)":
            lambda: max(0.001, rng.gauss(mean_s, mean_s * 0.25)),
        "Variable (noticeable swings)":
            lambda: rng.expovariate(1.0 / mean_s),
        "Unpredictable (wild swings)":
            lambda: max(0.001, rng.weibullvariate(mean_s * 0.85, 0.75)),
    }
    return VAR_MAP.get(variability, lambda: mean_s)


# =============================================================
# SECTION 4: SIMULATION BUILDER
# =============================================================

def build_and_run(config: dict, seed: int, sim_duration_s: float) -> dict:
    """
    Builds complete factory from UI config dict.
    Runs simulation for sim_duration_s seconds.
    Returns per-block statistics.
    """
    SimEntity.reset_counter()
    rng = random.Random(seed)
    sim = EventCalendar()
    blocks_cfg = config["blocks"]

    # ── Pass 1: Instantiate resource pools first ──────────────
    resource_pools: Dict[str, ResourcePool] = {}
    for bc in blocks_cfg:
        if bc["type"] == "Resource Pool":
            cap = int(bc.get("params", {}).get("capacity", 1))
            resource_pools[bc["name"]] = ResourcePool(bc["name"], cap, sim)

    # ── Pass 2: Instantiate all process blocks ────────────────
    block_objs: Dict[str, SovereignBlock] = {}
    queues: Dict[str, SovereignQueue] = {}
    servers: Dict[str, SovereignServer] = {}
    generators: Dict[str, SovereignGenerator] = {}

    for bc in blocks_cfg:
        nm = bc["name"]
        bt = bc["type"]
        p = bc.get("params", {})

        if bt == "Generator":
            if p.get("use_truck_recipe", False):
                truck_kg = float(p.get("truck_delivery_kg", 1000))
                recipe_kg = float(p.get("recipe_kg_per_unit", 1.0))
                freq_s = to_seconds(float(p.get("truck_frequency", 1)), p.get("truck_frequency_unit", "Hours"))
                units_per_truck = max(truck_kg / recipe_kg, 0.001)
                iat_s = freq_s / units_per_truck
            else:
                iat_s = to_seconds(float(p.get("inter_arrival_time", 5)), p.get("inter_arrival_unit", "Minutes"))

            variability = p.get("variability", "Very Steady (almost always the same)")
            iat_fn = get_distribution_fn(rng, iat_s, variability)
            gen = SovereignGenerator(
                nm, sim, rng,
                iat_fn=iat_fn,
                entities_per_arrival=int(p.get("entities_per_arrival", 1)),
                max_entities=int(p.get("max_entities", 0)),
                entity_name=p.get("entity_name", "Part"),
            )
            block_objs[nm] = gen
            generators[nm] = gen

        elif bt == "Queue":
            q = SovereignQueue(
                nm, sim, rng,
                max_capacity=int(p.get("max_capacity", 0)),
                discipline=p.get("discipline", "FIFO"),
            )
            block_objs[nm] = q
            queues[nm] = q

        elif bt == "Server":
            st_s = to_seconds(float(p.get("service_time", 5)), p.get("service_time_unit", "Minutes"))
            variability = p.get("variability", "Very Steady (almost always the same)")
            svc_fn = get_distribution_fn(rng, st_s, variability)
            pool = resource_pools.get(p.get("resource_pool")) if p.get("resource_pool") else None
            server = SovereignServer(
                nm, sim, rng,
                service_time_fn=svc_fn,
                num_servers=int(p.get("num_servers", 1)),
                resource_pool=pool,
            )
            block_objs[nm] = server
            servers[nm] = server

        elif bt == "Combine":
            comb = SovereignCombine(
                nm, sim, rng,
                batch_size=int(p.get("batch_size", 2)),
                output_entity_name=p.get("output_entity_name", "Batch"),
            )
            block_objs[nm] = comb

        # Branch deferred to Pass 3 (needs all blocks instantiated first)

    # ── Pass 3: Instantiate Branches (needs all blocks in objs) ─
    for bc in blocks_cfg:
        if bc["type"] != "Branch":
            continue
        nm = bc["name"]
        routes_cfg = bc.get("params", {}).get("routes", [])
        routes = [
            (float(r.get("probability", 0.5)), block_objs.get(r.get("next_block")))
            for r in routes_cfg
        ]
        # Keep None-destination routes (Exit) — SovereignBranch._on_receive handles them
        if routes:
            block_objs[nm] = SovereignBranch(nm, sim, rng, routes)

    # ── Pass 4: Wire routing and queue-server connections ─────
    for bc in blocks_cfg:
        nm = bc["name"]
        bt = bc["type"]
        if bt in ("Branch", "Resource Pool"):
            continue
        block = block_objs.get(nm)
        if not block:
            continue

        # Set next_block for all non-branch blocks
        next_nm = bc.get("next_block", "Exit")
        next_obj = block_objs.get(next_nm)
        if next_obj:
            block.set_next(next_obj)

        # Auto-wire Queue → Server observer registration
        if bt == "Queue" and next_nm in servers:
            servers[next_nm].set_input_queue(queues[nm])

    # Wire Combine input queues
    for bc in blocks_cfg:
        if bc["type"] != "Combine":
            continue
        comb = block_objs.get(bc["name"])
        if not comb:
            continue
        for qname in bc.get("params", {}).get("input_queues", []):
            q = queues.get(qname)
            if q:
                comb.add_input_queue(q)

    # ── Pass 5: Start all generators ─────────────────────────
    for gen in generators.values():
        gen.start()

    # ── Run simulation ────────────────────────────────────────
    sim.run(until=sim_duration_s)

    # ── Finalize statistics ───────────────────────────────────
    results: Dict[str, dict] = {}
    for nm, blk in block_objs.items():
        blk.stats.queue_length.finalize(sim_duration_s)
        blk.stats.utilization.finalize(sim_duration_s)
        bt = next((b["type"] for b in blocks_cfg if b["name"] == nm), "")
        results[nm] = {
            "type": bt,
            "entities_in": blk.stats.entities_in,
            "entities_out": blk.stats.entities_out,
            "avg_queue_length": blk.stats.queue_length.mean(),
            "utilization": blk.stats.utilization.mean(),
            "avg_wait_s": blk.stats.wait_time.mean(),
            "avg_service_s": blk.stats.service_time.mean(),
        }

    for nm, pool in resource_pools.items():
        pool.stats.utilization.finalize(sim_duration_s)
        results[nm] = {
            "type": "Resource Pool",
            "entities_in": 0,
            "entities_out": 0,
            "avg_queue_length": 0,
            "utilization": pool.stats.utilization.mean(),
            "avg_wait_s": 0,
            "avg_service_s": 0,
        }

    # Total throughput = generated minus scrapped
    # Scrap sinks are named __scrap_*; everything else that exited is finished product.
    gen_out    = sum(g.stats.entities_out for g in generators.values())
    scrap_out  = sum(blk.stats.entities_in
                     for nm, blk in block_objs.items() if nm.startswith("__scrap_"))
    total_out  = max(0, gen_out - scrap_out)
    results["_meta"] = {
        "total_throughput": total_out,
        "sim_duration_s": sim_duration_s,
    }
    return results


def run_monte_carlo(config: dict, sim_duration_s: float, n_runs: int = 5) -> dict:
    """
    5-seed Monte Carlo run with Student-T 95% CI.
    Seeds are well-separated to minimize correlation.
    """
    all_runs = [
        build_and_run(config, 42 + i * 137, sim_duration_s)
        for i in range(n_runs)
    ]

    block_names = [k for k in all_runs[0] if k != "_meta"]
    aggregated: Dict[str, dict] = {}

    for nm in block_names:
        bt = all_runs[0].get(nm, {}).get("type", "")
        agg: Dict[str, dict] = {}
        for metric in ["utilization", "avg_queue_length", "avg_wait_s", "entities_out", "entities_in"]:
            vals = [r[nm].get(metric, 0) for r in all_runs if nm in r]
            if vals:
                mean, lo, hi = student_t_ci(vals)
                agg[metric] = {"mean": mean, "ci_low": lo, "ci_high": hi, "runs": vals}
        aggregated[nm] = {"type": bt, "metrics": agg}

    tps = [r["_meta"]["total_throughput"] for r in all_runs]
    mean_tp, lo_tp, hi_tp = student_t_ci(tps)
    aggregated["_summary"] = {
        "tp_mean": mean_tp, "tp_lo": lo_tp, "tp_hi": hi_tp, "tp_runs": tps,
    }
    return aggregated


def compute_stability(config: dict) -> Dict[str, dict]:
    """
    Stability Gate: ρ = λ / (C × μ)
    Must check ρ < 1 for each server before allowing run.
    """
    lambda_total = 0.0
    for bc in config["blocks"]:
        if bc["type"] != "Generator":
            continue
        p = bc.get("params", {})
        if p.get("use_truck_recipe", False):
            freq_s = to_seconds(float(p.get("truck_frequency", 1)), p.get("truck_frequency_unit", "Hours"))
            units = max(float(p.get("truck_delivery_kg", 1000)) / max(float(p.get("recipe_kg_per_unit", 1)), 0.001), 0.001)
            iat_s = freq_s / units
        else:
            iat_s = to_seconds(float(p.get("inter_arrival_time", 5)), p.get("inter_arrival_unit", "Minutes"))
        epa = int(p.get("entities_per_arrival", 1))
        if iat_s > 0:
            lambda_total += epa / iat_s

    results = {}
    for bc in config["blocks"]:
        if bc["type"] != "Server":
            continue
        p = bc.get("params", {})
        st_s = to_seconds(float(p.get("service_time", 5)), p.get("service_time_unit", "Minutes"))
        c = int(p.get("num_servers", 1))
        mu = 1.0 / st_s if st_s > 0 else float("inf")
        rho = lambda_total / (c * mu) if (c * mu) > 0 else float("inf")
        results[bc["name"]] = {
            "rho": rho, "lambda": lambda_total,
            "mu": mu, "c": c, "stable": rho < 1.0,
        }
    return results


def compute_lost_capital(mc: dict, config: dict,
                         wip_rate: float, idle_rate: float) -> dict:
    """
    McKinsey Lost Capital Report.
    WIP cost  = avg_queue_length × avg_wait_hours × wip_rate
    Idle cost = (1 − utilization) × sim_hours × idle_rate × num_servers
    Bottleneck = server with highest utilization.
    """
    sim_h = config.get("sim_duration_s", 28800) / 3600.0
    report: Dict[str, dict] = {}
    bottleneck = None
    max_util = -1.0

    for nm, data in mc.items():
        if nm.startswith("_") or data.get("type") != "Server":
            continue
        m = data["metrics"]
        util = m.get("utilization", {}).get("mean", 0)
        wq_s = m.get("avg_wait_s", {}).get("mean", 0)
        ql = m.get("avg_queue_length", {}).get("mean", 0)

        # Look up num_servers from config
        c = next((int(b.get("params", {}).get("num_servers", 1))
                  for b in config["blocks"] if b["name"] == nm), 1)

        wip_cost = ql * (wq_s / 3600) * wip_rate
        idle_cost = (1 - util) * sim_h * idle_rate * c
        total = wip_cost + idle_cost

        report[nm] = {
            "utilization": util,
            "wip_cost": wip_cost,
            "idle_cost": idle_cost,
            "total_lost": total,
        }
        if util > max_util:
            max_util = util
            bottleneck = nm

    report["_bottleneck"] = bottleneck
    report["_max_util"] = max_util
    return report


# =============================================================
# SECTION 5: GRAPHVIZ FACTORY DIAGRAM
# =============================================================

def build_dot(blocks_cfg: list) -> str:
    """
    AnyLogic-style factory flowchart in DOT language.
    Shapes: Generator=ellipse, Queue=rectangle, Server=rectangle,
            Branch=diamond, Combine=trapezium, Resource=parallelogram
    """
    SHAPES = {
        "Generator": ("ellipse",     "#FFE600", "#1A1A1A"),
        "Queue":      ("rectangle",  "#FFFFFF", "#1A1A1A"),
        "Server":     ("rectangle",  "#FFE600", "#1A1A1A"),
        "Branch":     ("diamond",    "#FFD700", "#1A1A1A"),
        "Combine":    ("trapezium",  "#E8E8E8", "#1A1A1A"),
        "Resource Pool": ("parallelogram", "#C8F0C8", "#1A1A1A"),
    }

    lines = [
        "digraph Factory {",
        "    rankdir=LR;",
        "    bgcolor=\"white\";",
        "    node [fontname=\"Helvetica-Bold\", fontsize=10, penwidth=2.0];",
        "    edge [penwidth=1.8, color=\"#333333\", fontname=\"Helvetica\", fontsize=9];",
    ]

    for bc in blocks_cfg:
        nm, bt = bc["name"], bc["type"]
        shape, fill, fc = SHAPES.get(bt, ("box", "#EEEEEE", "#000000"))
        label = f"{nm}\\n[{bt}]"
        lines.append(f'    "{nm}" [shape={shape}, style="filled,bold", '
                     f'fillcolor="{fill}", fontcolor="{fc}", label="{label}"];')

    # EXIT node
    needs_exit = any(
        bc.get("next_block") == "Exit" or
        any(r.get("next_block") == "Exit"
            for r in bc.get("params", {}).get("routes", []))
        for bc in blocks_cfg
    )
    if needs_exit:
        lines.append('    "EXIT" [shape=doublecircle, style="filled,bold", '
                     'fillcolor="#CC3333", fontcolor="white", label="EXIT"];')

    # Edges
    for bc in blocks_cfg:
        nm, bt = bc["name"], bc["type"]
        if bt == "Branch":
            for r in bc.get("params", {}).get("routes", []):
                dest = r.get("next_block", "Exit")
                prob = float(r.get("probability", 0))
                lines.append(f'    "{nm}" -> "{dest}" [label="{prob*100:.0f}%", '
                             'style=dashed, color="#888888"];')
        elif bt == "Combine":
            for qnm in bc.get("params", {}).get("input_queues", []):
                lines.append(f'    "{qnm}" -> "{nm}" [style=dotted, color="#999999", '
                             'label="batch input"];')
            nxt = bc.get("next_block", "Exit")
            lines.append(f'    "{nm}" -> "{nxt}";')
        elif bt != "Resource Pool":
            nxt = bc.get("next_block", "Exit")
            lines.append(f'    "{nm}" -> "{nxt}";')

    # Resource pool dashed connections to servers that use them
    for bc in blocks_cfg:
        if bc["type"] == "Server":
            pool_nm = bc.get("params", {}).get("resource_pool")
            if pool_nm:
                lines.append(f'    "{pool_nm}" -> "{bc["name"]}" '
                             '[style=dotted, color="#4CAF50", label="uses", '
                             'constraint=false];')

    lines.append("}")
    return "\n".join(lines)


# =============================================================
# SECTION 5 : GRAPHVIZ DIAGRAM (WIZARD)
# =============================================================

def build_dot_wizard(blocks: list, paths: list, mode: str) -> str:
    SHAPES = {
        "Server":     ("circle",  "#FFE600", "#000000"),
        "Queue":      ("invtriangle",  "#FFFFFF",  "#000000"),
        "Delay":      ("delay",  "#D0E8FF",  "#000000"),
        "Inspection": ("square",    "#FFD7D7",  "#000000"),
        "Branch":     ("diamond",    "#FFD700",  "#000000"),
        "Generator":  ("ellipse",    "#FFE600",  "#000000"),
    }
    lines = [
        'digraph Factory {',
        '  rankdir=LR; bgcolor="white";',
        '  node [fontname="Helvetica-Bold", fontsize=9, penwidth=2.0];',
        '  edge [penwidth=1.5, color="#000000", fontname="Helvetica", fontsize=8];',
        '  "Source" [shape=ellipse, style="filled,bold", fillcolor="#0078d4",'
        '            fontcolor="white", label="Source"];',
    ]
    if mode == "Split and Transportation" and len(paths) > 1:
        lines.append('  "SplitBranch" [shape=diamond, style="filled,bold",'
                     ' fillcolor="#FFD700", fontcolor="#000000", label="Split"];')
        lines.append('  "Source" -> "SplitBranch";')
        for pth in paths:
            safe = pth.replace(" ", "_")
            lines.append(f'  "SplitBranch" -> "HEAD_{safe}" [label="{pth}", style=dashed];')
            lines.append(f'  "HEAD_{safe}" [shape=point, width=0.05];')
    else:
        path_blks = [b for b in blocks if b.get("path") == paths[0]] if blocks else []
        if path_blks:
            lines.append(f'  "Source" -> "{path_blks[0]["name"]}";')

    prev: dict = {}
    for blk in blocks:
        nm  = blk["name"]
        bt  = blk["type"]
        pth = blk.get("path", paths[0] if paths else "Main Path")
        shape, fill, fc = SHAPES.get(bt, ("box", "#EEEEEE", "#000000"))
        extra = ""
        if bt == "Server":   extra = f"\\nx{blk.get('cap',1)}"
        if bt == "Inspection": extra = f"\\nFail:{blk.get('fail',0):.0f}%"
        label = f"{nm}\\n[{bt}]{extra}"
        lines.append(f'  "{nm}" [shape={shape}, style="filled,bold",'
                     f' fillcolor="{fill}", fontcolor="{fc}", label="{label}"];')
        safe = pth.replace(" ", "_")
        if mode == "Split and Transportation" and len(paths) > 1:
            if pth not in prev:
                lines.append(f'  "HEAD_{safe}" -> "{nm}";')
            else:
                lines.append(f'  "{prev[pth]}" -> "{nm}";')
        else:
            if pth in prev:
                lines.append(f'  "{prev[pth]}" -> "{nm}";')
        prev[pth] = nm
        if bt == "Inspection":
            lines.append(f'  "{nm}" -> "SCRAP" [label="Fail", style=dashed, color="#CC3333"];')

    lines.append('  "SCRAP" [shape=box, style="filled,bold", fillcolor="#CC3333",'
                 ' fontcolor="white", label="SCRAP"];')
    lines.append('  "EXIT" [shape=doublecircle, style="filled,bold", fillcolor="#1e8e3e",'
                 ' fontcolor="white", label="EXIT"];')
    for pth, last_nm in prev.items():
        bt_last = next((b["type"] for b in blocks if b["name"] == last_nm), "")
        if bt_last != "Inspection":
            lines.append(f'  "{last_nm}" -> "EXIT";')
    lines.append("}")
    return "\n".join(lines)


# =============================================================
# SECTION 6 : CONFIG BUILDER  wizard → engine dict
# =============================================================

def build_engine_config(ss) -> dict:
    engine_blocks = []
    iat_s = float(ss.get("iat_s", 60.0))

    engine_blocks.append({
        "name": "Source_Generator", "type": "Generator",
        "params": {
            "iat_s": iat_s,
            "variability": ss.get("variability", "Very Steady (almost always the same)"),
            "entity_name": ss.get("unit", "Part"),
            "entities_per_arrival": 1,
            "max_entities": 0,
        },
        "next_block": "Storage_Queue",
    })
    engine_blocks.append({
        "name": "Storage_Queue", "type": "Queue",
        "params": {"max_capacity": int(ss.get("storage_cap", 0)), "discipline": "FIFO"},
        "next_block": "",
    })

    paths      = ss.get("paths", ["Main Path"])
    mode       = ss.get("logistics_mode", "Direct Transportation")
    wiz_blocks = ss.get("blocks", [])

    if mode == "Direct Transportation" or len(paths) == 1:
        transport_s = to_seconds(float(ss.get("transport_time", 0)),
                                 ss.get("transport_unit", "Minutes"))
        pb = [b for b in wiz_blocks if b.get("path") == paths[0]]
        if transport_s > 0:
            engine_blocks[-1]["next_block"] = "Transport_Delay"
            engine_blocks.append({
                "name": "Transport_Delay", "type": "Server",
                "params": {"service_time_s": transport_s, "num_servers": 999,
                           "variability": "Very Steady (almost always the same)"},
                "next_block": pb[0]["name"] if pb else "Exit",
            })
        else:
            engine_blocks[-1]["next_block"] = pb[0]["name"] if pb else "Exit"
        _append_path_blocks(engine_blocks, pb)
    else:
        prob_each = round(1.0 / len(paths), 4)
        split_s = to_seconds(float(ss.get("split_time", 0)),
                             ss.get("split_time_unit", "Seconds"))
        if split_s > 0:
            engine_blocks[-1]["next_block"] = "Split_Delay"
            engine_blocks.append({
                "name": "Split_Delay", "type": "Server",
                "params": {"service_time_s": split_s, "num_servers": 999,
                           "variability": "Very Steady (almost always the same)"},
                "next_block": "Split_Branch",
            })
        else:
            engine_blocks[-1]["next_block"] = "Split_Branch"
        routes = []
        for pth in paths:
            pb2 = [b for b in wiz_blocks if b.get("path") == pth]
            routes.append({"probability": prob_each,
                           "next_block": pb2[0]["name"] if pb2 else "Exit"})
        engine_blocks.append({
            "name": "Split_Branch", "type": "Branch",
            "params": {"routes": routes}, "next_block": "Exit",
        })
        for pth in paths:
            _append_path_blocks(engine_blocks,
                                [b for b in wiz_blocks if b.get("path") == pth])

    return {"blocks": engine_blocks,
            "sim_duration_s": float(ss.get("sim_duration_s", 28800))}


def _append_path_blocks(engine_blocks: list, path_blocks: list):
    for i, blk in enumerate(path_blocks):
        bt  = blk["type"]
        nm  = blk["name"]
        nxt = path_blocks[i+1]["name"] if i+1 < len(path_blocks) else "Exit"
        if bt == "Server":
            engine_blocks.append({
                "name": nm, "type": "Server",
                "params": {
                    "service_time_s": float(blk.get("time", 60)),
                    "num_servers":    int(blk.get("cap", 1)),
                    "variability":    blk.get("variability",
                                              "Very Steady (almost always the same)"),
                },
                "next_block": nxt,
            })
        elif bt == "Queue":
            engine_blocks.append({
                "name": nm, "type": "Queue",
                "params": {"max_capacity": int(blk.get("cap", 0)), "discipline": "FIFO"},
                "next_block": nxt,
            })
        elif bt == "Delay":
            engine_blocks.append({
                "name": nm, "type": "Server",
                "params": {
                    "service_time_s": float(blk.get("time", 60)),
                    "num_servers": 999,
                    "variability": "Very Steady (almost always the same)",
                },
                "next_block": nxt,
            })
        elif bt == "Inspection":
            fail_rate = float(blk.get("fail", 10)) / 100.0
            pass_rate = 1.0 - fail_rate
            branch_nm = f"__branch_{nm}"
            # Scrap sink block so we can count failures
            scrap_nm  = f"__scrap_{nm}"
            engine_blocks.append({
                "name": nm, "type": "Server",
                "params": {
                    "service_time_s": float(blk.get("time", 60)),
                    "num_servers": 1,
                    "variability": "Very Steady (almost always the same)",
                },
                "next_block": branch_nm,
            })
            engine_blocks.append({
                "name": branch_nm, "type": "Branch",
                "params": {"routes": [
                    {"probability": pass_rate,  "next_block": nxt},
                    {"probability": fail_rate,  "next_block": scrap_nm},
                ]},
                "next_block": "Exit",
            })
            # Scrap sink — entities_out of this block = scrapped units
            engine_blocks.append({
                "name": scrap_nm, "type": "Queue",
                "params": {"max_capacity": 0, "discipline": "FIFO"},
                "next_block": "Exit",
            })


# =============================================================
# SECTION 7 : CSS
# =============================================================

CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Google+Sans:wght@400;500;700&family=Roboto:wght@300;400;500&display=swap');

html, body, [class*="css"] {
    font-family: 'Roboto', sans-serif !important;
    background: #f8f9fa;
    color: #000000;
}
.main .block-container { padding-top: 0; max-width: 780px; margin: 0 auto; }
#MainMenu, footer, header { visibility: hidden; }

/* ── HEADER ── */
.ez-header {
    display: flex; align-items: center; gap: 14px;
    padding: 16px 0 10px 0;
    border-bottom: 2px solid #0078d4;
    margin-bottom: 6px;
}
.ez-logo {
    background: #0078d4; color: white;
    font-weight: 700; font-size: 1.05rem;
    padding: 6px 14px; border-radius: 4px;
    letter-spacing: 0.5px; white-space: nowrap;
}
.ez-title { font-size: 1.2rem; font-weight: 600; color: #000000; }

/* ── PROGRESS ── */
.ez-prog-wrap { margin: 8px 0 16px 0; }
.ez-prog-labels {
    display: flex; justify-content: space-between;
    font-size: 0.75rem; color: #000000; font-weight: 500;
    margin-bottom: 3px;
}
.ez-prog-track { background: #d0d0d0; border-radius: 4px; height: 6px; }
.ez-prog-fill  { background: #0078d4; border-radius: 4px; height: 6px; transition: width 0.3s; }

/* ── STEP HEADER ── */
.ez-step-badge {
    display: inline-block; background: #0078d4; color: white;
    font-size: 0.7rem; font-weight: 700; padding: 2px 10px;
    border-radius: 12px; letter-spacing: 0.5px; margin-bottom: 4px;
}
.ez-step-title {
    font-size: 1.45rem; font-weight: 600;
    color: #000000; margin-bottom: 12px;
}

/* ── CARDS ── */
.ez-card {
    background: #ffffff;
    border: 2px solid #000000;
    border-radius: 4px;
    padding: 16px 20px;
    margin-bottom: 12px;
}
.ez-card-title {
    font-size: 0.95rem; font-weight: 700;
    color: #000000; margin-bottom: 10px;
    padding-bottom: 6px; border-bottom: 2px solid #0078d4;
}

/* ── COMPUTED BADGE ── */
.ez-computed {
    background: #e8f0fe; border: 1.5px solid #0078d4;
    border-left: 4px solid #0078d4;
    border-radius: 4px; padding: 9px 14px;
    font-size: 0.87rem; color: #000000; font-weight: 500;
    margin: 8px 0;
}
.ez-computed strong { color: #0050a0; }

/* ── YIELD BADGE ── */
.yield-badge {
    display: inline-block;
    background: #1e8e3e; color: #ffffff;
    font-weight: 700; font-size: 1rem;
    padding: 6px 18px; border-radius: 20px;
    border: 2px solid #000000;
    margin: 6px 0;
}

/* ── BUTTONS ── */
.stButton > button {
    background-color: #0078d4 !important;
    color: #ffffff !important;
    border: 2px solid #000000 !important;
    border-radius: 4px !important;
    font-weight: 600 !important;
    font-size: 0.875rem !important;
    padding: 8px 22px !important;
    transition: background 0.15s !important;
}
.stButton > button:hover {
    background-color: #005a9e !important;
    color: #ffffff !important;
}
.stButton > button:disabled {
    background-color: #c8c8c8 !important;
    color: #666666 !important;
    border-color: #aaaaaa !important;
}

/* ── ALERTS ── */
.ez-warn {
    background: #fff8e1; border: 1.5px solid #f9ab00;
    border-left: 4px solid #f9ab00;
    border-radius: 4px; padding: 9px 14px;
    font-size: 0.85rem; color: #000000; margin: 6px 0;
}
.ez-err {
    background: #fce8e6; border: 1.5px solid #d93025;
    border-left: 4px solid #d93025;
    border-radius: 4px; padding: 9px 14px;
    font-size: 0.85rem; color: #000000; margin: 6px 0;
}
.ez-ok {
    background: #e6f4ea; border: 1.5px solid #1e8e3e;
    border-left: 4px solid #1e8e3e;
    border-radius: 4px; padding: 9px 14px;
    font-size: 0.85rem; color: #000000; margin: 6px 0;
}

/* ── STABILITY BADGES ── */
.badge-stable {
    background: #e6f4ea; color: #000000;
    font-size: 0.8rem; font-weight: 700;
    padding: 3px 10px; border-radius: 12px;
    border: 1.5px solid #1e8e3e;
    display: inline-block; margin: 3px;
}
.badge-unstable {
    background: #fce8e6; color: #000000;
    font-size: 0.8rem; font-weight: 700;
    padding: 3px 10px; border-radius: 12px;
    border: 1.5px solid #d93025;
    display: inline-block; margin: 3px;
}

/* ── BOTTLENECK ── */
.bottleneck-card {
    background: #000000; color: #FFE600;
    border-radius: 4px; padding: 12px 18px;
    font-size: 1rem; font-weight: 700;
    border-left: 6px solid #d93025;
    margin: 10px 0;
}
.annualized-card {
    background: #0078d4; color: #ffffff;
    border-radius: 4px; padding: 12px 18px;
    font-weight: 700; font-size: 0.95rem;
    margin: 8px 0; border: 2px solid #000000;
}
.leakage-card {
    background: #d93025; color: #ffffff;
    border-radius: 4px; padding: 14px 18px;
    font-weight: 700; font-size: 1.1rem;
    margin: 12px 0; border: 2px solid #000000;
    text-align: center;
}

/* ── PL TABLE ── */
.pl-table { width: 100%; border-collapse: collapse; margin: 10px 0; }
.pl-table th {
    background: #000000; color: #FFE600;
    font-size: 0.78rem; font-weight: 700;
    padding: 8px 12px; text-align: left;
    text-transform: uppercase; letter-spacing: 0.5px;
    border: 1px solid #000000;
}
.pl-table td {
    padding: 8px 12px; font-size: 0.85rem;
    color: #000000; border: 1px solid #444444;
    background: #ffffff;
}
.pl-table tr:nth-child(even) td { background: #f5f5f5; }
.pl-table tr:hover td { background: #e8f0fe; }
.pl-table .total-row td {
    background: #e8f0fe; font-weight: 700;
    border-top: 2px solid #0078d4;
}

/* ── INPUTS ── */
.stSelectbox > div > div,
.stNumberInput > div > div > input,
.stTextInput > div > div > input {
    border-radius: 4px !important;
    border: 1.5px solid #000000 !important;
    font-family: 'Roboto', sans-serif !important;
    color: #000000 !important;
}
label, .stSelectbox label, .stNumberInput label,
.stTextInput label { font-size: 0.85rem !important; color: #000000 !important; font-weight: 600 !important; }
.stRadio label { color: #000000 !important; font-weight: 500 !important; }
hr { border: none !important; border-top: 1.5px solid #cccccc !important; margin: 12px 0 !important; }

/* ── SIDEBAR ── */
section[data-testid="stSidebar"] {
    background: #ffffff; border-right: 2px solid #000000;
}
.sidebar-hd {
    font-weight: 700; font-size: 0.95rem;
    color: #000000; margin-bottom: 8px;
    border-bottom: 2px solid #0078d4; padding-bottom: 4px;
}
.path-badge {
    background: #e8f0fe; color: #000000;
    font-size: 0.75rem; font-weight: 600;
    padding: 2px 10px; border-radius: 12px;
    border: 1px solid #0078d4; display: inline-block;
    margin-bottom: 8px;
}

/* ── BLOCK LEDGER ── */
.blk-row {
    display: flex; align-items: center; gap: 8px;
    background: #ffffff; border: 1.5px solid #000000;
    border-radius: 4px; padding: 6px 10px; margin-bottom: 5px;
}
.blk-chip {
    font-size: 0.7rem; font-weight: 700;
    padding: 2px 8px; border-radius: 10px;
    color: #ffffff; white-space: nowrap; border: 1px solid #000000;
}
.chip-server     { background: #1e8e3e; }
.chip-queue      { background: #0078d4; }
.chip-delay      { background: #b45309; }
.chip-inspection { background: #d93025; }

/* ── NAV FOOTER ── */
.nav-wrap { border-top: 2px solid #000000; padding-top: 12px; margin-top: 10px; }

/* ── METRICS ── */
[data-testid="metric-container"] {
    background: #ffffff !important; border: 2px solid #000000 !important;
    border-radius: 4px !important;
}
[data-testid="stMetricValue"]  { color: #000000 !important; font-weight: 700 !important; }
[data-testid="stMetricLabel"]  { color: #000000 !important; font-weight: 600 !important; font-size: 0.78rem !important; }
[data-testid="stMetricDelta"]  { font-size: 0.78rem !important; }

/* ── DATAFRAME ── */
[data-testid="stDataFrame"] thead th {
    background: #000000 !important; color: #FFE600 !important;
    font-size: 0.78rem !important; text-transform: uppercase !important;
}
</style>
"""


# =============================================================
# SECTION 8 : SESSION STATE
# =============================================================

STEPS = {
    1: "Material",
    2: "Storage",
    3: "Sourcing",
    4: "Transportation",
    5: "Process Flow",
    6: "Run Config",
    7: "P&L Report",
}
TOTAL_STEPS = 7

VARIABILITY_OPTIONS = [
    "Very Steady (almost always the same)",
    "Mostly Steady (small variations)",
    "Variable (noticeable swings)",
    "Unpredictable (wild swings)",
]
UNIT_OPTIONS = ["Seconds", "Minutes", "Hours"]


def init_state():
    if "step" not in st.session_state:
        st.session_state.update({
            "step": 1,
            "current_path_idx": 0,
            # Step 1
            "unit": "Kg",
            # Step 2
            "storage_cap": 0.0,
            # Step 3
            "freq_val": 1.0,
            "freq_unit": "Day",
            "qty": 100.0,
            "iat_s": 864.0,
            "entity_name": "Part",
            "variability": "Very Steady (almost always the same)",
            "unit_sourcing_cost": 10.0,
            "unit_sale_price": 25.0,
            # Step 4
            "logistics_mode": "Direct Transportation",
            "transport_time": 0.0,
            "transport_unit": "Minutes",
            "n_splits": 2,
            "split_time": 0.0,
            "split_time_unit": "Seconds",
            "paths": ["Main Path"],
            # Step 5
            "blocks": [],
            # Step 6
            "shift_hours": 8.0,
            "sim_duration_s": 28800.0,
            "wip_rate": 50.0,
            "idle_rate": 100.0,
            "daily_electricity_cost": 200.0,
            "daily_repair_cost": 80.0,
            "ack_unstable": False,
            # Results
            "mc_results": None,
            "lost_capital": None,
            "run_complete": False,
        })


# =============================================================
# SECTION 9 : UI HELPERS
# =============================================================

def render_header():
    st.markdown(
        '<div class="ez-header">'
        '  <div class="ez-logo">EasySim</div>'
        '  <div class="ez-title">Digital Twin Builder</div>'
        '</div>',
        unsafe_allow_html=True,
    )


def render_progress():
    step = st.session_state.step
    pct  = int((step - 1) / (TOTAL_STEPS - 1) * 100)
    label = STEPS.get(step, "")
    st.markdown(
        f'<div class="ez-prog-wrap">'
        f'  <div class="ez-prog-labels"><span>{label}</span><span>Step {step} / {TOTAL_STEPS}</span></div>'
        f'  <div class="ez-prog-track"><div class="ez-prog-fill" style="width:{pct}%"></div></div>'
        f'</div>',
        unsafe_allow_html=True,
    )


def step_header(title: str):
    step = st.session_state.step
    st.markdown(f'<div class="ez-step-badge">STEP {step} / {TOTAL_STEPS}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="ez-step-title">{title}</div>', unsafe_allow_html=True)


def nav_footer(back_fn=None, next_fn=None, next_label="NEXT",
               next_disabled=False, show_back=True):
    st.markdown('<div class="nav-wrap">', unsafe_allow_html=True)
    cb, _, cn = st.columns([2, 5, 2])
    with cb:
        if show_back and back_fn:
            if st.button("BACK", key=f"back_{st.session_state.step}"):
                back_fn(); st.rerun()
    with cn:
        if next_fn:
            if st.button(next_label, key=f"next_{st.session_state.step}",
                         disabled=next_disabled):
                next_fn(); st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)


def render_blocks_ledger(path_filter: str):
    blks = [b for b in st.session_state.blocks if b.get("path") == path_filter]
    if not blks:
        return
    st.markdown("**Blocks on this path:**")
    for blk in blks:
        gi = st.session_state.blocks.index(blk)
        c1, c2, c3 = st.columns([4, 2, 1])
        c1.markdown(f"**{blk['name']}**")
        chip_cls = {"Server": "chip-server", "Queue": "chip-queue",
                    "Delay": "chip-delay", "Inspection": "chip-inspection"}.get(blk["type"], "chip-queue")
        c2.markdown(f'<span class="blk-chip {chip_cls}">{blk["type"]}</span>', unsafe_allow_html=True)
        if c3.button("X", key=f"del_{gi}"):
            st.session_state.blocks.pop(gi); st.rerun()


def render_sidebar():
    with st.sidebar:
        st.markdown('<div class="sidebar-hd">Factory Diagram</div>', unsafe_allow_html=True)
        if st.session_state.blocks:
            active = st.session_state.paths[
                min(st.session_state.current_path_idx, len(st.session_state.paths)-1)]
            st.markdown(f'<div class="path-badge">Path: {active}</div>', unsafe_allow_html=True)
        try:
            dot = build_dot_wizard(
                st.session_state.blocks,
                st.session_state.paths,
                st.session_state.logistics_mode,
            )
            st.graphviz_chart(dot, use_container_width=True)
        except Exception as e:
            st.caption(f"Diagram: {e}")


# =============================================================
# SECTION 10 : WIZARD STEPS
# =============================================================

# ── STEP 1 : Material Unit ────────────────────────────────────
def step1():
    step_header("Material")
    unit_opts = ["Kg", "Litre", "Piece", "Ton", "Other"]
    idx = unit_opts.index(st.session_state.unit) if st.session_state.unit in unit_opts else 0
    st.session_state.unit = st.selectbox("Unit of source material:", unit_opts, index=idx, key="s1_u")
    nav_footer(show_back=False,
               next_fn=lambda: st.session_state.update({"step": 2}),
               next_label="NEXT")


# ── STEP 2 : Storage ─────────────────────────────────────────
def step2():
    step_header("Storage")
    unit = st.session_state.unit
    st.session_state.storage_cap = st.number_input(
        f"Store room capacity ({unit}), 0 = unlimited:",
        min_value=0.0, value=float(st.session_state.storage_cap), step=50.0, key="s2_c")
    nav_footer(
        back_fn=lambda: st.session_state.update({"step": 1}),
        next_fn=lambda: st.session_state.update({"step": 3}),
    )


# ── STEP 3 : Sourcing ─────────────────────────────────────────
def step3():
    step_header("Sourcing")
    unit = st.session_state.unit

    st.markdown('<div class="ez-card"><div class="ez-card-title">Arrival Frequency</div>', unsafe_allow_html=True)
    c1, c2 = st.columns([2, 1])
    with c1:
        freq_val = st.number_input("How often?", min_value=1.0, step=1.0,
                                   value=float(st.session_state.freq_val), key="s3_fv")
    with c2:
        fu_opts = ["Hours","Day", "Week", "Month" ]
        fidx = fu_opts.index(st.session_state.freq_unit) if st.session_state.freq_unit in fu_opts else 0
        freq_unit = st.selectbox("Per:", fu_opts, index=fidx, key="s3_fu")
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="ez-card"><div class="ez-card-title">Quantity & Costs</div>', unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1:
        qty = st.number_input(f"Qty per delivery ({unit}):", min_value=0.001,
                              step=10.0, value=float(st.session_state.qty), key="s3_q")
        unit_sourcing_cost = st.number_input(
            f"Cost to buy 1 {unit} (RUPEES.):",
            min_value=0.0, value=float(st.session_state.unit_sourcing_cost),
            step=0.5, key="s3_usc")
    with c2:
        unit_sale_price = st.number_input(
            f"Sale price of 1 finished unit (RUPEES.):",
            min_value=0.0, value=float(st.session_state.unit_sale_price),
            step=0.5, key="s3_usp")
        variability = st.selectbox(
            "Arrival consistency:", VARIABILITY_OPTIONS,
            index=VARIABILITY_OPTIONS.index(st.session_state.variability)
            if st.session_state.variability in VARIABILITY_OPTIONS else 0,
            key="s3_var")
    entity_name = st.text_input("Material / part name:",
                                value=st.session_state.entity_name, key="s3_en")
    st.markdown('</div>', unsafe_allow_html=True)

    # IAT computation
    freq_s = to_seconds(freq_val, freq_unit)
    iat_s  = freq_s / max(qty, 0.001)
    st.markdown(
        f'<div class="ez-computed">'
        f'Inter-Arrival Time: <strong>{iat_s:.2f}s</strong> &nbsp;|&nbsp; '
        f'{iat_s/60:.2f} min &nbsp;|&nbsp; '
        f'Rate: {3600/iat_s:.1f} units/hr'
        f'</div>',
        unsafe_allow_html=True,
    )
    st.session_state.update({
        "freq_val": freq_val, "freq_unit": freq_unit,
        "qty": qty, "iat_s": iat_s,
        "entity_name": entity_name, "variability": variability,
        "unit_sourcing_cost": unit_sourcing_cost,
        "unit_sale_price": unit_sale_price,
    })
    nav_footer(
        back_fn=lambda: st.session_state.update({"step": 2}),
        next_fn=lambda: st.session_state.update({"step": 4}),
    )


# ── STEP 4 : Logistics ────────────────────────────────────────
def step4():
    step_header("Logistics")
    unit = st.session_state.unit

    mode = st.selectbox(
        "Transport mode:",
        ["Direct Transportation", "Split and Transportation"],
        index=0 if st.session_state.logistics_mode == "Direct Transportation" else 1,
        key="s4_mode")
    st.session_state.logistics_mode = mode

    if mode == "Direct Transportation":
        c1, c2 = st.columns([3, 1])
        with c1:
            st.session_state.transport_time = st.number_input(
                "Delivery time:", min_value=0.0,
                value=float(st.session_state.transport_time), step=1.0, key="s4_tt")
        with c2:
            tu_opts = ["Minutes", "Seconds", "Hours"]
            tidx = tu_opts.index(st.session_state.transport_unit) if st.session_state.transport_unit in tu_opts else 0
            st.session_state.transport_unit = st.selectbox("Unit:", tu_opts, index=tidx, key="s4_tu")
        st.session_state.paths = ["Main Path"]
    else:
        c1, c2 = st.columns(2)
        with c1:
            n_splits = st.number_input("Number of splits:", min_value=2, max_value=8,
                                       value=int(st.session_state.n_splits), step=1, key="s4_ns")
            st.session_state.n_splits = int(n_splits)
        with c2:
            c3, c4 = st.columns([3, 1])
            with c3:
                st.session_state.split_time = st.number_input(
                    f"Time to split (per {unit}):", min_value=0.0,
                    value=float(st.session_state.split_time), step=0.5, key="s4_st")
            with c4:
                su_opts = ["Seconds", "Minutes"]
                suidx = su_opts.index(st.session_state.split_time_unit) if st.session_state.split_time_unit in su_opts else 0
                st.session_state.split_time_unit = st.selectbox("Unit:", su_opts, index=suidx, key="s4_stu")
        st.session_state.paths = [f"Split Path {i+1}" for i in range(st.session_state.n_splits)]
        st.markdown(
            f'<div class="ez-computed">{st.session_state.n_splits} parallel paths will be created.</div>',
            unsafe_allow_html=True)

    nav_footer(
        back_fn=lambda: st.session_state.update({"step": 3}),
        next_fn=lambda: st.session_state.update({"step": 5, "current_path_idx": 0}),
    )


# ── STEP 5 : Process Flow ─────────────────────────────────────
def step5():
    paths  = st.session_state.paths
    pidx   = max(0, min(st.session_state.current_path_idx, len(paths)-1))
    st.session_state.current_path_idx = pidx
    active = paths[pidx]

    step_header(f"Process Flow — {active}")

    if len(paths) > 1:
        cols = st.columns(len(paths))
        for i, pth in enumerate(paths):
            style = "font-weight:700;color:#0078d4;text-decoration:underline;" if i == pidx else "color:#666;"
            cols[i].markdown(f'<span style="{style}">{pth}</span>', unsafe_allow_html=True)

    action = st.selectbox(
        "Add step:",
        ["Operation (Server)", "Queue (Waiting Line)",
         "Delay (Fixed Wait)", "Inspection (Quality Check)", "Exit"],
        key=f"s5_act_{pidx}")

    if action == "Operation (Server)":
        st.markdown('<div class="ez-card"><div class="ez-card-title">Operation</div>', unsafe_allow_html=True)
        c1, c2, c3 = st.columns([3, 1, 1])
        with c1:
            op_name = st.text_input("Name:", value=f"Op_{len(st.session_state.blocks)+1}", key=f"opn_{pidx}")
            op_var  = st.selectbox("Variability:", VARIABILITY_OPTIONS, key=f"opv_{pidx}")
        with c2:
            op_time = st.number_input("Time:", min_value=0.001, value=5.0, step=0.5, key=f"opt_{pidx}")
            op_unit = st.selectbox("Unit:", ["mins", "secs", "hours"], key=f"opu_{pidx}")
        with c3:
            op_mach = st.number_input("Machines:", min_value=1, value=1, step=1, key=f"opm_{pidx}")
        st.markdown('</div>', unsafe_allow_html=True)
        if st.button("Add Operation", key=f"add_op_{pidx}"):
            if not op_name.strip():
                st.error("Name required.")
            elif op_name in [b["name"] for b in st.session_state.blocks]:
                st.error(f"'{op_name}' exists.")
            else:
                st.session_state.blocks.append({
                    "name": op_name.strip(), "type": "Server",
                    "time": to_seconds(op_time, op_unit),
                    "cap": int(op_mach), "variability": op_var, "path": active,
                })
                st.rerun()

    elif action == "Queue (Waiting Line)":
        st.markdown('<div class="ez-card"><div class="ez-card-title">Queue</div>', unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1:
            q_name = st.text_input("Name:", value=f"Queue_{len(st.session_state.blocks)+1}", key=f"qn_{pidx}")
        with c2:
            q_cap = st.number_input("Max capacity (0=unlimited):", min_value=0, value=0, key=f"qc_{pidx}")
        st.markdown('</div>', unsafe_allow_html=True)
        if st.button("Add Queue", key=f"add_q_{pidx}"):
            if not q_name.strip():
                st.error("Name required.")
            else:
                st.session_state.blocks.append({
                    "name": q_name.strip(), "type": "Queue",
                    "cap": int(q_cap), "path": active,
                })
                st.rerun()

    elif action == "Delay (Fixed Wait)":
        st.markdown('<div class="ez-card"><div class="ez-card-title">Delay</div>', unsafe_allow_html=True)
        c1, c2, c3 = st.columns([3, 1, 1])
        with c1:
            d_name = st.text_input("Name:", value=f"Delay_{len(st.session_state.blocks)+1}", key=f"dn_{pidx}")
        with c2:
            d_time = st.number_input("Duration:", min_value=0.0, value=5.0, step=1.0, key=f"dt_{pidx}")
        with c3:
            d_unit = st.selectbox("Unit:", ["mins", "secs", "hours"], key=f"du_{pidx}")
        st.markdown('</div>', unsafe_allow_html=True)
        if st.button("Add Delay", key=f"add_d_{pidx}"):
            if not d_name.strip():
                st.error("Name required.")
            else:
                st.session_state.blocks.append({
                    "name": d_name.strip(), "type": "Delay",
                    "time": to_seconds(d_time, d_unit), "path": active,
                })
                st.rerun()

    elif action == "Inspection (Quality Check)":
        st.markdown('<div class="ez-card"><div class="ez-card-title">Inspection</div>', unsafe_allow_html=True)
        c1, c2, c3 = st.columns([3, 1, 1])
        with c1:
            i_name  = st.text_input("Name:", value=f"Inspect_{len(st.session_state.blocks)+1}", key=f"in_{pidx}")
            i_time  = st.number_input("Time per unit (mins):", min_value=0.001, value=2.0, key=f"it_{pidx}")
        with c2:
            fail_p = st.number_input("Fail % (0-100):", min_value=0, max_value=100, value=10, key=f"if_{pidx}")
        with c3:
            st.markdown(f'<div style="margin-top:24px;font-size:0.8rem;color:#000;font-weight:600;">'
                        f'Pass: {100-fail_p}%<br>Fail: {fail_p}%</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
        if st.button("Add Inspection", key=f"add_i_{pidx}"):
            if not i_name.strip():
                st.error("Name required.")
            else:
                st.session_state.blocks.append({
                    "name": i_name.strip(), "type": "Inspection",
                    "time": i_time * 60.0, "fail": float(fail_p), "path": active,
                })
                st.rerun()

    st.markdown("---")
    render_blocks_ledger(active)

    st.markdown('<div class="nav-wrap">', unsafe_allow_html=True)
    cb, _, cn = st.columns([2, 5, 2])
    with cb:
        if st.button("BACK", key=f"s5b_{pidx}"):
            if pidx > 0:
                st.session_state.current_path_idx = pidx - 1
            else:
                st.session_state.step = 4
            st.rerun()
    with cn:
        nl = "NEXT PATH" if pidx < len(paths)-1 else "NEXT"
        if st.button(nl, key=f"s5n_{pidx}"):
            if pidx < len(paths)-1:
                st.session_state.current_path_idx = pidx + 1
            else:
                st.session_state.step = 6
                st.session_state.current_path_idx = 0
            st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)


# ── STEP 6 : Run Configuration ────────────────────────────────
def step6():
    step_header("Run Configuration")

    # ── Shift configuration ───────────────────────────────────
    st.markdown('<div class="ez-card"><div class="ez-card-title">Shift Configuration</div>', unsafe_allow_html=True)
    shift_hours = st.number_input(
        "How many hours does the factory work in one shift?",
        min_value=1.0, max_value=24.0,
        value=float(st.session_state.shift_hours), step=0.5, key="s6_sh")
    sim_s = shift_hours * 3600.0
    st.session_state.update({"shift_hours": shift_hours, "sim_duration_s": sim_s})
    st.markdown(
        f'<div class="ez-computed">'
        f'Simulation duration: <strong>{sim_s:,.0f} seconds</strong> ({shift_hours:.1f} hours)</div>',
        unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    # ── WIP / idle financial rates ────────────────────────────
    st.markdown('<div class="ez-card"><div class="ez-card-title">Machine Cost Rates</div>', unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1:
        wip_rate = st.number_input(
            f"WIP holding cost (RUPEES. / hr / {st.session_state.unit}):",
            min_value=0.0, value=float(st.session_state.wip_rate), step=5.0, key="s6_wip")
    with c2:
        idle_rate = st.number_input(
            "Machine idle cost (RUPEES. / hr):",
            min_value=0.0, value=float(st.session_state.idle_rate), step=5.0, key="s6_idle")
    st.session_state.update({"wip_rate": wip_rate, "idle_rate": idle_rate})
    st.markdown('</div>', unsafe_allow_html=True)

    # ── Daily OPEX ────────────────────────────────────────────
    st.markdown('<div class="ez-card"><div class="ez-card-title">Daily Operating Expenses (OPEX)</div>', unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1:
        elec_cost = st.number_input(
            "Daily electricity cost (RUPEES.):",
            min_value=0.0, value=float(st.session_state.daily_electricity_cost),
            step=10.0, key="s6_elec")
    with c2:
        repair_cost = st.number_input(
            "Daily repair & maintenance cost (RUPEES.):",
            min_value=0.0, value=float(st.session_state.daily_repair_cost),
            step=5.0, key="s6_rep")
    st.session_state.update({
        "daily_electricity_cost": elec_cost,
        "daily_repair_cost": repair_cost,
    })
    total_opex = elec_cost + repair_cost
    st.markdown(
        f'<div class="ez-computed">Fixed daily burn: <strong>RUPEES.{total_opex:,.2f}</strong></div>',
        unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    # ── Stability gate ────────────────────────────────────────
    config  = build_engine_config(st.session_state)
    can_run = True

    if config["blocks"]:
        stability = compute_stability(config)
        if stability:
            st.markdown("**Stability Check (ρ = λ / C·μ):**")
            unstable = []

            for sname, sd in stability.items():
                rho = sd["rho"]

                if rho < 0.70:
                    msg = f'The machine "{sname}" is very fast. It will spend a lot of time sitting idle waiting for parts.'
                    cls = "badge-stable"
                elif 0.70 <= rho <= 0.90:
                    msg = f'The machine "{sname}" is in the Sweet Spot. It is working efficiently with enough breathing room.'
                    cls = "badge-stable"
                elif 0.90 < rho < 1.00:
                    msg = f'The machine "{sname}" is in the Critical Zone. It is at full capacity; any small delay will cause a bottleneck.'
                    cls = "badge-unstable"
                elif rho == 1.00:
                    msg = f'The machine "{sname}" is a Perfect Match, but it has zero time to catch up if a line forms.'
                    cls = "badge-unstable"
                else:
                    msg = f'The machine "{sname}" is OVERLOADED. Parts are arriving faster than it can process; the pile-up will never stop.'
                    cls = "badge-unstable"
                    unstable.append(sname)

                st.markdown(
                    f'<div class="{cls}" style="display:block; margin-bottom:10px; padding:10px;">{msg}</div>',
                    unsafe_allow_html=True,
                )

            if unstable and not st.session_state.ack_unstable:
                st.markdown(
                    f'<div class="ez-err"><strong>Warning:</strong> Production line will overflow '
                    f'at current rates — {", ".join(unstable)} ρ > 1. '
                    f'Results show transient behaviour only.</div>',
                    unsafe_allow_html=True)
                if st.button("I UNDERSTAND — RUN ANYWAY"):
                    st.session_state.ack_unstable = True
                    st.rerun()
                can_run = False

    # ── Run ───────────────────────────────────────────────────
    st.markdown("---")
    if can_run:
        if st.button("RUN DIGITAL TWIN (5 Monte Carlo Seeds)", key="run_btn"):
            with st.spinner("Running 5 simulations — computing CI and P&L report..."):
                try:
                    mc = run_monte_carlo(config, sim_s, n_runs=5)
                    lc = compute_lost_capital(mc, config, wip_rate, idle_rate)
                    st.session_state.update({
                        "mc_results": mc, "lost_capital": lc,
                        "run_complete": True, "step": 7,
                    })
                    st.rerun()
                except Exception as exc:
                    import traceback
                    st.error(f"Engine error: {exc}")
                    st.code(traceback.format_exc())

    def go_back():
        st.session_state.step = 5
        st.session_state.current_path_idx = len(st.session_state.paths) - 1

    nav_footer(back_fn=go_back, next_fn=None)


# ── STEP 7 : P&L REPORT ───────────────────────────────────────
def step7():
    step_header("P&L Report")

    mc = st.session_state.mc_results
    lc = st.session_state.lost_capital
    if not mc:
        st.info("No results. Go back to Step 6 and run the simulation.")
        nav_footer(back_fn=lambda: st.session_state.update({"step": 6}), next_fn=None)
        return

    summary  = mc.get("_summary", {})
    mean_tp  = summary.get("tp_mean", 0)
    lo_tp    = max(0.0, summary.get("tp_lo", 0))
    hi_tp    = summary.get("tp_hi", 0)

    # ── Scrap count from __scrap_ blocks ─────────────────────
    total_scrapped = 0.0
    total_sourced  = 0.0
    for nm, data in mc.items():
        if nm.startswith("_"):
            continue
        m = data.get("metrics", {})
        if nm.startswith("__scrap_"):
            total_scrapped += m.get("entities_in",  {}).get("mean", 0)
        if nm == "Source_Generator":
            total_sourced = m.get("entities_out", {}).get("mean", 0)

    if total_sourced <= 0:
        total_sourced = max(mean_tp + total_scrapped, 1.0)

    yield_pct        = max(0.0, (total_sourced - total_scrapped) / total_sourced * 100) if total_sourced > 0 else 100.0
    scrap_loss       = total_scrapped * st.session_state.unit_sourcing_cost
    opex_burn        = st.session_state.daily_electricity_cost + st.session_state.daily_repair_cost
    unit_sale_price  = st.session_state.unit_sale_price
    gross_revenue    = mean_tp * unit_sale_price

    # Aggregate WIP + idle from lc
    total_wip  = sum(d.get("wip_cost", 0)  for k, d in lc.items() if not k.startswith("_"))
    total_idle = sum(d.get("idle_cost", 0) for k, d in lc.items() if not k.startswith("_"))

    # ── Production headline ───────────────────────────────────
    st.markdown('<div class="ez-card"><div class="ez-card-title">Production Forecast</div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    c1.metric("Mean Output",    f"{mean_tp:.1f} units")
    c2.metric("95% CI Lower",   f"{lo_tp:.1f} units")
    c3.metric("95% CI Upper",   f"{hi_tp:.1f} units")

    yield_color = "#1e8e3e" if yield_pct >= 90 else ("#f9ab00" if yield_pct >= 75 else "#d93025")
    st.markdown(
        f'<div style="margin:8px 0;">'
        f'<span class="yield-badge" style="background:{yield_color};">'
        f'Floor Yield: {yield_pct:.1f}%'
        f'</span>'
        f'<span style="font-size:0.8rem;color:#555;margin-left:10px;">'
        f'{total_scrapped:.0f} units scrapped / {total_sourced:.0f} sourced'
        f'</span></div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div style="font-size:0.72rem;color:#666;border-top:1px solid #ddd;padding-top:4px;">'
        'Student T | 5 seeds | df=4 | t-crit=2.776</div>',
        unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    # ── Gross revenue ─────────────────────────────────────────
    st.markdown(
        f'<div class="ez-computed">'
        f'Gross Revenue (per shift): <strong>RUPEES.{gross_revenue:,.2f}</strong> '
        f'({mean_tp:.1f} units × RUPEES.{unit_sale_price:.2f})'
        f'</div>',
        unsafe_allow_html=True)

    # ── Bottleneck ────────────────────────────────────────────
    bottleneck = lc.get("_bottleneck")
    if bottleneck:
        bn_util = lc.get(bottleneck, {}).get("utilization", 0)
        st.markdown(
            f'<div class="bottleneck-card">'
            f'CRITICAL PRESSURE POINT (BOTTLENECK): {bottleneck} — Utilization {bn_util*100:.1f}%'
            f'</div>',
            unsafe_allow_html=True)

    # ── P&L table ─────────────────────────────────────────────
    total_leakage = total_wip + total_idle + scrap_loss + opex_burn

    rows = [
        ("WIP Holding Loss",        f"RUPEES.{total_wip:,.2f}",
         f"Capital tied up in {total_wip/max(total_leakage,0.001)*100:.0f}% of leakage"),
        ("Machine Idle Loss",        f"RUPEES.{total_idle:,.2f}",
         f"Wasted capacity — machines idle for {(1 - lc.get('_max_util', 0))*100:.0f}% of time"),
        ("Material Scrap Loss",      f"RUPEES.{scrap_loss:,.2f}",
         f"{total_scrapped:.0f} units failed × RUPEES.{st.session_state.unit_sourcing_cost:.2f}/unit"),
        ("Fixed Operational Burn",   f"RUPEES.{opex_burn:,.2f}",
         f"Electricity RUPEES.{st.session_state.daily_electricity_cost:.0f} + Repair RUPEES.{st.session_state.daily_repair_cost:.0f}"),
    ]

    rows_html = "".join(
        f"<tr><td>{r}</td><td style='font-weight:700;'>{c}</td><td style='color:#555;font-size:0.8rem;'>{n}</td></tr>"
        for r, c, n in rows
    )
    total_row = (
        f"<tr class='total-row'>"
        f"<td><strong>TOTAL DAILY LEAKAGE</strong></td>"
        f"<td><strong>RUPEES.{total_leakage:,.2f}</strong></td>"
        f"<td style='color:#0050a0;font-size:0.8rem;'>All hidden costs combined</td>"
        f"</tr>"
    )

    st.markdown(
        f"""<table class="pl-table">
          <thead>
            <tr><th>Loss Category</th><th>Daily Hidden Cost</th><th>Reason</th></tr>
          </thead>
          <tbody>{rows_html}{total_row}</tbody>
        </table>""",
        unsafe_allow_html=True,
    )

    # ── Bottom line ───────────────────────────────────────────
    st.markdown(
        f'<div class="leakage-card">'
        f'TOTAL DAILY LEAKAGE: RUPEES.{total_leakage:,.2f}'
        f'</div>',
        unsafe_allow_html=True)

    net_profit = gross_revenue - total_leakage
    net_color  = "#1e8e3e" if net_profit >= 0 else "#d93025"
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(
            f'<div class="annualized-card">'
            f'Annualized Leakage (250 days): RUPEES.{total_leakage * 250:,.0f}'
            f'</div>', unsafe_allow_html=True)
    with c2:
        st.markdown(
            f'<div style="background:{net_color};color:#fff;border:2px solid #000;'
            f'border-radius:4px;padding:12px 18px;font-weight:700;font-size:0.95rem;">'
            f'Est. Net Profit / Shift: RUPEES.{net_profit:,.2f}'
            f'</div>', unsafe_allow_html=True)

    # ── Profit margin insight ─────────────────────────────────
    if gross_revenue > 0:
        margin_pct = net_profit / gross_revenue * 100
        extra_units_needed = abs(total_leakage) / max(unit_sale_price, 0.001)
        st.markdown(
            f'<div class="ez-computed" style="margin-top:10px;">'
            f'Current gross margin: <strong>{margin_pct:.1f}%</strong> &nbsp;|&nbsp; '
            f'To recover all leakage you need <strong>{extra_units_needed:.0f} more units/shift</strong>'
            f'</div>',
            unsafe_allow_html=True)

    # ── Block performance ─────────────────────────────────────
    with st.expander("Block Performance Details"):
        rows_perf = []
        for nm, data in mc.items():
            if nm.startswith("_") or nm.startswith("__"):
                continue
            m = data.get("metrics", {})
            util = m.get("utilization",      {}).get("mean", 0)
            ul   = m.get("utilization",      {}).get("ci_low", 0)
            uh   = m.get("utilization",      {}).get("ci_high", 0)
            wq   = m.get("avg_wait_s",       {}).get("mean", 0)
            ql   = m.get("avg_queue_length", {}).get("mean", 0)
            tp   = m.get("entities_out",     {}).get("mean", 0)
            rows_perf.append({
                "Block":         nm,
                "Type":          data.get("type", ""),
                "Utilization":   f"{util*100:.1f}% [{ul*100:.0f}–{uh*100:.0f}%]",
                "Avg Queue":     f"{ql:.2f}",
                "Avg Wait(min)": f"{wq/60:.2f}",
                "Total Finished":f"{tp:.0f}",
            })
        if rows_perf:
            st.dataframe(pd.DataFrame(rows_perf), use_container_width=True, hide_index=True)

    st.markdown("---")
    c1, c2 = st.columns(2)
    if c1.button("RESTART", key="restart"):
        SimEntity.reset_counter()
        for k in list(st.session_state.keys()):
            del st.session_state[k]
        st.rerun()
    if c2.button("EDIT MODEL (Back to Step 5)", key="edit"):
        st.session_state.step = 5
        st.session_state.current_path_idx = 0
        st.rerun()


# =============================================================
# SECTION 11 : MAIN
# =============================================================

def main():
    st.set_page_config(
        page_title="EasySim v4.0 | Sovereign DES",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown(CSS, unsafe_allow_html=True)
    init_state()
    render_sidebar()

    _, col, _ = st.columns([1, 5, 1])
    with col:
        render_header()
        render_progress()
        step = st.session_state.step
        if   step == 1: step1()
        elif step == 2: step2()
        elif step == 3: step3()
        elif step == 4: step4()
        elif step == 5: step5()
        elif step == 6: step6()
        elif step == 7: step7()
        else:
            st.session_state.step = 1; st.rerun()


if __name__ == "__main__":
    main()
