"""Records whose state changes over days: patrols, incidents, notices, payments,
citizen reports, deforestation alerts, planting lots, and soil samples.

Each record is created on a local day and gets its whole future at creation
(status changes, deletion, payments), drawn from the stream of its creation
day. A change table's drop for date D lists the records with an event on D,
in their state at the end of D. Generating a day reads a bounded window of
creation days, never the whole history.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from open_data_platform.simulators.bioguardia.clock import DAY_SECONDS, local_midnight
from open_data_platform.simulators.bioguardia.fleet import Fleet, fleet_for
from open_data_platform.simulators.bioguardia.pii import (
    cnpj,
    company_name,
    cpf,
    full_name,
    pii_sentence,
)
from open_data_platform.simulators.bioguardia.rng import Rng, cumulative
from open_data_platform.simulators.bioguardia.world import FIRE_SEASON, World

HOUR = 3600

_CACHE: OrderedDict[tuple[Any, ...], Any] = OrderedDict()
_CACHE_LIMIT = 4000


def _cached(key: tuple[Any, ...], build: Callable[[], Any]) -> Any:
    value = _CACHE.get(key)
    if value is None:
        value = build()
        _CACHE[key] = value
        if len(_CACHE) > _CACHE_LIMIT:
            _CACHE.popitem(last=False)
    else:
        _CACHE.move_to_end(key)
    return value


def _reserve_weights(world: World) -> list[float]:
    return cumulative([r.weight for r in world.reserves])


# -- staff on duty ---------------------------------------------------------


def rangers_by_reserve(world: World, day: date) -> dict[int, list[str]]:
    """Active rangers per assigned reserve, from the weekly roster (Monday noon)."""
    monday = day - timedelta(days=day.weekday())

    def build() -> dict[int, list[str]]:
        noon = local_midnight(monday) + 12 * HOUR
        out: dict[int, list[str]] = {}
        for slot in range(world.staff_slots):
            member = world.staff_at(slot, noon)
            if member is None:
                continue
            state = world.staff_state(member, noon)
            if state.status == "active" and state.role in {"ranger", "senior_ranger"}:
                out.setdefault(state.reserve, []).append(member.staff_id)
        return out

    return _cached((id(world), "rangers", monday), build)


# -- patrols ---------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Patrol:
    patrol_id: str
    reserve: int
    zone: int
    team_code: str
    leader_staff_id: str
    member_count: int
    transport: str
    created_at: int
    planned_start: int
    cancelled_at: int | None
    started_at: int
    ended_at: int
    aborted: bool
    walk_seed: int


TRANSPORTS = (
    ("foot", 0.70, 3.2),
    ("vehicle", 0.12, 18.0),
    ("boat", 0.10, 9.0),
    ("drone_support", 0.08, 3.0),
)
_TRANSPORT_WEIGHTS = cumulative([t[1] for t in TRANSPORTS])
SPEED_KMH = {name: speed for name, _, speed in TRANSPORTS}


def patrols_planned_for(world: World, day: date) -> list[Patrol]:
    """Patrols planned to run on local ``day``."""

    def build() -> list[Patrol]:
        cfg = world.config
        rng = Rng(cfg.seed, "patrols", day.isoformat())
        rangers = rangers_by_reserve(world, day)
        everyone = [s for ids in rangers.values() for s in ids]
        weights = _reserve_weights(world)
        base = local_midnight(day)
        out: list[Patrol] = []
        for n in range(cfg.scaled(1000)):
            reserve = rng.weighted(weights)
            zones = world.zones_by_reserve[reserve]
            zone = zones[rng.below(len(zones))]
            pool = rangers.get(reserve) or everyone or ["STF-0000001"]
            leader = pool[rng.below(len(pool))]
            transport = TRANSPORTS[rng.weighted(_TRANSPORT_WEIGHTS)][0]
            night = rng.chance(0.1)
            planned = (
                base
                + (rng.between(19, 22) if night else rng.between(5, 15)) * HOUR
                + rng.below(4) * 900
            )
            created = base - rng.between(1, 3) * DAY_SECONDS + rng.between(8, 17) * HOUR
            cancelled = (
                created + rng.between(1, 20) * HOUR if rng.chance(0.02) else None
            )
            if cancelled is not None and cancelled >= planned:
                cancelled = planned - HOUR
            started = planned + rng.below(1800)
            duration = rng.between(3 * 4, 10 * 4) * 900
            aborted = rng.chance(0.03)
            if aborted:
                duration = int(duration * rng.uniform(0.2, 0.6))
            out.append(
                Patrol(
                    patrol_id=f"PTR-{day:%Y%m%d}-{n + 1:05d}",
                    reserve=reserve,
                    zone=zone,
                    team_code=f"T-{reserve + 1:03d}-{rng.between(1, 6)}",
                    leader_staff_id=leader,
                    member_count=rng.between(2, 6),
                    transport=transport,
                    created_at=created,
                    planned_start=planned,
                    cancelled_at=cancelled,
                    started_at=started,
                    ended_at=started + duration,
                    aborted=aborted,
                    walk_seed=rng.below(1 << 30),
                )
            )
        return out

    return _cached((id(world), "patrols", day), build)


# -- deforestation alerts ---------------------------------------------------


@dataclass(frozen=True, slots=True)
class DeforestationAlert:
    alert_id: str
    detected_on: date
    detected_at: int
    sensor: str
    lat: float
    lon: float
    area_cents: int
    reserve: int | None
    zone: int | None
    confidence_e3: int
    validated_at: int
    confirmed: bool


def deforestation_alerts_on(world: World, day: date) -> list[DeforestationAlert]:
    def build() -> list[DeforestationAlert]:
        cfg = world.config
        rng = Rng(cfg.seed, "deforestation", day.isoformat())
        base = local_midnight(day)
        out = []
        for n in range(rng.count(150 * cfg.scale)):
            zone_index = rng.below(len(world.zones))
            zone = world.zones[zone_index]
            inside = rng.chance(0.6)
            lat = zone.lat + rng.gauss(0.0, 0.01 if inside else 0.12)
            lon = zone.lon + rng.gauss(0.0, 0.01 if inside else 0.12)
            out.append(
                DeforestationAlert(
                    alert_id=f"DFA-{day:%Y%m%d}-{n + 1:05d}",
                    detected_on=day,
                    detected_at=base + rng.between(2, 20) * HOUR,
                    sensor="radar" if rng.chance(0.4) else "optical",
                    lat=lat,
                    lon=lon,
                    area_cents=int(rng.lognormal(1.2, 1.0) * 100) + 50,
                    reserve=zone.reserve if inside else None,
                    zone=zone_index if inside else None,
                    confidence_e3=rng.between(500, 990),
                    validated_at=base
                    + rng.between(1, 20) * DAY_SECONDS
                    + rng.between(8, 18) * HOUR,
                    confirmed=rng.chance(0.35),
                )
            )
        return out

    return _cached((id(world), "deforestation", day), build)


# -- incidents, notices, payments --------------------------------------------


@dataclass(frozen=True, slots=True)
class Payment:
    payment_id: str
    notice_ref: str
    payer_document: str
    paid_on: date
    posted_on: date
    amount_cents: int
    method: str
    bank_reference: str
    is_refund: bool
    case: str


@dataclass(frozen=True, slots=True)
class Notice:
    notice_id: str
    incident_id: str
    offender_type: str
    offender_name: str
    offender_document: str
    infraction_type: str
    issued_at: int
    due_on: date
    # (instant, status, amount in cents after the change)
    transitions: tuple[tuple[int, str, int], ...]
    payments: tuple[Payment, ...]


@dataclass(frozen=True, slots=True)
class Incident:
    incident_id: str
    created_day: date
    reserve: int
    zone: int
    incident_type: str
    severity: int
    reported_at: int
    reported_by: str
    reporter_staff_id: str | None
    reporter_citizen: int | None
    source_device_id: str | None
    source_alert_id: str | None
    patrol_id: str | None
    lat: float
    lon: float
    area_cents: int | None
    notes: str
    notes_pii: tuple[str, ...]
    transitions: tuple[tuple[int, str], ...]
    deleted_at: int | None
    notice: Notice | None


BASE_INCIDENTS: tuple[tuple[str, float, int], ...] = (
    ("illegal_logging", 60, 4),
    ("poaching", 40, 4),
    ("encroachment", 50, 3),
    ("illegal_mining", 10, 5),
    ("trash_dumping", 80, 2),
    ("invasive_species", 40, 2),
    ("road_kill", 60, 1),
    ("trap_found", 30, 3),
    ("unauthorized_fire", 20, 4),
)
ENFORCEABLE = {
    "illegal_logging",
    "poaching",
    "encroachment",
    "illegal_mining",
    "unauthorized_fire",
    "wildfire",
}
FINE_RANGES = {
    "illegal_logging": (5_000, 500_000),
    "poaching": (500, 50_000),
    "encroachment": (2_000, 200_000),
    "illegal_mining": (10_000, 1_000_000),
    "unauthorized_fire": (1_000, 100_000),
    "wildfire": (5_000, 300_000),
}
NOTES: dict[str, tuple[str, ...]] = {
    "illegal_logging": (
        "Equipe encontrou {n} árvores cortadas próximo à trilha {t}.",
        "Toras empilhadas e marcas de motosserra recentes na {z}.",
    ),
    "poaching": (
        "Encontrados vestígios de caça: {n} cartuchos e um acampamento.",
        "Armadilha com animal preso foi desmontada; animal liberado.",
    ),
    "encroachment": (
        "Cerca nova avançando {n} metros sobre a área protegida.",
        "Construção irregular iniciada dentro da {z}.",
    ),
    "illegal_mining": (
        "Escavação com bomba d'água às margens do rio, {n} metros de extensão.",
    ),
    "trash_dumping": ("Descarte de entulho e lixo doméstico, cerca de {n} sacos.",),
    "invasive_species": ("Foco de espécie invasora em {n} m², remoção agendada.",),
    "road_kill": ("Animal atropelado na estrada de acesso, km {n}.",),
    "trap_found": ("Laço de aço encontrado e removido na trilha {t}.",),
    "unauthorized_fire": (
        "Queima de pasto sem autorização atingiu {n} metros da borda.",
    ),
    "wildfire": (
        "Incêndio florestal com frente de {n} metros; brigada acionada.",
        "Fogo controlado após {n} horas de combate na {z}.",
    ),
}


def _notes(rng: Rng, incident_type: str, zone_code: str) -> tuple[str, tuple[str, ...]]:
    text = rng.choice(NOTES[incident_type]).format(
        n=rng.between(2, 300), t=rng.between(1, 40), z=zone_code
    )
    if rng.chance(0.08):
        text += '\nObservação: área "isolada" até nova vistoria.'
    kinds: tuple[str, ...] = ()
    if rng.chance(0.04):
        sentence, kinds = pii_sentence(rng)
        text = f"{text} {sentence}"
    return text, kinds


def _lifecycle(
    rng: Rng, reported_at: int, incident_type: str
) -> tuple[tuple[int, str], ...]:
    steps = [(reported_at, "reported")]
    when = reported_at + rng.between(2, 48) * HOUR
    if rng.chance(0.1):
        steps.append((when, "dismissed"))
        return tuple(steps)
    steps.append((when, "verified"))
    if incident_type in ENFORCEABLE and rng.chance(0.6):
        when += rng.between(1, 10) * DAY_SECONDS
        steps.append((when, "under_investigation"))
        if rng.chance(0.2):
            when += rng.between(3, 20) * DAY_SECONDS
            steps.append((when, "referred_to_police"))
    when += rng.between(5, 50) * DAY_SECONDS
    steps.append((when, "closed"))
    return tuple(steps)


def _payments(
    rng: Rng,
    notice_id: str,
    document: str,
    issued_day: date,
    due_on: date,
    amount: int,
    cancelled: int | None,
) -> tuple[tuple[Payment, ...], str | None]:
    """Payments for one notice and the status they lead to (paid, or None)."""
    payments: list[Payment] = []
    draw = rng.random()

    def pay(
        k: int,
        paid_on: date,
        cents: int,
        case: str,
        *,
        ref: str = notice_id,
        refund: bool = False,
    ) -> None:
        payments.append(
            Payment(
                payment_id=f"PAY-{notice_id[4:]}-{k:02d}",
                notice_ref=ref,
                payer_document=document,
                paid_on=paid_on,
                posted_on=paid_on + timedelta(days=rng.between(0, 3)),
                amount_cents=cents,
                method=rng.choice(("boleto", "pix", "darf", "transfer")),
                bank_reference=f"BR{rng.between(10**9, 10**10 - 1)}",
                is_refund=refund,
                case=case,
            )
        )

    window = max(1, (due_on - issued_day).days)
    if cancelled is not None:
        if rng.chance(0.3):
            paid = issued_day + timedelta(days=rng.below(window))
            pay(1, paid, amount, "paid_then_cancelled")
            pay(
                2,
                paid + timedelta(days=rng.between(20, 60)),
                amount,
                "refund",
                refund=True,
            )
        return tuple(payments), None
    if draw < 0.55:
        pay(1, issued_day + timedelta(days=rng.below(window)), amount, "full")
        return tuple(payments), "paid"
    if draw < 0.67:
        parts = rng.between(2, 6)
        share = amount // parts
        for k in range(parts):
            cents = share if k < parts - 1 else amount - share * (parts - 1)
            pay(
                k + 1,
                issued_day + timedelta(days=rng.below(window) + 30 * k),
                cents,
                "installment",
            )
        return tuple(payments), "paid"
    if draw < 0.82:
        return (), None  # never paid: overdue
    if draw < 0.85:
        pay(
            1,
            issued_day + timedelta(days=rng.below(window)),
            amount + rng.between(1_000, 50_000),
            "overpaid",
        )
        return tuple(payments), "paid"
    if draw < 0.87:
        paid = issued_day + timedelta(days=rng.below(window))
        pay(1, paid, amount, "duplicate")
        pay(2, paid + timedelta(days=rng.between(0, 3)), amount, "duplicate")
        return tuple(payments), "paid"
    if draw < 0.90:
        digits = list(notice_id)
        position = len(digits) - 1 - rng.below(4)
        digits[position] = str((int(digits[position]) + 1 + rng.below(8)) % 10)
        pay(
            1,
            issued_day + timedelta(days=rng.below(window)),
            amount,
            "unmatched",
            ref="".join(digits),
        )
        return tuple(payments), None
    pay(
        1,
        issued_day + timedelta(days=rng.below(window)),
        int(amount * rng.uniform(0.5, 0.9)),
        "partial",
    )
    return tuple(payments), None


def _notice(rng: Rng, incident_id: str, incident_type: str, opened_at: int) -> Notice:
    issued = opened_at + rng.between(1, 15) * DAY_SECONDS
    issued_day = date(1970, 1, 1) + timedelta(days=(issued - 3 * HOUR) // DAY_SECONDS)
    due_on = issued_day + timedelta(days=30)
    low, high = FINE_RANGES[incident_type]
    amount = rng.between(low, high) * 100 + rng.choice((0, 0, 50, 99))
    individual = rng.chance(0.7)
    name = full_name(rng) if individual else company_name(rng)
    document = cpf(rng) if individual else cnpj(rng)
    notice_id = "AIN-" + incident_id[4:]
    transitions: list[tuple[int, str, int]] = [(issued, "issued", amount)]
    when = issued
    if rng.chance(0.2):
        when += rng.between(5, 20) * DAY_SECONDS
        transitions.append((when, "appealed", amount))
        when += rng.between(20, 60) * DAY_SECONDS
        if rng.chance(0.5):
            amount = int(amount * rng.uniform(0.5, 0.8))
            transitions.append((when, "reduced", amount))
        else:
            transitions.append((when, "upheld", amount))
        due_on = date(1970, 1, 1) + timedelta(
            days=(when - 3 * HOUR) // DAY_SECONDS + 30
        )
    cancelled = None
    if rng.chance(0.05):
        cancelled = when + rng.between(10, 40) * DAY_SECONDS
    payments, outcome = _payments(
        rng, notice_id, document, issued_day, due_on, amount, cancelled
    )
    if cancelled is not None:
        transitions.append((cancelled, "cancelled", amount))
    elif outcome == "paid":
        last = max(p.posted_on for p in payments if not p.is_refund)
        transitions.append((local_midnight(last) + 7 * HOUR, "paid", amount))
    else:
        transitions.append(
            (local_midnight(due_on + timedelta(days=1)) + 6 * HOUR, "overdue", amount)
        )
    transitions.sort(key=lambda item: item[0])
    return Notice(
        notice_id=notice_id,
        incident_id=incident_id,
        offender_type="individual" if individual else "company",
        offender_name=name,
        offender_document=document,
        infraction_type=incident_type,
        issued_at=issued,
        due_on=due_on,
        transitions=tuple(transitions),
        payments=payments,
    )


def incidents_created_on(world: World, day: date) -> list[Incident]:
    """Incidents reported on local ``day``, with their whole lifecycle."""

    def build() -> list[Incident]:
        cfg = world.config
        fleet: Fleet = fleet_for(world)
        rng = Rng(cfg.seed, "incidents", day.isoformat())
        base = local_midnight(day)
        end = base + DAY_SECONDS
        weights = _reserve_weights(world)
        rangers = rangers_by_reserve(world, day)
        everyone = [s for ids in rangers.values() for s in ids] or ["STF-0000001"]
        registered = max(1, world.citizens_registered(base))
        seeds: list[
            tuple[
                str, int, int, str, str | None, str | None, float | None, float | None
            ]
        ] = []
        # (type, zone, reported_at, reported_by, device, alert_id, lat, lon)
        for incident_type, mean, _ in BASE_INCIDENTS:
            for _ in range(rng.count(mean * cfg.scale)):
                reserve = rng.weighted(weights)
                zones = world.zones_by_reserve[reserve]
                zone = zones[rng.below(len(zones))]
                by = (
                    "citizen"
                    if rng.chance(0.15)
                    else ("patrol" if rng.chance(0.7) else "staff")
                )
                seeds.append(
                    (
                        incident_type,
                        zone,
                        base + rng.below(DAY_SECONDS),
                        by,
                        None,
                        None,
                        None,
                        None,
                    )
                )
        for utc_day in (day, day + timedelta(days=1)):
            for fire in fleet.fires(utc_day):
                reported = fire.start + rng.between(1, 12) * 1800
                if base <= reported < end:
                    by = rng.choice(("patrol", "citizen", "sensor_alert"))
                    seeds.append(
                        ("wildfire", fire.zone, reported, by, None, None, None, None)
                    )
            for (_, slot_index), alerts in sorted(fleet.threat_alerts(utc_day).items()):
                slot = fleet.slots_by_type["acoustic"][slot_index]
                for alert in alerts:
                    if (
                        alert.alert_type not in {"chainsaw", "gunshot"}
                        or rng.random() >= 0.3
                    ):
                        continue
                    reported = alert.at + rng.between(1, 12) * 1800
                    if base <= reported < end:
                        device = fleet.device_at(slot, alert.at)
                        kind = (
                            "illegal_logging"
                            if alert.alert_type == "chainsaw"
                            else "poaching"
                        )
                        seeds.append(
                            (
                                kind,
                                slot.zone,
                                reported,
                                "sensor_alert",
                                device.device_id if device else None,
                                None,
                                None,
                                None,
                            )
                        )
        for back in range(0, 21):
            for alert in deforestation_alerts_on(world, day - timedelta(days=back)):
                if (
                    alert.confirmed
                    and base <= alert.validated_at < end
                    and alert.zone is not None
                ):
                    seeds.append(
                        (
                            "encroachment"
                            if alert.area_cents < 500
                            else "illegal_logging",
                            alert.zone,
                            alert.validated_at,
                            "satellite_alert",
                            None,
                            alert.alert_id,
                            alert.lat,
                            alert.lon,
                        )
                    )
        seeds.sort(key=lambda s: (s[2], s[0], s[1]))
        patrol_count = cfg.scaled(1000)
        out: list[Incident] = []
        for n, (
            incident_type,
            zone_index,
            reported,
            by,
            device_id,
            alert_id,
            lat,
            lon,
        ) in enumerate(seeds):
            zone = world.zones[zone_index]
            severity = next((s for t, _, s in BASE_INCIDENTS if t == incident_type), 4)
            incident_id = f"INC-{day:%Y%m%d}-{n + 1:05d}"
            notes, kinds = _notes(rng, incident_type, f"zona {zone.zone_id[-4:]}")
            transitions = _lifecycle(rng, reported, incident_type)
            deleted = (
                reported + rng.between(1, 5) * DAY_SECONDS
                if rng.chance(0.005)
                else None
            )
            opened = next(
                (t for t, s in transitions if s == "under_investigation"), None
            )
            notice = None
            if (
                opened is not None
                and incident_type in ENFORCEABLE
                and deleted is None
                and rng.chance(0.5)
            ):
                notice = _notice(
                    Rng(cfg.seed, "notice", incident_id),
                    incident_id,
                    incident_type,
                    opened,
                )
            out.append(
                Incident(
                    incident_id=incident_id,
                    created_day=day,
                    reserve=zone.reserve,
                    zone=zone_index,
                    incident_type=incident_type,
                    severity=min(5, max(1, severity + rng.between(-1, 1))),
                    reported_at=reported,
                    reported_by=by,
                    reporter_staff_id=everyone[rng.below(len(everyone))]
                    if by in {"patrol", "staff"}
                    else None,
                    reporter_citizen=rng.below(registered) if by == "citizen" else None,
                    source_device_id=device_id,
                    source_alert_id=alert_id,
                    patrol_id=f"PTR-{day:%Y%m%d}-{rng.below(patrol_count) + 1:05d}"
                    if by == "patrol"
                    else None,
                    lat=lat if lat is not None else zone.lat + rng.gauss(0.0, 0.01),
                    lon=lon if lon is not None else zone.lon + rng.gauss(0.0, 0.01),
                    area_cents=rng.between(5, 5000) * 10
                    if incident_type
                    in {
                        "illegal_logging",
                        "encroachment",
                        "wildfire",
                        "unauthorized_fire",
                        "illegal_mining",
                    }
                    else None,
                    notes=notes,
                    notes_pii=kinds,
                    transitions=transitions,
                    deleted_at=deleted,
                    notice=notice,
                )
            )
        return out

    return _cached((id(world), "incidents", day), build)


# -- citizen reports --------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CitizenReport:
    report_id: str
    citizen: int | None
    received_at: int
    channel: str
    category: str
    lat: float
    lon: float
    description: str
    description_pii: tuple[str, ...]
    linked_incident_id: str | None
    triaged_at: int
    triage_status: str
    deleted_at: int | None


REPORT_CATEGORIES = (
    ("fire_smoke", 0.15),
    ("logging", 0.12),
    ("hunting", 0.10),
    ("animal_sighting", 0.30),
    ("trash", 0.13),
    ("other", 0.20),
)
_REPORT_WEIGHTS = cumulative([w for _, w in REPORT_CATEGORIES])
REPORT_TEXT: dict[str, tuple[str, ...]] = {
    "fire_smoke": (
        "Muita fumaça vindo da serra perto da estrada.",
        "Vi fogo na mata atrás do sítio, parece grande.",
    ),
    "logging": (
        "Barulho de motosserra o dia inteiro perto do rio.",
        "Caminhão saindo da mata carregado de toras.",
    ),
    "hunting": (
        "Ouvi tiros de madrugada na beira da reserva.",
        "Tem gente armando laço na trilha da cachoeira.",
    ),
    "animal_sighting": (
        "Vi um bando de bugios na mata ciliar hoje cedo.",
        "Apareceu uma onça-parda na estrada de terra.",
    ),
    "trash": (
        "Jogaram entulho na entrada da trilha.",
        "Muito lixo plástico acumulado na margem do rio.",
    ),
    "other": (
        "Placa da reserva foi derrubada.",
        "Ponte da trilha está quebrada, perigoso.",
    ),
}
_INCIDENT_CATEGORY = {
    "wildfire": "fire_smoke",
    "unauthorized_fire": "fire_smoke",
    "illegal_logging": "logging",
    "poaching": "hunting",
    "trap_found": "hunting",
}


def reports_received_on(world: World, day: date) -> list[CitizenReport]:
    def build() -> list[CitizenReport]:
        cfg = world.config
        rng = Rng(cfg.seed, "reports", day.isoformat())
        base = local_midnight(day)
        registered = max(1, world.citizens_registered(base))
        items: list[tuple[int, str, int | None, str | None, int]] = []
        # (received_at, category, citizen, incident_id, zone)
        for _ in range(cfg.scaled(2600)):
            category = REPORT_CATEGORIES[rng.weighted(_REPORT_WEIGHTS)][0]
            citizen = rng.below(registered) if rng.chance(0.85) else None
            items.append(
                (
                    base + rng.below(DAY_SECONDS),
                    category,
                    citizen,
                    None,
                    rng.below(len(world.zones)),
                )
            )
        for incident in incidents_created_on(world, day):
            if incident.reported_by == "citizen":
                category = _INCIDENT_CATEGORY.get(incident.incident_type, "other")
                items.append(
                    (
                        max(base, incident.reported_at - rng.between(10, 120) * 60),
                        category,
                        incident.reporter_citizen,
                        incident.incident_id,
                        incident.zone,
                    )
                )
        items.sort(key=lambda item: (item[0], item[1]))
        out: list[CitizenReport] = []
        for n, (received, category, citizen, incident_id, zone_index) in enumerate(
            items
        ):
            zone = world.zones[zone_index]
            text = rng.choice(REPORT_TEXT[category])
            kinds: tuple[str, ...] = ()
            if rng.chance(0.04):
                sentence, kinds = pii_sentence(rng)
                text = f"{text} {sentence}"
            triaged = received + rng.between(1, 72) * HOUR
            spam = incident_id is None and rng.chance(0.03)
            status = (
                "linked"
                if incident_id
                else (
                    "spam" if spam else ("dismissed" if rng.chance(0.3) else "triaged")
                )
            )
            out.append(
                CitizenReport(
                    report_id=f"RPT-{day:%Y%m%d}-{n + 1:05d}",
                    citizen=citizen,
                    received_at=received,
                    channel=rng.choice(("app", "hotline", "whatsapp", "email")),
                    category=category,
                    lat=zone.lat + rng.gauss(0.0, 0.03),
                    lon=zone.lon + rng.gauss(0.0, 0.03),
                    description=text,
                    description_pii=kinds,
                    linked_incident_id=incident_id,
                    triaged_at=triaged,
                    triage_status=status,
                    deleted_at=triaged + rng.between(1, 3) * DAY_SECONDS
                    if spam
                    else None,
                )
            )
        return out

    return _cached((id(world), "reports", day), build)


# -- restoration -------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PlantingLot:
    lot_id: str
    zone: int
    species: int
    planted_on: date
    planted_at: int
    seedlings: int
    area_cents: int
    crew: int
    checks: tuple[
        tuple[int, int, int], ...
    ]  # (instant, days after planting, survival in 0.01 %)


def lots_planted_on(world: World, day: date) -> list[PlantingLot]:
    def build() -> list[PlantingLot]:
        cfg = world.config
        rng = Rng(cfg.seed, "lots", day.isoformat())
        base = local_midnight(day)
        season = 2.0 if day.month in {10, 11, 12, 1, 2, 3} else 0.6
        zones = world.zones_by_type.get("restoration") or [z.index for z in world.zones]
        out = []
        for n in range(rng.count(150 * cfg.scale * season)):
            zone = zones[rng.below(len(zones))]
            order, weights = world.ranked_species("tree", world.zones[zone].reserve)
            species = order[rng.weighted(weights)]
            seedlings = rng.between(200, 5000)
            survival = 10_000
            checks = []
            for after, jitter in ((30, 3), (90, 5), (365, 10)):
                survival = int(survival * rng.uniform(0.80, 0.98))
                when = (
                    base
                    + (after + rng.between(-jitter, jitter)) * DAY_SECONDS
                    + rng.between(8, 16) * HOUR
                )
                checks.append((when, after, survival))
            out.append(
                PlantingLot(
                    lot_id=f"LOT-{day:%Y%m%d}-{n + 1:05d}",
                    zone=zone,
                    species=species,
                    planted_on=day,
                    planted_at=base + rng.between(7, 15) * HOUR,
                    seedlings=seedlings,
                    area_cents=int(seedlings / rng.uniform(1100, 2500) * 100) + 1,
                    crew=rng.between(4, 20),
                    checks=tuple(checks),
                )
            )
        return out

    return _cached((id(world), "lots", day), build)


ANALYTES: tuple[tuple[str, str, str, float, float, int], ...] = (
    ("organic_carbon", "%", "walkley_black", 0.5, 6.0, 2),
    ("bulk_density", "g/cm3", "core_ring", 0.80, 1.60, 2),
    ("ph_h2o", "pH", "potentiometric", 4.0, 6.5, 1),
    ("total_nitrogen", "%", "kjeldahl", 0.05, 0.60, 3),
    ("available_p", "mg/kg", "mehlich_1", 1.0, 25.0, 1),
    ("clay", "%", "pipette", 10.0, 65.0, 1),
)


@dataclass(frozen=True, slots=True)
class SoilResult:
    sample_id: str
    version: int
    zone: int
    lot_id: str | None
    collected_on: date
    received_at: int
    lab_code: str
    analytes: tuple[
        tuple[str, str, str, int, int], ...
    ]  # name, unit, method, value, scale
    correction: bool


def samples_collected_on(world: World, day: date) -> list[SoilResult]:
    """Every result version of the samples collected on ``day``."""

    def build() -> list[SoilResult]:
        cfg = world.config
        rng = Rng(cfg.seed, "soil", day.isoformat())
        base = local_midnight(day)
        out: list[SoilResult] = []
        zones = world.zones_by_type.get("restoration", []) + world.zones_by_type.get(
            "core", []
        )
        for n in range(cfg.scaled(250)):
            lot = None
            if rng.chance(0.4):
                lots = lots_planted_on(
                    world, day - timedelta(days=rng.between(30, 365))
                )
                lot = lots[rng.below(len(lots))] if lots else None
            zone = lot.zone if lot else zones[rng.below(len(zones))]
            values = []
            for name, unit_name, method, low, high, scale in ANALYTES:
                values.append(
                    (
                        name,
                        unit_name,
                        method,
                        round(rng.uniform(low, high) * 10**scale),
                        scale,
                    )
                )
            sample_id = f"SMP-{day:%Y%m%d}-{n + 1:05d}"
            received = (
                base + rng.between(5, 30) * DAY_SECONDS + rng.between(9, 18) * HOUR
            )
            lab = rng.choice(("LAB-SP01", "LAB-PR02", "LAB-BA03", "LAB-RJ04"))
            out.append(
                SoilResult(
                    sample_id,
                    1,
                    zone,
                    lot.lot_id if lot else None,
                    day,
                    received,
                    lab,
                    tuple(values),
                    False,
                )
            )
            if rng.chance(0.03):
                fixed = list(values)
                position = rng.below(len(fixed))
                name, unit_name, method, value, scale = fixed[position]
                fixed[position] = (
                    name,
                    unit_name,
                    method,
                    int(value * rng.uniform(0.7, 1.3)),
                    scale,
                )
                corrected = received + rng.between(10, 40) * DAY_SECONDS
                out.append(
                    SoilResult(
                        sample_id,
                        2,
                        zone,
                        lot.lot_id if lot else None,
                        day,
                        corrected,
                        lab,
                        tuple(fixed),
                        True,
                    )
                )
        return out

    return _cached((id(world), "soil", day), build)


def season_factor(day: date) -> float:
    return FIRE_SEASON[day.month - 1]
