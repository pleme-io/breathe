//! **The authorization axis's forcing functions.** Stage S4.
//!
//! # Why this file exists
//!
//! On 2026-06-19, commit `76924b0` rewrote `Band::promotion_mode` from
//!
//! ```text
//! None if self.dry_run() => Shadow,   None => ShadowConfirmEffect
//! ```
//!
//! to `self.mode_spec().unwrap_or(ShadowConfirmEffect)`. That silently inverted
//! the meaning of `spec.dryRun` on every live CR: 74 of camelot-eks's 76 bands
//! declared `dryRun: true` as an explicit safety statement, and ~4,045 real
//! carves ran under it anyway.
//!
//! **The commit went green.** Not because the suite was thin — 882 tests passed
//! — but because every one of them tested the FSM (*given* a `PromotionMode`,
//! does the lifecycle behave?) and not one tested the MAPPING (*given* an
//! authored spec shape, which `PromotionMode` do we get?). The rewritten line
//! was the mapping. Nothing pinned it, so nothing broke.
//!
//! Two further facts made it invisible for five weeks: there is no `ci.yml` in
//! this repo (those 882 tests are enforced by nothing on push), and the field's
//! CRD description continued to describe the *old* rule — so an operator reading
//! `kubectl explain` was told the opposite of what the controller did.
//!
//! # What is pinned here
//!
//! | test | catches |
//! |---|---|
//! | [`gate_composition_matrix`] | a resolution change that alters what an authored spec shape means — **including `76924b0` itself** |
//! | [`gate_matrix_covers_every_discriminant`] | a matrix that has gone stale relative to the type it pins |
//! | [`an_authored_intent_makes_every_kind_agree`] | a new/edited band kind that diverges from the fleet gate |
//! | [`every_gate_field_is_observable_or_declared_retired`] | a field going inert **and** a retired field coming back to life |
//! | [`crd_descriptions_carry_the_canonical_claims`] | prose drifting from the class split it describes |
//! | [`every_write_gated_crd_is_named_in_the_census`] | a write surface added OUTSIDE `DimensionId::ALL`, where the `[Kind; 10]` guard is structurally blind (this is how `PodMemoryHigh` reached a real host write unnoticed) |
//! | [`every_write_surface_status_carries_the_typed_gate`] | a write surface that cannot answer "am I writing, and why" |
//! | [`legacy_two_state_gate_reproduces_the_bool_truth_table`] | the Tier-B two-key rule changing meaning while being retyped from `bool` to `EffectiveGate` |
//! | [`authored_write_gate_names_its_authority_or_refuses`] | the convenience wrapper becoming a back door around attribution |
//!
//! # Tier (never rounded up)
//!
//! * **Truly-unrepresentable (compile error).** Adding a `DimensionId` arm
//!   without adding a fixture is `E0308`: [`kinds`] returns
//!   `[Kind; DimensionId::ALL.len()]`, so the array literal's length must track
//!   the enum. Adding a `ShadowReason` / `WitnessKind` / `PromotionMode` arm
//!   without a label is `E0004` in this file's exhaustive `match` helpers.
//!
//!   Precise credit, because the difference matters when you are reading a build
//!   log: adding an 11th dimension was tried, and the workspace *does* fail to
//!   build — but the first `E0308` comes from **`breathe-catalog`**, which
//!   already carried its own `[_; 10]` guard, so compilation never reaches this
//!   file. This array is therefore a *second, independent* guard, not the one
//!   that fires first. Verified separately by dropping a fixture, which yields
//!   `gate_matrix.rs:179: expected an array with a size of 10, found one with a
//!   size of 9`.
//! * **CI forcing-function (a test, not a type).** Everything else. The matrix's
//!   expectations are literal values a human wrote; a resolution change makes
//!   them red, but only *once the test runs*. Given this repo had no test CI at
//!   all until the `ci.yml` landed alongside this file, that caveat is load-
//!   bearing rather than pedantic.
//! * **NOT claimed.** That a new `Band` impl reusing an *existing* `DimensionId`
//!   would be caught (it would not — see [`kinds`]). That the prose in a CRD
//!   description is *true* (see `breathe_provider::gate`'s claim consts and
//!   [`crd_descriptions_carry_the_canonical_claims`]'s own doc). That any of
//!   this makes an unwise *authored* write impossible — a human may author
//!   `{intent: write}` on a database and mean it; the axis makes that legible
//!   and attributable, never prevented.
//!
//! # These tests were proven to bite
//!
//! Each was verified by deliberately reintroducing the defect and observing the
//! failure, then reverting (2026-07-26):
//!
//! | injected defect | caught by | signal |
//! |---|---|---|
//! | restore the pre-`76924b0` `dryRun` resolution | matrix + inert lint | 12/270 cells; 8 × `RESURRECTED` |
//! | drop `HostParamBand`'s two-state override | matrix + inert lint | 5/270 cells; 1 × `INERT` |
//! | revert `dryRun`'s description to the old claim | doc-truth | 12 descriptions |
//! | add an 11th `DimensionId` | the build | `E0308` (in `breathe-catalog` first) |
//! | drop a fixture | the build | `E0308` in this file |
//! | mislabel a fixture's `dim` | fixture-identity | `cgroup` probed 2× |

use breathe_crd::{
    AppBand, ArcBand, Band, BreatheCloudPool, BreatheConfig, BreatheNodePool, BreatheOverview, BreathePosture, RequestBand,
    CgroupBand, CgroupCpuBand, CpuBand, Densa, HostParamBand, IsolationBand, KubeParamBand, MemoryBand, PodMemoryHigh,
    PromotionMode, QuinhaoPool, ReplicaBand, StorageBand,
};
use breathe_provider::gate::{EffectiveGate, LegacyPath, LegacyPathKind, ShadowReasonKind, WitnessKind};
use breathe_provider::DimensionId;
use kube::CustomResourceExt;
use serde_json::{json, Value};

// ─────────────────────────────── the ten kinds ────────────────────────────────

/// Which resolution shape a kind's *unauthored* (legacy) path takes. The three
/// classes are a fact about the shipped code, not a taxonomy invented here — and
/// making them explicit is the point: before this file, the divergence existed
/// but no artifact stated it, so nobody could see a kind drift out of its class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Class {
    /// `mode_spec()` reads `spec.mode`; no `promotion_mode()` override. Falls
    /// through to the compiled `ShadowConfirmEffect`. Seven kinds.
    Chain,
    /// `mode_spec()` is hardcoded `None` and `promotion_mode()` is overridden to
    /// a pure two-state `dryRun ? Shadow : Effect`. **The only two kinds for
    /// which `spec.dryRun` still decides anything.** `HostParamBand`,
    /// `KubeParamBand`.
    TwoState,
    /// `mode_spec()` is hardcoded `None` and there is no override, so the kind is
    /// *permanently* on the compiled `ShadowConfirmEffect` — `spec.mode` is not
    /// a field it has, and `spec.dryRun` is inert. `AppBand` alone. Until
    /// `writeIntent` landed, shadow was unrepresentable on this plane at any
    /// price.
    Default,
}

/// Build a CR of `B` from a shared shape. Generic over the band type so every
/// kind is exercised through the *same* code path — a per-kind copy is how
/// divergence hides.
fn build<B: Band>(kind: &str, required: Value, spec_extra: &Value, obs: Obs) -> B {
    let mut spec = json!({ "targetRef": { "kind": "Deployment", "name": "d", "apiVersion": "apps/v1" } });
    let s = spec.as_object_mut().expect("object");
    s.extend(required.as_object().expect("required is an object").clone());
    s.extend(spec_extra.as_object().expect("extra is an object").clone());

    let mut meta = json!({ "name": "x", "namespace": "n" });
    if obs.operator_confirmed {
        meta.as_object_mut()
            .expect("object")
            .insert("annotations".into(), json!({ breathe_crd::CONFIRMED_ANNOTATION: "true" }));
    }

    let mut obj = json!({
        "apiVersion": "breathe.pleme.io/v1", "kind": kind, "metadata": meta, "spec": spec,
    });
    if let Some(st) = obs.status() {
        obj.as_object_mut().expect("object").insert("status".into(), st);
    }
    serde_json::from_value(obj)
        .unwrap_or_else(|e| panic!("{kind} fixture must parse (a fixture bug hides a real failure): {e}"))
}

/// Resolve one kind's gate for a given authored shape + observation.
fn probe<B: Band>(kind: &str, required: Value, spec_extra: &Value, obs: Obs, now: i64, frozen: bool) -> EffectiveGate {
    build::<B>(kind, required, spec_extra, obs).resolve_gate(now, frozen)
}

/// What the kind itself reports its dimension to be — read off a real CR, never
/// re-asserted from the fixture's own declaration.
fn dim_of<B: Band>(kind: &str, required: Value) -> DimensionId {
    build::<B>(kind, required, &json!({}), READY_LONG).dimension_id()
}

type ProbeFn = fn(&Value, Obs, i64, bool) -> EffectiveGate;

struct Kind {
    /// The fixture's DECLARED dimension, checked against [`Kind::report_dim`].
    dim: DimensionId,
    name: &'static str,
    class: Class,
    probe: ProbeFn,
    /// The dimension the CR itself reports — the independent value that makes
    /// `dim` a claim rather than a label.
    report_dim: fn() -> DimensionId,
}

/// Every band kind, one row each.
///
/// **The length is `DimensionId::ALL.len()`, deliberately.** Add a dimension and
/// this array stops type-checking (`E0308: expected an array with a fixed size of
/// 11 elements, found one with 10`) — the compile error the operator asked for,
/// rather than a test that merely notices later.
///
/// What this does NOT catch, stated plainly: a *new `Band` impl* that reuses an
/// **existing** `DimensionId`. Practically that cannot ship, because
/// `breathe-facade` dispatches its `kube::Api<T>` on the id and two kinds cannot
/// share one — but that is an argument from elsewhere in the codebase, not a
/// guarantee this array makes.
const N_KINDS: usize = DimensionId::ALL.len();

// This function IS a table: one literal row per shipped dimension, and it grows
// by ~7 lines every time the substrate gains one. It crossed clippy's 100-line
// threshold at the eleventh (`RequestBand`). Splitting a per-dimension table into
// sub-functions to satisfy a line count would scatter the very list this file
// exists to keep in one readable place — the array's own length is already the
// forcing function that matters.
#[allow(clippy::too_many_lines)]
fn kinds() -> [Kind; N_KINDS] {
    // Each kind's non-gate required fields — the minimum that parses.
    fn none() -> Value {
        json!({})
    }
    fn host_param() -> Value {
        json!({
            "knob": { "sysctl": { "key": "vm.dirty_bytes" } },
            "metric": { "meminfoField": { "field": "Dirty" } },
        })
    }
    fn kube_param() -> Value {
        json!({
            "layout": { "crField": {
                "apiVersion": "postgresql.cnpg.io/v1", "kind": "Cluster", "name": "db",
                "fieldPath": "/spec/postgresql/parameters/max_connections", "restartFree": false
            } },
            "metric": { "prometheus": "max(cnpg_backends_total)" },
        })
    }
    fn app() -> Value {
        json!({
            "layout": { "apiCall": { "endpoint": "redis://redis:6379", "command": "maxmemory" } },
            "metric": { "prometheus": "redis_memory_used_bytes" },
        })
    }
    fn replica() -> Value {
        json!({ "metric": { "prometheus": "rate(http_requests_total[1m])" } })
    }
    /// A `RequestBand`'s one non-gate required field: which resource's request it
    /// carves. Required precisely because guessing between the OOM lever and the
    /// scheduling lever is the ambiguity the dimension exists to remove.
    fn request() -> Value {
        json!({ "resource": "memory" })
    }

    [
        Kind {
            dim: DimensionId::Memory,
            name: "MemoryBand",
            class: Class::Chain,
            probe: |e, o, n, f| probe::<MemoryBand>("MemoryBand", none(), e, o, n, f),
            report_dim: || dim_of::<MemoryBand>("MemoryBand", none()),
        },
        Kind {
            dim: DimensionId::Storage,
            name: "StorageBand",
            class: Class::Chain,
            probe: |e, o, n, f| probe::<StorageBand>("StorageBand", none(), e, o, n, f),
            report_dim: || dim_of::<StorageBand>("StorageBand", none()),
        },
        Kind {
            dim: DimensionId::Cpu,
            name: "CpuBand",
            class: Class::Chain,
            probe: |e, o, n, f| probe::<CpuBand>("CpuBand", none(), e, o, n, f),
            report_dim: || dim_of::<CpuBand>("CpuBand", none()),
        },
        Kind {
            dim: DimensionId::Replica,
            name: "ReplicaBand",
            class: Class::Chain,
            probe: |e, o, n, f| probe::<ReplicaBand>("ReplicaBand", replica(), e, o, n, f),
            report_dim: || dim_of::<ReplicaBand>("ReplicaBand", replica()),
        },
        Kind {
            dim: DimensionId::Arc,
            name: "ArcBand",
            class: Class::Chain,
            probe: |e, o, n, f| probe::<ArcBand>("ArcBand", none(), e, o, n, f),
            report_dim: || dim_of::<ArcBand>("ArcBand", none()),
        },
        Kind {
            dim: DimensionId::Cgroup,
            name: "CgroupBand",
            class: Class::Chain,
            probe: |e, o, n, f| probe::<CgroupBand>("CgroupBand", none(), e, o, n, f),
            report_dim: || dim_of::<CgroupBand>("CgroupBand", none()),
        },
        Kind {
            dim: DimensionId::CgroupCpu,
            name: "CgroupCpuBand",
            class: Class::Chain,
            probe: |e, o, n, f| probe::<CgroupCpuBand>("CgroupCpuBand", none(), e, o, n, f),
            report_dim: || dim_of::<CgroupCpuBand>("CgroupCpuBand", none()),
        },
        Kind {
            dim: DimensionId::HostParam,
            name: "HostParamBand",
            class: Class::TwoState,
            probe: |e, o, n, f| probe::<HostParamBand>("HostParamBand", host_param(), e, o, n, f),
            report_dim: || dim_of::<HostParamBand>("HostParamBand", host_param()),
        },
        Kind {
            dim: DimensionId::KubeParam,
            name: "KubeParamBand",
            class: Class::TwoState,
            probe: |e, o, n, f| probe::<KubeParamBand>("KubeParamBand", kube_param(), e, o, n, f),
            report_dim: || dim_of::<KubeParamBand>("KubeParamBand", kube_param()),
        },
        Kind {
            dim: DimensionId::AppParam,
            name: "AppBand",
            class: Class::Default,
            probe: |e, o, n, f| probe::<AppBand>("AppBand", app(), e, o, n, f),
            report_dim: || dim_of::<AppBand>("AppBand", app()),
        },
        // The RESERVATION band. `Chain`, deliberately and not by inheritance: it
        // is hand-rolled (not `band_kind!`-stamped), so its class was a free
        // choice, and it takes the full `writeIntent > mode > compiled
        // shadowConfirmEffect` chain rather than the honest-but-weaker two-state
        // reading. A band that can decide whether a workload survives OOM
        // pressure must ride the same shadow→confirm→effect promotion every
        // other carving band rides.
        Kind {
            dim: DimensionId::Request,
            name: "RequestBand",
            class: Class::Chain,
            probe: |e, o, n, f| probe::<RequestBand>("RequestBand", request(), e, o, n, f),
            report_dim: || dim_of::<RequestBand>("RequestBand", request()),
        },
    ]
}

// ───────────────────────────── observation states ─────────────────────────────

const READY_AT: &str = "1970-01-01T00:16:40Z"; // epoch 1000
const READY_EPOCH: i64 = 1000;
/// Well past the 1800s default confirm window.
const NOW_LONG: i64 = READY_EPOCH + 100_000;
/// Ten seconds in — inside the window.
const NOW_SHORT: i64 = READY_EPOCH + 10;

/// The band's observed condition, as the confirm gate sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Obs {
    label: &'static str,
    ready: bool,
    stale: bool,
    conflict: bool,
    no_status: bool,
    operator_confirmed: bool,
    now: i64,
}

impl Obs {
    const fn base(label: &'static str) -> Self {
        Self { label, ready: true, stale: false, conflict: false, no_status: false, operator_confirmed: false, now: NOW_LONG }
    }
    fn status(self) -> Option<Value> {
        if self.no_status {
            return None;
        }
        let mut conds = vec![json!({
            "type": "Ready",
            "status": if self.ready { "True" } else { "False" },
            "reason": "R", "message": "m", "lastTransitionTime": READY_AT,
        })];
        if self.stale {
            conds.push(json!({ "type": "Stale", "status": "True", "reason": "R", "message": "m", "lastTransitionTime": READY_AT }));
        }
        if self.conflict {
            conds.push(json!({ "type": "Conflict", "status": "True", "reason": "R", "message": "m", "lastTransitionTime": READY_AT }));
        }
        Some(json!({ "conditions": conds }))
    }
}

/// Ready and healthy, window long past.
const READY_LONG: Obs = Obs::base("ready-long");
/// Ready and healthy, but only 10s in.
const READY_SHORT: Obs = Obs { now: NOW_SHORT, ..Obs::base("ready-short") };
const NOT_READY: Obs = Obs { ready: false, ..Obs::base("not-ready") };
const STALE: Obs = Obs { stale: true, ..Obs::base("stale") };
const CONFLICT: Obs = Obs { conflict: true, ..Obs::base("conflict") };
const NO_STATUS: Obs = Obs { no_status: true, ..Obs::base("no-status") };
const CONFIRMED: Obs = Obs { operator_confirmed: true, now: NOW_SHORT, ..Obs::base("operator-confirmed") };

const ALL_OBS: [Obs; 7] = [READY_LONG, READY_SHORT, NOT_READY, STALE, CONFLICT, NO_STATUS, CONFIRMED];

// ────────────────────────────── the matrix rows ───────────────────────────────

/// What a row expects. Written out literally per row and per class, so a change
/// to the resolution rule must EDIT THIS TABLE — visibly, in the diff. That is
/// the whole mechanism: `76924b0` could not have been a silent one-line rewrite
/// against a table that states, in English and in data, what each authored shape
/// means.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Expect {
    Shadow(ShadowReasonKind),
    Live(WitnessKind),
}

impl Expect {
    fn matches(self, g: &EffectiveGate) -> bool {
        match (self, g) {
            (Self::Shadow(k), EffectiveGate::Shadow { reason }) => reason.kind() == k,
            (Self::Live(k), EffectiveGate::Live { witness }) => witness.kind() == k,
            _ => false,
        }
    }
}

fn describe(g: &EffectiveGate) -> String {
    match g {
        EffectiveGate::Shadow { reason } => format!("Shadow({:?})", reason.kind()),
        EffectiveGate::Live { witness } => format!("Live({:?})", witness.kind()),
    }
}

struct Row {
    /// The authored `spec.writeIntent`, or `None`.
    intent: Option<&'static str>,
    /// The authored `spec.mode`, or `None`. Silently ignored by `TwoState` and
    /// `Default` kinds, which carry no such field — an absence the matrix makes
    /// visible rather than hiding.
    mode: Option<&'static str>,
    dry_run: bool,
    /// The EXTERNAL freeze key (a pool / fleet master switch), not a spec field.
    frozen: bool,
    obs: Obs,
    chain: Expect,
    two_state: Expect,
    default: Expect,
    /// Why this row is what it is — read this before "fixing" a red row.
    why: &'static str,
}

const S: fn(ShadowReasonKind) -> Expect = Expect::Shadow;
const L: fn(WitnessKind) -> Expect = Expect::Live;

use ShadowReasonKind as SR;
use WitnessKind as WK;

/// The authored-shape → resolved-gate contract.
///
/// Groups, in order: intent coverage · mode coverage · **the root-defect
/// `dryRun` × `mode` block** · observation states · the freeze key · precedence.
#[allow(clippy::too_many_lines)]
fn gate_matrix() -> Vec<Row> {
    let mut rows: Vec<Row> = Vec::new();
    let mut push = |intent, mode, dry_run, frozen, obs, chain, two_state, default, why| {
        rows.push(Row { intent, mode, dry_run, frozen, obs, chain, two_state, default, why });
    };

    // ── G1 · every intent arm ────────────────────────────────────────────────
    push(None, None, false, false, READY_LONG,
        L(WK::LegacyDefault), L(WK::LegacyDefault), L(WK::LegacyDefault),
        "unauthored ⇒ the legacy chain promotes; the witness names it as migration debt");
    push(Some(r#"{"intent":"observe"}"#), None, false, false, READY_LONG,
        S(SR::ModeShadow), S(SR::ModeShadow), S(SR::ModeShadow),
        "the honest shadow — an authored hold, distinguishable from an accidental one");
    push(Some(r#"{"intent":"calibrateThenWrite"}"#), None, false, false, READY_LONG,
        L(WK::ConfirmGatePassed), L(WK::ConfirmGatePassed), L(WK::ConfirmGatePassed),
        "the bounded calibration 76924b0 wanted — stated out loud rather than inferred");
    push(Some(r#"{"intent":"write","authorizedBy":"drzzln 2026-07-26 S4"}"#), None, false, false, READY_LONG,
        L(WK::ExplicitIntent), L(WK::ExplicitIntent), L(WK::ExplicitIntent),
        "a named human authority — the only witness a CR can carry on its own");
    push(Some(r#"{"intent":"write"}"#), None, false, false, READY_LONG,
        S(SR::IntentMalformed), S(SR::IntentMalformed), S(SR::IntentMalformed),
        "unattributed go-live FAILS SAFE. Runtime mitigation, NOT apiserver rejection");
    push(Some(r#"{"intent":"frozen"}"#), None, false, false, READY_LONG,
        S(SR::Suspended), S(SR::Suspended), S(SR::Suspended),
        "never write, keep observing — folds the old `mode: suspended` into one word");

    // ── G2 · every mode arm (unauthored intent) ──────────────────────────────
    push(None, Some("shadow"), false, false, READY_LONG,
        S(SR::ModeShadow), L(WK::LegacyDefault), L(WK::LegacyDefault),
        "`mode` is NOT A FIELD on the two-state/default kinds — authoring it there does nothing");
    push(None, Some("shadowConfirmEffect"), false, false, READY_LONG,
        L(WK::LegacyDefault), L(WK::LegacyDefault), L(WK::LegacyDefault),
        "the compiled default, stated explicitly — same verdict either way");
    push(None, Some("effect"), false, false, READY_LONG,
        L(WK::LegacyDefault), L(WK::LegacyDefault), L(WK::LegacyDefault),
        "eyes-open go-live through the retired field; still LegacyDefault (no writeIntent authored)");
    push(None, Some("suspended"), false, false, READY_LONG,
        S(SR::Suspended), L(WK::LegacyDefault), L(WK::LegacyDefault),
        "same `mode`-is-not-a-field divergence as the `shadow` row");

    // ── G3 · THE ROOT DEFECT: dryRun × mode ──────────────────────────────────
    //
    // The block `76924b0` changed. Every `chain` column below reads Live with
    // `dryRun: true` authored — that IS the inversion, pinned as fact rather
    // than wished away. The `two_state` column is the control: `dryRun` still
    // decides there, which is exactly why a blanket "dryRun is retired" would
    // have been the opposite lie.
    push(None, None, true, false, READY_LONG,
        L(WK::LegacyDefault), S(SR::ModeShadow), L(WK::LegacyDefault),
        "★ THE ROW WHOSE ABSENCE LET 76924b0 SHIP GREEN. `dryRun: true`, no `mode`, no \
         `writeIntent`, window elapsed — the exact shape of ~70 live camelot-eks bands. \
         Chain/Default kinds CARVE FOR REAL; only the two-state kinds honour it. Before \
         76924b0 the chain column read Shadow(ModeShadow); the commit flipped it and no \
         test named the value, so nothing went red. Changing this cell is now a visible act");
    push(None, Some("shadow"), true, false, READY_LONG,
        S(SR::ModeShadow), S(SR::ModeShadow), L(WK::LegacyDefault),
        "chain shadows on `mode`, two-state on `dryRun` — same verdict, different cause");
    push(None, Some("effect"), true, false, READY_LONG,
        L(WK::LegacyDefault), S(SR::ModeShadow), L(WK::LegacyDefault),
        "the two-state kinds' `dryRun` BEATS an authored `mode: effect` they cannot read");
    push(None, Some("suspended"), true, false, READY_LONG,
        S(SR::Suspended), S(SR::ModeShadow), L(WK::LegacyDefault),
        "AppBand is live with BOTH `dryRun: true` and `mode: suspended` authored — it reads neither");
    push(None, Some("shadowConfirmEffect"), true, false, READY_LONG,
        L(WK::LegacyDefault), S(SR::ModeShadow), L(WK::LegacyDefault),
        "an explicit default plus `dryRun: true` still carves on the chain kinds");
    push(Some(r#"{"intent":"observe"}"#), None, true, false, READY_LONG,
        S(SR::ModeShadow), S(SR::ModeShadow), S(SR::ModeShadow),
        "the remedy: an authored intent makes the retired field irrelevant on every kind");

    // ── G4 · observation states (calibrating) ────────────────────────────────
    push(Some(r#"{"intent":"calibrateThenWrite"}"#), None, false, false, READY_SHORT,
        S(SR::ConfirmPending), S(SR::ConfirmPending), S(SR::ConfirmPending),
        "inside the window — held, and the status reports held/needed seconds");
    push(Some(r#"{"intent":"calibrateThenWrite"}"#), None, false, false, NOT_READY,
        S(SR::NotReady), S(SR::NotReady), S(SR::NotReady),
        "ACCIDENTAL shadow — the distinction six camelot bands needed and no surface could make");
    push(Some(r#"{"intent":"calibrateThenWrite"}"#), None, false, false, STALE,
        S(SR::Stale), S(SR::Stale), S(SR::Stale), "accidental — metric too old to trust");
    push(Some(r#"{"intent":"calibrateThenWrite"}"#), None, false, false, CONFLICT,
        S(SR::Conflict), S(SR::Conflict), S(SR::Conflict), "accidental — another field manager owns the target");
    push(Some(r#"{"intent":"calibrateThenWrite"}"#), None, false, false, NO_STATUS,
        S(SR::NotReady), S(SR::NotReady), S(SR::NotReady), "never observed ⇒ not ready, never a write");
    push(Some(r#"{"intent":"calibrateThenWrite"}"#), None, false, false, CONFIRMED,
        L(WK::OperatorAnnotation), L(WK::OperatorAnnotation), L(WK::OperatorAnnotation),
        "the breathe.pleme.io/confirmed fast-path short-circuits the window, and SAYS it did");

    // ── G5 · the external freeze key beats everything ────────────────────────
    push(Some(r#"{"intent":"write","authorizedBy":"drzzln"}"#), None, false, true, READY_LONG,
        S(SR::Frozen), S(SR::Frozen), S(SR::Frozen),
        "TWO-KEY RULE: a pool/fleet freeze outranks even a named human go-live");
    push(None, Some("effect"), false, true, READY_LONG,
        S(SR::Frozen), S(SR::Frozen), S(SR::Frozen), "…and outranks the retired `mode: effect` too");

    // ── G6 · precedence (the rows the doc-truth test cites) ──────────────────
    push(Some(r#"{"intent":"observe"}"#), Some("effect"), false, false, READY_LONG,
        S(SR::ModeShadow), S(SR::ModeShadow), S(SR::ModeShadow),
        "★ PROOF of `writeIntent` > `mode`: intent holds a band its `mode` says to carve");
    push(Some(r#"{"intent":"write","authorizedBy":"drzzln"}"#), Some("shadow"), false, false, READY_LONG,
        L(WK::ExplicitIntent), L(WK::ExplicitIntent), L(WK::ExplicitIntent),
        "★ PROOF the other way: intent carves a band its `mode` says to hold");
    push(Some(r#"{"intent":"write","authorizedBy":"drzzln"}"#), None, false, false, NOT_READY,
        L(WK::ExplicitIntent), L(WK::ExplicitIntent), L(WK::ExplicitIntent),
        "an explicit write does NOT wait on readiness — deliberate, and worth seeing stated");

    rows
}

/// Assemble a row's `spec` fragment.
fn row_spec(r: &Row) -> Value {
    let mut v = serde_json::Map::new();
    if let Some(i) = r.intent {
        v.insert("writeIntent".into(), serde_json::from_str(i).expect("row intent JSON"));
    }
    if let Some(m) = r.mode {
        v.insert("mode".into(), json!(m));
    }
    if r.dry_run {
        v.insert("dryRun".into(), json!(true));
    }
    Value::Object(v)
}

// ─────────────────────────────────── tests ────────────────────────────────────

/// **The composition matrix.** Every row × every one of the ten band kinds,
/// against a literal expected `(state, discriminant)` per class.
///
/// Failures aggregate before the assert — one run reports every broken cell, not
/// just the first (★★ CLOSED-LOOP MASS-SYNTHESIS).
#[test]
fn gate_composition_matrix() {
    let ks = kinds();
    let rows = gate_matrix();
    let mut failures: Vec<String> = Vec::new();
    let mut checked = 0usize;

    for (ri, r) in rows.iter().enumerate() {
        let spec = row_spec(r);
        for k in &ks {
            let want = match k.class {
                Class::Chain => r.chain,
                Class::TwoState => r.two_state,
                Class::Default => r.default,
            };
            let got = (k.probe)(&spec, r.obs, r.obs.now, r.frozen);
            checked += 1;
            if !want.matches(&got) {
                failures.push(format!(
                    "row {ri} [{}] intent={:?} mode={:?} dryRun={} frozen={} obs={}\n     \
                     want {want:?}, got {}\n     why: {}",
                    k.name, r.intent, r.mode, r.dry_run, r.frozen, r.obs.label, describe(&got), r.why
                ));
            }
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {checked} matrix cells disagree with the pinned contract.\n\n{}\n\n\
         Each cell is a LITERAL expectation a human wrote about what an authored spec shape \
         means. If a resolution change is intended, edit the cell AND its `why` — that edit \
         is the review signal `76924b0` never produced.",
        failures.len(),
        failures.join("\n\n")
    );
}

/// The compile-time guarantee ([`KINDS`]'s `[Kind; DimensionId::ALL.len()]`)
/// pins the fixture COUNT. This pins their IDENTITY — ten fixtures that all
/// probed `MemoryBand` would type-check and prove nothing.
///
/// Tier: the count is **truly-unrepresentable** (`E0308`); the identity is a
/// **CI forcing-function**. They are not the same tier and are stated apart.
#[test]
fn kind_fixtures_cover_every_dimension_exactly_once() {
    let ks = kinds();
    for d in DimensionId::ALL {
        let n = ks.iter().filter(|k| k.dim == d).count();
        assert_eq!(n, 1, "dimension `{d}` is probed {n}× — it must be exactly once, by exactly one fixture");
    }
    // And each fixture's DECLARED dimension is the one its CR actually reports,
    // so a copy-paste that left the wrong `dim` on a row cannot pass.
    for k in &ks {
        let actual = (k.report_dim)();
        assert_eq!(
            k.dim, actual,
            "{}: fixture declares dimension `{}` but the CR's own `dimension_id()` reports `{actual}` \
             — the fixture is probing a kind it is not labelled as",
            k.name, k.dim
        );
    }
}

/// The matrix is only a contract while it still covers the type it pins. Adding a
/// `ShadowReason`, `WitnessKind` or `PromotionMode` arm and leaving the matrix
/// alone fails here — and the exhaustive `match` label helpers below make adding
/// an arm without *naming* it `E0004`.
#[test]
fn gate_matrix_covers_every_discriminant() {
    // E0004 on a new arm — the label helpers are the compile-time half.
    fn mode_label(m: PromotionMode) -> &'static str {
        match m {
            PromotionMode::Shadow => "shadow",
            PromotionMode::Effect => "effect",
            PromotionMode::ShadowConfirmEffect => "shadowConfirmEffect",
            PromotionMode::Suspended => "suspended",
        }
    }
    fn reason_label(r: ShadowReasonKind) -> &'static str {
        match r {
            SR::Frozen => "frozen",
            SR::ModeShadow => "modeShadow",
            SR::Suspended => "suspended",
            SR::NotReady => "notReady",
            SR::Stale => "stale",
            SR::Conflict => "conflict",
            SR::IntentMalformed => "intentMalformed",
            SR::ConfirmPending => "confirmPending",
        }
    }
    fn witness_label(w: WitnessKind) -> &'static str {
        match w {
            WK::ExplicitIntent => "explicitIntent",
            WK::ConfirmGatePassed => "confirmGatePassed",
            WK::OperatorAnnotation => "operatorAnnotation",
            WK::LegacyDefault => "legacyDefault",
        }
    }

    let rows = gate_matrix();
    let expects: Vec<Expect> = rows.iter().flat_map(|r| [r.chain, r.two_state, r.default]).collect();

    // Every shadow reason is reachable from some row.
    for r in [SR::Frozen, SR::ModeShadow, SR::Suspended, SR::NotReady, SR::Stale, SR::Conflict, SR::IntentMalformed, SR::ConfirmPending] {
        assert!(
            expects.contains(&Expect::Shadow(r)),
            "no matrix row expects Shadow({}) — the reason is unpinned, so a change that \
             stops producing it (or starts producing it wrongly) is invisible",
            reason_label(r)
        );
    }
    // Every witness is reachable from some row.
    for w in [WK::ExplicitIntent, WK::ConfirmGatePassed, WK::OperatorAnnotation, WK::LegacyDefault] {
        assert!(
            expects.contains(&Expect::Live(w)),
            "no matrix row expects Live({}) — an authorization path nothing pins",
            witness_label(w)
        );
    }
    // Every `mode` arm is authored by some row, plus the unauthored case.
    let modes: Vec<Option<&str>> = rows.iter().map(|r| r.mode).collect();
    assert!(modes.contains(&None), "no row leaves `mode` unauthored — the default path is unpinned");
    for m in [PromotionMode::Shadow, PromotionMode::Effect, PromotionMode::ShadowConfirmEffect, PromotionMode::Suspended] {
        assert!(modes.contains(&Some(mode_label(m))), "no matrix row authors `mode: {}`", mode_label(m));
    }
    // Every intent arm, plus unauthored, plus the malformed case.
    let intents: Vec<Option<&str>> = rows.iter().map(|r| r.intent).collect();
    assert!(intents.contains(&None), "no row leaves `writeIntent` unauthored");
    for k in breathe_provider::gate::IntentKind::ALL {
        assert!(
            intents.iter().flatten().any(|i| i.contains(&format!("\"intent\":\"{}\"", k.as_str()))),
            "no matrix row authors `writeIntent.intent: {}`",
            k.as_str()
        );
    }
    assert!(intents.contains(&Some(r#"{"intent":"write"}"#)), "the malformed (unattributed) write is unpinned");
    // Both `dryRun` states and both freeze states.
    assert!(rows.iter().any(|r| r.dry_run) && rows.iter().any(|r| !r.dry_run), "`dryRun` is not varied");
    assert!(rows.iter().any(|r| r.frozen) && rows.iter().any(|r| !r.frozen), "the freeze key is not varied");
    // Every observation state.
    for o in ALL_OBS {
        assert!(rows.iter().any(|r| r.obs.label == o.label), "no matrix row uses observation state `{}`", o.label);
    }
}

/// **The fleet-coherence property.** Once an intent is authored, the resolution
/// never consults `promotion_mode()` — so all ten kinds MUST agree, and a new or
/// edited kind that diverges is caught here without needing its own row.
///
/// This is the generated (Layer A) half of the matrix: the cross product is swept
/// structurally rather than pinned cell-by-cell, and it covers the shapes the
/// literal table cannot afford to enumerate.
#[test]
fn an_authored_intent_makes_every_kind_agree() {
    let ks = kinds();
    let intents = [
        r#"{"intent":"observe"}"#,
        r#"{"intent":"frozen"}"#,
        r#"{"intent":"write","authorizedBy":"a"}"#,
        r#"{"intent":"write"}"#,
        r#"{"intent":"calibrateThenWrite"}"#,
        r#"{"intent":"calibrateThenWrite","confirmAfterSeconds":1}"#,
    ];
    let mut failures = Vec::new();
    for intent in intents {
        for mode in [None, Some("shadow"), Some("effect"), Some("shadowConfirmEffect"), Some("suspended")] {
            for dry_run in [false, true] {
                for frozen in [false, true] {
                    for obs in ALL_OBS {
                        let r = Row {
                            intent: Some(intent), mode, dry_run, frozen, obs,
                            chain: S(SR::Frozen), two_state: S(SR::Frozen), default: S(SR::Frozen), why: "",
                        };
                        let spec = row_spec(&r);
                        let mut seen: Vec<(&str, String)> = Vec::new();
                        for k in &ks {
                            seen.push((k.name, describe(&(k.probe)(&spec, obs, obs.now, frozen))));
                        }
                        let first = seen[0].1.clone();
                        if let Some((name, got)) = seen.iter().find(|(_, g)| *g != first) {
                            failures.push(format!(
                                "intent={intent} mode={mode:?} dryRun={dry_run} frozen={frozen} obs={}: \
                                 {} ⇒ {got} but {} ⇒ {first}",
                                obs.label, name, seen[0].0
                            ));
                        }
                    }
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} authored-intent shapes resolve DIFFERENTLY across band kinds. An authored \
         `writeIntent` must never depend on which dimension a band carves — if it does, one \
         kind has grown a private authorization rule.\n\n{}",
        failures.len(),
        failures.join("\n")
    );
}

// ───────────────────── the inert-field lint (both directions) ─────────────────

/// What a spec field is DECLARED to be, on a given kind, with respect to the
/// authorization axis. The lint checks the declaration against reality **in both
/// directions** — which is what makes it catch `76924b0`'s class.
#[derive(Clone, Copy, Debug)]
enum Status {
    /// Authoring this field MUST change the resolved gate somewhere. If it does
    /// not, the field has gone inert — the defect.
    Gates,
    /// Declared retired: authoring it must NEVER change the resolved gate. If it
    /// does, a retired field has come back to life — the *other* direction of
    /// the same defect, and the one that keeps MODULARIZE-DON'T-DELETE honest.
    Retired,
    /// Not a field of this kind's spec at all (serde ignores it; a structural
    /// CRD schema prunes it). Must have no effect.
    NotAField,
    /// A live field belonging to a DIFFERENT axis. Must have no effect *here*.
    OtherAxis(&'static str),
}

struct GateField {
    name: &'static str,
    /// The "authored" value; absence is the "unauthored" control.
    on: fn() -> Value,
    status: fn(Class) -> Status,
    note: &'static str,
}

fn gate_fields() -> Vec<GateField> {
    vec![
        GateField {
            name: "writeIntent",
            on: || json!({ "intent": "observe" }),
            status: |_| Status::Gates,
            note: "the authorization axis itself — inert on ANY kind would be a total failure",
        },
        GateField {
            name: "dryRun",
            on: || json!(true),
            // THE DECLARATION 76924b0 NEVER MADE. Before it, `dryRun` gated all
            // ten; after, only two. Had this registry existed, the commit would
            // have failed with "dryRun declared Gates on memory but is inert",
            // forcing the author to edit this line — the visible, reviewable act
            // that was missing.
            status: |c| match c {
                Class::TwoState => Status::Gates,
                Class::Chain | Class::Default => Status::Retired,
            },
            note: "RETIRED 2026-06-19 (breathe@76924b0) on 8 of 10 kinds; still live on the two-state pair",
        },
        GateField {
            name: "mode",
            on: || json!("shadow"),
            status: |c| match c {
                Class::Chain => Status::Gates,
                Class::TwoState | Class::Default => Status::NotAField,
            },
            note: "retired-but-read on the chain kinds; not a field at all on the other three",
        },
        GateField {
            name: "confirmAfterSeconds",
            on: || json!(1),
            status: |c| match c {
                Class::Chain => Status::Gates,
                Class::TwoState | Class::Default => Status::NotAField,
            },
            note: "sizes the legacy confirm window; the other kinds hardcode d_confirm_after()",
        },
        GateField {
            name: "suspend",
            on: || json!(true),
            status: |_| Status::OtherAxis("reconcile-at-all (breathe-controller/src/main.rs), not write-or-not"),
            note: "spec.suspend never reaches resolve_gate — outorga's Observation has no suspended(). \
                   `writeIntent: frozen` is the write-axis word; this is the D4 de-collision, mechanised",
        },
    ]
}

/// **The inert-field lint.** For every gate-bearing spec field × every band kind,
/// build two CRs differing ONLY in that field, sweep every observation state and
/// both freeze states, and check the declared status against what actually
/// happens.
///
/// The two-way check is the point. A one-way "every field must matter" lint would
/// itself be red today, because `dryRun` is *deliberately* inert on eight kinds —
/// so it would have been deleted or exempted, and the exemption would have been
/// the hiding place. Declaring retirement explicitly, and then proving the
/// retirement is real, is what makes the registry load-bearing instead of
/// decorative.
///
/// Tier: **CI forcing-function.** A field's status is a human declaration checked
/// by a test, not a type. What it buys is that *changing the declaration* is now
/// a required, visible part of any diff that changes the behaviour.
#[test]
fn every_gate_field_is_observable_or_declared_retired() {
    let ks = kinds();
    let fields = gate_fields();
    let mut failures = Vec::new();

    for f in &fields {
        for k in &ks {
            let off = json!({});
            let on = {
                let mut m = serde_json::Map::new();
                m.insert(f.name.to_string(), (f.on)());
                Value::Object(m)
            };

            // Where does authoring the field change the verdict?
            let mut differs: Vec<String> = Vec::new();
            for obs in ALL_OBS {
                for frozen in [false, true] {
                    let a = (k.probe)(&off, obs, obs.now, frozen);
                    let b = (k.probe)(&on, obs, obs.now, frozen);
                    if describe(&a) != describe(&b) {
                        differs.push(format!("obs={} frozen={frozen}: {} → {}", obs.label, describe(&a), describe(&b)));
                    }
                }
            }

            match (f.status)(k.class) {
                Status::Gates if differs.is_empty() => failures.push(format!(
                    "INERT: `spec.{}` is declared to GATE WRITES on {} ({:?}) but authoring it changes \
                     NOTHING across {} observation states × 2 freeze states.\n     \
                     This is exactly 76924b0's defect. Either restore the field's effect, or move it to \
                     `Status::Retired` in this file's registry — a declaration a reviewer can see.\n     \
                     registry note: {}",
                    f.name, k.name, k.class, ALL_OBS.len(), f.note
                )),
                Status::Retired | Status::NotAField | Status::OtherAxis(_) if !differs.is_empty() => {
                    let what = match (f.status)(k.class) {
                        Status::Retired => "RETIRED".to_string(),
                        Status::NotAField => "NOT A FIELD".to_string(),
                        Status::OtherAxis(a) => format!("on a DIFFERENT axis ({a})"),
                        Status::Gates => unreachable!(),
                    };
                    failures.push(format!(
                        "RESURRECTED: `spec.{}` is declared {what} on {} ({:?}), but authoring it CHANGES \
                         the resolved gate:\n       {}\n     \
                         A retired field that decides again is the same defect running backwards — the CRD \
                         description tells operators it does nothing. Update both, or revert.\n     \
                         registry note: {}",
                        f.name, k.name, k.class, differs.join("\n       "), f.note
                    ));
                }
                _ => {}
            }
        }
    }

    assert!(failures.is_empty(), "{} gate-field declarations disagree with reality:\n\n{}", failures.len(), failures.join("\n\n"));
}

// ───────────────────────────── the doc-truth check ────────────────────────────

/// Collapse whitespace so a doc-comment rewrap never breaks a truth check.
fn flat(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Pull `spec.properties.<name>.description` out of a generated CRD.
fn spec_desc(crd: &Value, prop: &str) -> Option<String> {
    let versions = crd.get("spec")?.get("versions")?.as_array()?;
    let schema = versions.first()?.get("schema")?.get("openAPIV3Schema")?;
    let d = schema.get("properties")?.get("spec")?.get("properties")?.get(prop)?.get("description")?;
    Some(flat(d.as_str()?))
}

/// **Doc-truth.** The CRD an operator actually reads with `kubectl explain` must
/// carry the canonical claim for its class.
///
/// # The chain, stated exactly
///
/// This test pins **text ≡ const**. `crd_descriptions` ⊇ `gate::CLAIM_*`. It does
/// **not** prove the const is true — no compiler checks English.
///
/// What makes the chain real for the ONE claim that matters (does `dryRun` gate
/// this kind?) is that three independent artifacts are driven by the same class
/// split, and this test asserts all three agree:
/// 1. the claim const chosen per kind (**this test**),
/// 2. `DimensionId::dry_run_is_honored()` (**asserted below**),
/// 3. the literal `chain`/`two_state`/`default` columns of [`gate_matrix`]
///    (**`gate_composition_matrix`**).
///
/// So a behaviour change that leaves the prose stale reddens the matrix, and a
/// prose change that leaves behaviour alone reddens this. For every *other*
/// sentence in these descriptions there is no such chain, and the honest
/// statement remains: **a doc string is not type-checkable.**
///
/// This test's own first act was to catch a false sentence: the six `band_kind!`
/// kinds claimed an unattributed `{intent: write}` "is rejected at parse time".
/// It is not — a k8s structural schema cannot express a conditional `required`,
/// so it is held at runtime as `intentMalformed`. The prose was corrected rather
/// than pinned.
#[test]
fn crd_descriptions_carry_the_canonical_claims() {
    use breathe_provider::gate::{
        CLAIM_DRY_RUN_INERT, CLAIM_DRY_RUN_LIVE, CLAIM_RESOLUTION_ORDER, CLAIM_UNATTRIBUTED_WRITE,
        RETIREMENT_NOTICE_DRY_RUN,
    };

    let crds: Vec<(&str, Class, DimensionId, Value)> = vec![
        ("MemoryBand", Class::Chain, DimensionId::Memory, serde_json::to_value(MemoryBand::crd()).unwrap()),
        ("CpuBand", Class::Chain, DimensionId::Cpu, serde_json::to_value(CpuBand::crd()).unwrap()),
        ("StorageBand", Class::Chain, DimensionId::Storage, serde_json::to_value(StorageBand::crd()).unwrap()),
        ("ArcBand", Class::Chain, DimensionId::Arc, serde_json::to_value(ArcBand::crd()).unwrap()),
        ("CgroupBand", Class::Chain, DimensionId::Cgroup, serde_json::to_value(CgroupBand::crd()).unwrap()),
        ("CgroupCpuBand", Class::Chain, DimensionId::CgroupCpu, serde_json::to_value(CgroupCpuBand::crd()).unwrap()),
        ("ReplicaBand", Class::Chain, DimensionId::Replica, serde_json::to_value(ReplicaBand::crd()).unwrap()),
        ("HostParamBand", Class::TwoState, DimensionId::HostParam, serde_json::to_value(HostParamBand::crd()).unwrap()),
        ("KubeParamBand", Class::TwoState, DimensionId::KubeParam, serde_json::to_value(KubeParamBand::crd()).unwrap()),
        ("AppBand", Class::Default, DimensionId::AppParam, serde_json::to_value(AppBand::crd()).unwrap()),
        ("RequestBand", Class::Chain, DimensionId::Request, serde_json::to_value(RequestBand::crd()).unwrap()),
    ];
    assert_eq!(crds.len(), N_KINDS, "every band kind must be doc-checked");

    let claim_inert = flat(CLAIM_DRY_RUN_INERT);
    let claim_live = flat(CLAIM_DRY_RUN_LIVE);
    let claim_order = flat(CLAIM_RESOLUTION_ORDER);
    let claim_unattributed = flat(CLAIM_UNATTRIBUTED_WRITE);
    let retirement = flat(RETIREMENT_NOTICE_DRY_RUN);
    let mut failures = Vec::new();

    for (name, class, dim, crd) in &crds {
        let dry = spec_desc(crd, "dryRun")
            .unwrap_or_else(|| panic!("{name}: spec.dryRun must exist and be documented"));
        let wi = spec_desc(crd, "writeIntent")
            .unwrap_or_else(|| panic!("{name}: spec.writeIntent must exist and be documented"));

        // (2) of the chain: the flag and the class split are the same fact.
        assert_eq!(
            dim.dry_run_is_honored(),
            *class == Class::TwoState,
            "{name}: DimensionId::dry_run_is_honored()={} but its resolution class is {class:?} — \
             the flag every operator surface reads disagrees with the behaviour the matrix pins",
            dim.dry_run_is_honored()
        );

        match class {
            Class::TwoState => {
                if !dry.contains(&claim_live) {
                    failures.push(format!("{name}: `dryRun` DOES gate this kind, but its description omits {CLAIM_DRY_RUN_LIVE:?}"));
                }
                if dry.contains(&claim_inert) {
                    failures.push(format!(
                        "{name}: description claims {CLAIM_DRY_RUN_INERT:?} — FALSE here. A blanket \
                         \"dryRun is retired\" is the exact opposite lie to the one 76924b0 left behind"
                    ));
                }
            }
            Class::Chain | Class::Default => {
                if !dry.contains(&claim_inert) {
                    failures.push(format!("{name}: `dryRun` is inert here, but its description omits {CLAIM_DRY_RUN_INERT:?}"));
                }
                if !dry.contains(&retirement) {
                    failures.push(format!("{name}: `dryRun`'s description omits the retirement stamp {RETIREMENT_NOTICE_DRY_RUN:?}"));
                }
            }
        }

        // Every kind must tell an operator what an unattributed go-live does.
        if !flat(&format!("{dry} {wi}")).contains(&claim_unattributed) {
            failures.push(format!("{name}: no description states what an unattributed `{{intent: write}}` does"));
        }

        // Kinds that HAVE a `mode` field must state the order; kinds that do not
        // must not have the property at all (the matrix's `NotAField` rows).
        let has_mode = spec_desc(crd, "mode").is_some()
            || crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]["properties"]
                .get("mode")
                .is_some();
        match class {
            Class::Chain => {
                if !has_mode {
                    failures.push(format!("{name}: a chain kind must carry `spec.mode`"));
                }
                if !flat(&format!("{dry} {wi}")).contains(&claim_order) {
                    failures.push(format!("{name}: carries `spec.mode` but never states {CLAIM_RESOLUTION_ORDER:?}"));
                }
            }
            Class::TwoState | Class::Default => {
                if has_mode {
                    failures.push(format!(
                        "{name}: carries `spec.mode` in its schema, but its `mode_spec()` is hardcoded \
                         `None` — an operator could author a field the controller cannot read"
                    ));
                }
            }
        }
    }

    assert!(failures.is_empty(), "{} CRD description(s) disagree with behaviour:\n  - {}", failures.len(), failures.join("\n  - "));
}

// ═══════════════════════════════════════════════════════════════════════════
// THE WRITE-SURFACE CENSUS — the guard for kinds OUTSIDE `DimensionId::ALL`.
// ═══════════════════════════════════════════════════════════════════════════

/// What a CRD kind is, with respect to writing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SurfaceRole {
    /// A kind whose controller mutates something (a cluster object, a cgroup
    /// file, a cloud node). MUST carry `status.effectiveGate`.
    WriteSurface,
    /// A kind that HOLDS a master write key for OTHER kinds but writes nothing
    /// itself (`BreatheNodePool.spec.writeEnabled` is read by host bands and by
    /// the `PodMemoryHigh` dispatch; the pool has no write of its own).
    KeyHolder,
    /// A kind whose `dryRun` marks its PUBLISHED LEDGER advisory-vs-enforced for
    /// a DOWNSTREAM consumer, while breathe itself mutates nothing.
    /// `QuinhaoPool` divides a band's capacity into per-claimant grants and
    /// publishes them in `status.grants`; the `StorageBand` holds the real limit,
    /// and gaveta decides whether to honour a grant. Breathe never reaches
    /// `Cluster::apply` on this path, so there is no authorization verdict to
    /// report — the flag is a hint to someone else's write, not a gate on ours.
    ///
    /// Found by this census on its first run, which is the point: it carries a
    /// `dryRun` and was in neither of the two roles above. If breathe ever carves
    /// from a grant, this row is wrong and must become a `WriteSurface`.
    AdvisoryLedger,
}

/// **Every CRD kind that carries a write gate, named.**
///
/// # The hole this closes
///
/// The fleet's kind vocabulary is `DimensionId::ALL`, a `[Self; 10]` whose
/// length makes an eleventh *dimension* a compile error. But
/// `PodMemoryHigh` — a real cgroup write on every node — **is not a
/// `DimensionId` at all.** Neither are `BreatheCloudPool` (which provisions and
/// deprovisions cloud nodes) or `IsolationBand`. The ten-arm guard is
/// structurally blind to them: no arm to add, no `E0004` to fire. That is
/// exactly how `PodMemoryHigh` reached a real host write behind a bare
/// `do_write` bool with no witness on the path, unnoticed.
///
/// So the census is keyed on the thing these kinds DO have in common — a
/// `dryRun` and/or `writeEnabled` field in their spec — read out of the
/// **generated CRD schema**, not out of a hand-kept list. A new gated kind
/// appears in `crdgen`'s output the moment it is written, and if it is not
/// named here the test fails.
///
/// # Tier (not rounded up)
///
/// **CI-caught, not a compile error.** Rust cannot enumerate "things that
/// call `Cluster::apply`", and a schema walk is a test, not a type. What IS a
/// compile error is the layer below: `Cluster::apply` takes a `&LiveWitness`,
/// so none of these kinds can reach a write without an authorization verdict.
/// This census pins the *observability* half — that every write surface
/// declares its verdict where an operator can read it.
///
/// It also cannot see a write surface with **no CRD at all** (a controller that
/// mutates on a timer, say). None exists today; if one is added, this list is
/// the wrong shape for it and the honest move is to say so rather than to
/// quietly not cover it.
const WRITE_SURFACE_CENSUS: &[(&str, SurfaceRole)] = &[
    // The ten `DimensionId` kinds — also covered by `kinds()`'s `[Kind; 10]`.
    ("MemoryBand", SurfaceRole::WriteSurface),
    ("CpuBand", SurfaceRole::WriteSurface),
    ("StorageBand", SurfaceRole::WriteSurface),
    ("ReplicaBand", SurfaceRole::WriteSurface),
    ("ArcBand", SurfaceRole::WriteSurface),
    ("CgroupBand", SurfaceRole::WriteSurface),
    ("CgroupCpuBand", SurfaceRole::WriteSurface),
    ("HostParamBand", SurfaceRole::WriteSurface),
    ("KubeParamBand", SurfaceRole::WriteSurface),
    ("AppBand", SurfaceRole::WriteSurface),
    // A write surface by DECLARATION, not yet by behaviour: the request
    // actuation is unwired (`KubeCluster::apply`'s `PodRequestResize` arm returns
    // a typed permanent error, and no controller watches this kind). It is
    // enrolled here anyway, because the census's job is to catch a kind that can
    // write without declaring a verdict — and enrolling it now means the day the
    // actuation lands, nothing has to remember to come back here.
    ("RequestBand", SurfaceRole::WriteSurface),
    // …and the three that are NOT dimensions, which is the whole point.
    ("PodMemoryHigh", SurfaceRole::WriteSurface),
    ("BreatheCloudPool", SurfaceRole::WriteSurface),
    ("IsolationBand", SurfaceRole::WriteSurface),
    // Holds the key, turns none itself.
    ("BreatheNodePool", SurfaceRole::KeyHolder),
    // Publishes an advisory ledger; breathe writes nothing.
    ("QuinhaoPool", SurfaceRole::AdvisoryLedger),
];

/// Every CRD `crdgen` emits, as `(kind, schema)`.
fn all_crds() -> Vec<(String, Value)> {
    // `vec!`, not an array: 19 `CustomResourceDefinition`s on the stack trips
    // clippy's `large_stack_arrays`.
    let raw = vec![
        MemoryBand::crd(),
        CpuBand::crd(),
        StorageBand::crd(),
        ReplicaBand::crd(),
        ArcBand::crd(),
        CgroupBand::crd(),
        CgroupCpuBand::crd(),
        HostParamBand::crd(),
        KubeParamBand::crd(),
        AppBand::crd(),
        RequestBand::crd(),
        BreatheNodePool::crd(),
        BreathePosture::crd(),
        PodMemoryHigh::crd(),
        BreatheCloudPool::crd(),
        IsolationBand::crd(),
        BreatheOverview::crd(),
        BreatheConfig::crd(),
        Densa::crd(),
        QuinhaoPool::crd(),
    ];
    raw.into_iter()
        .map(|c| {
            let v = serde_json::to_value(&c).expect("serialize CRD");
            let kind = v["spec"]["names"]["kind"].as_str().expect("kind").to_owned();
            (kind, v)
        })
        .collect()
}

fn schema_of<'a>(crd: &'a Value, section: &str) -> Option<&'a Value> {
    crd.get("spec")?.get("versions")?.as_array()?.first()?.get("schema")?.get("openAPIV3Schema")?
        .get("properties")?.get(section)?.get("properties")
}

fn has_prop(crd: &Value, section: &str, prop: &str) -> bool {
    schema_of(crd, section).is_some_and(|p| p.get(prop).is_some())
}

/// **No write surface is invisible.** Every generated CRD whose spec carries a
/// write gate (`dryRun` / `writeEnabled`) must be named in
/// [`WRITE_SURFACE_CENSUS`] — so an eleventh write surface cannot be added
/// outside `DimensionId::ALL` and go unnoticed the way `PodMemoryHigh` did.
#[test]
fn every_write_gated_crd_is_named_in_the_census() {
    let mut gated: Vec<String> = Vec::new();
    for (kind, crd) in all_crds() {
        if has_prop(&crd, "spec", "dryRun") || has_prop(&crd, "spec", "writeEnabled") {
            gated.push(kind);
        }
    }
    gated.sort();

    let mut named: Vec<String> = WRITE_SURFACE_CENSUS.iter().map(|(k, _)| (*k).to_owned()).collect();
    named.sort();

    assert_eq!(
        gated, named,
        "the set of write-gated CRDs and the census disagree.\n  \
         generated + gated: {gated:?}\n  named in census:   {named:?}\n\
         A kind that carries `dryRun`/`writeEnabled` MUTATES something. Add it to \
         WRITE_SURFACE_CENSUS (and give it a `status.effectiveGate`), or remove its gate."
    );
}

/// **Every write surface declares its verdict.** A kind that can write must say,
/// in its own status, whether it is writing and why — the single legible verdict
/// the authorization refactor exists to produce.
///
/// `node_forma` and `origin_guard` wrote ONLY the legacy `effectiveDryRun` bool
/// until 2026-07-26, and `PodMemoryHigh` wrote neither; this is the test that
/// keeps that from recurring.
#[test]
fn every_write_surface_status_carries_the_typed_gate() {
    let crds: std::collections::BTreeMap<String, Value> = all_crds().into_iter().collect();
    let mut missing = Vec::new();
    for (kind, role) in WRITE_SURFACE_CENSUS {
        let crd = crds.get(*kind).unwrap_or_else(|| panic!("{kind} is in the census but crdgen does not emit it"));
        match role {
            SurfaceRole::WriteSurface => {
                if !has_prop(crd, "status", "effectiveGate") {
                    missing.push(*kind);
                }
            }
            // Neither of these writes, so neither has a verdict of its own to
            // report. Asserted, not assumed: if one ever grows an effectiveGate
            // it is really a write surface, and the census row is wrong.
            SurfaceRole::KeyHolder | SurfaceRole::AdvisoryLedger => {
                assert!(
                    !has_prop(crd, "status", "effectiveGate"),
                    "{kind} is classified {role:?} (breathe writes nothing on this path) but reports an \
                     effectiveGate — reclassify it as a WriteSurface"
                );
            }
        }
    }
    assert!(
        missing.is_empty(),
        "{} write surface(s) report no typed authorization verdict: {missing:?} \
         — an operator cannot ask these CRs 'are you writing, and why'",
        missing.len()
    );
}

/// **`legacy_two_state_gate` is byte-identical to the bool it replaces.** The
/// Tier-B kinds' two-key rule (`dryRun` selects shadow-vs-effect; `frozen` is the
/// pool master switch and overrides everything) must survive being retyped from
/// a `bool` to an `EffectiveGate`, or this refactor changed live behaviour on
/// `BreatheCloudPool` / `IsolationBand` / `PodMemoryHigh` while claiming not to.
#[test]
fn legacy_two_state_gate_reproduces_the_bool_truth_table() {
    for dry_run in [true, false] {
        for frozen in [true, false] {
            let typed = breathe_provider::legacy_two_state_gate(dry_run, frozen);
            let old = breathe_crd::legacy_effective_dry_run(dry_run, frozen);
            assert_eq!(
                typed.is_shadow(),
                old.is_shadow(),
                "legacy_two_state_gate({dry_run}, {frozen}) disagrees with the bool it replaced"
            );
            // …and the SHADOW arm is still exactly `dry_run || frozen`.
            assert_eq!(typed.is_shadow(), dry_run || frozen);

            match (&typed, dry_run, frozen) {
                // The freeze key wins, and says so — better attribution than the
                // bool ever had.
                (EffectiveGate::Shadow { reason }, _, true) => {
                    assert_eq!(reason.kind(), ShadowReasonKind::Frozen);
                }
                (EffectiveGate::Shadow { reason }, true, false) => {
                    assert_eq!(reason.kind(), ShadowReasonKind::ModeShadow);
                }
                // A live Tier-B write is HONESTLY reported as migration debt:
                // these kinds carry no `spec.writeIntent` yet, so every write
                // they make rests on a pre-2026-07 path.
                (EffectiveGate::Live { witness }, false, false) => {
                    assert_eq!(witness.kind(), WitnessKind::LegacyDefault);
                    assert!(witness.is_legacy_default(), "a Tier-B write is burn-down debt and must report as such");
                    assert_eq!(witness.legacy_path().map(LegacyPath::kind), Some(LegacyPathKind::TwoStateDryRun));
                }
                (g, d, f) => panic!("unexpected verdict for (dry_run={d}, frozen={f}): {g:?}"),
            }
        }
    }
}

/// `authored_write_gate` is a thin wrapper over `resolve_gate`, and refuses an
/// unattributed write exactly the way the parse boundary does — so the
/// convenience cannot become a back door.
#[test]
fn authored_write_gate_names_its_authority_or_refuses() {
    let live = breathe_provider::authored_write_gate("drzzln@2026-07-26");
    match &live {
        EffectiveGate::Live { witness } => {
            assert_eq!(witness.kind(), WitnessKind::ExplicitIntent);
            assert_eq!(witness.authorized_by(), Some("drzzln@2026-07-26"));
            assert!(!witness.is_legacy_default(), "an authored write is not migration debt");
        }
        g @ EffectiveGate::Shadow { .. } => panic!("an authored write must resolve live, got {g:?}"),
    }
    // Blank / whitespace-only authority is an unattributed write: held, never granted.
    for blank in ["", "   ", "\t"] {
        match breathe_provider::authored_write_gate(blank) {
            EffectiveGate::Shadow { reason } => assert_eq!(reason.kind(), ShadowReasonKind::IntentMalformed),
            g @ EffectiveGate::Live { .. } => panic!("a blank authority must NOT authorize a write, got {g:?}"),
        }
    }
}
