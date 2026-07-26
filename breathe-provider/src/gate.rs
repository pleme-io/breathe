//! The AUTHORIZATION axis (A1) — **one authored intent, one derived verdict,
//! one unforgeable witness.**
//!
//! # Why this module exists
//!
//! Before it, "may this band write?" was answered by FOUR contested fields —
//! `spec.dryRun`, `spec.mode`, `spec.suspend`, and a node's `writeEnabled` —
//! whose composition changed silently over time. On 2026-06-19 (`76924b0`)
//! `Band::promotion_mode` was rewritten from
//!
//! ```text
//! None if self.dry_run() => Shadow,   None => ShadowConfirmEffect
//! ```
//!
//! to `self.mode_spec().unwrap_or(ShadowConfirmEffect)`. The intent was
//! defensible (kill "parked in permanent shadow with no exit"), but the effect
//! **inverted the meaning of `spec.dryRun` on every live CR** with no schema
//! change, no migration and no doc update. `dryRun: true` — authored on 74 of
//! camelot-eks's 76 bands as an explicit safety declaration — became dead code,
//! and ~4,045 real carves ran under it.
//!
//! The defect class is not "a bad boolean". It is: **the authorization decision
//! had no name, no type, and no witness**, so nobody could see it change.
//!
//! # The shape
//!
//! * [`WriteIntent`] — what the AUTHOR declared. Deliberately has **no
//!   `Default` impl**: choosing an interpretation for an unauthored
//!   authorization field is precisely how `76924b0` happened.
//! * [`EffectiveGate`] — what the CONTROLLER derived this tick, written to
//!   status so `kubectl get` answers "is this band writing, and why".
//! * [`LiveWitness`] — **the unforgeable evidence that a write is authorized.**
//!
//! ## The load-bearing move
//!
//! [`LiveWitness`] is a newtype over a **private** enum whose only constructors
//! are `pub(crate)`. The sole path to one from outside this crate is
//! [`resolve_gate`]. Therefore, once `ResourceProvider::assign` takes a
//! `&LiveWitness` (stage S2), **carving a shadowed band is a compile error**,
//! not a runtime check — `EffectiveGate::Shadow` has no field of that type, so
//! the shadow arm has nothing to pass.
//!
//! (Note for reviewers: Rust has no per-variant field privacy — the fields of a
//! `pub enum`'s variants are always public. A public enum could therefore be
//! fabricated by any crate. The newtype-over-private-enum is what actually buys
//! the guarantee; a bare `pub enum LiveWitness { .. }` would not.)
//!
//! ## What is NOT claimed
//!
//! This module does not make an unwise write impossible — a human can author
//! `intent: write` on a database and mean it. It makes the state **legible and
//! attributable**: every live band names *who or what* authorized it, and every
//! shadowed band names *why* it is held.
//!
//! ## No forked algebra
//!
//! The confirm-gate math (has this band held Ready ∧ ¬Stale ∧ ¬Conflict long
//! enough?) lives in `outorga::PromotionPolicy` and is NOT re-implemented here.
//! [`resolve_gate`] consumes its verdict ([`ConfirmVerdict`]) and its legacy
//! decision ([`LegacyDecision`]) as **inputs**, computed by the caller
//! (`breathe-crd`, which owns the `outorga` dependency). That keeps this crate
//! `outorga`-free *and* keeps one tested FSM in the fleet.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The operator fast-path annotation: `breathe.pleme.io/confirmed: "true"`
/// satisfies a calibrating band's confirm gate immediately.
pub const CONFIRMED_ANNOTATION: &str = "breathe.pleme.io/confirmed";

// ───────────────────────── the canonical claims (doc-truth) ─────────────────────────
//
// Each const below is the ONE authoritative wording of a claim the CRD makes to
// operators about this axis. `breathe-crd/tests/gate_matrix.rs`'s
// `crd_descriptions_carry_the_canonical_claims` generates the real CRD schema and
// asserts each kind's descriptions carry the right claim for its class.
//
// TIER — read this before citing these as a guarantee. The test pins
// **text ≡ const**. It does NOT prove **const ≡ truth**: no compiler checks that
// English prose describes Rust. What makes the chain real for the ONE claim that
// matters most (does `dryRun` gate this kind?) is that the same class split drives
// three independent things — the const chosen per kind here, the
// `DimensionId::dry_run_is_honored()` flag, and the literal expectations in
// `GATE_MATRIX` — so a behavioural change that leaves the prose stale fails the
// matrix, and a prose change that leaves behaviour alone fails this test. For
// every OTHER sentence in these descriptions there is no such chain, and the
// honest statement is: a doc string is not type-checkable.

/// The retirement stamp every kind that ignores `spec.dryRun` must carry.
pub const RETIREMENT_NOTICE_DRY_RUN: &str = "RETIRED 2026-06-19 (breathe@76924b0)";

/// The claim an INERT-`dryRun` kind's description must make (8 of 10 kinds).
pub const CLAIM_DRY_RUN_INERT: &str = "this field has NO effect on this band kind";

/// The claim a LIVE-`dryRun` kind's description must make — and which an inert
/// kind's must NOT (`HostParamBand` / `KubeParamBand` only).
pub const CLAIM_DRY_RUN_LIVE: &str = "actually read `dryRun`";

/// The resolution order, stated once. Every kind whose spec carries a `mode`
/// field must say this somewhere in its schema.
pub const CLAIM_RESOLUTION_ORDER: &str = "`writeIntent` > `mode` > the compiled `shadowConfirmEffect`";

/// The claim every kind must make about an unattributed go-live.
///
/// This const exists because the previous wording on the six `band_kind!` kinds
/// was **false**: it said such a CR "is rejected at parse time". It is not — the
/// cross-field rule is not expressible in a k8s structural schema, so the
/// apiserver accepts it and [`resolve_gate`] holds it as
/// [`ShadowReason::IntentMalformed`]. Fail-safe and visible, but *mitigated at
/// runtime*, not *parse-time-rejected*. Rounding that up is how the whole
/// `dryRun` class was born; the honest sentence is pinned here instead.
pub const CLAIM_UNATTRIBUTED_WRITE: &str =
    "an `{intent: write}` naming no `authorizedBy` never goes live: it is held in shadow as `intentMalformed`";

fn d_confirm_after_seconds() -> u64 {
    1800
}

// ───────────────────────────── the authored intent ─────────────────────────────

/// **The authored authorization intent for one band** — the single value that
/// replaces the `dryRun` / `mode` / `suspend` triangle. This is the *parsed*
/// algebra; [`WriteIntentSpec`] is its wire shape.
///
/// Deliberately carries **no `Default` impl**. Absence is resolved by the
/// documented migration chain (`writeIntent` > `mode` > the compiled
/// `ShadowConfirmEffect`) during stages S1–S5, and becomes a required field
/// afterwards. A silent default on an authorization field is the exact footgun
/// this type exists to remove.
///
/// Illegal combinations are unrepresentable *here* — a `Write` always carries
/// an author — and are rejected at the [`WriteIntentSpec::parse`] boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriteIntent {
    /// Decide, report and attest — **never write.** The honest shadow: an
    /// explicit, eyes-open hold for a critical-path workload.
    Observe,
    /// Shadow until a clean-observation window proves the band safe, then
    /// write. The bounded calibration `76924b0` correctly wanted as the
    /// default — stated out loud instead of inferred.
    CalibrateThenWrite {
        /// Seconds the band must hold Ready ∧ ¬Stale ∧ ¬Conflict before it
        /// promotes itself to writing.
        confirm_after_seconds: u64,
    },
    /// Write now, on this named authority.
    Write {
        /// Who authorized this, and ideally when and why. Free text, carried
        /// verbatim into [`LiveWitness`] and into status. Never empty — see
        /// [`WriteIntentSpec::parse`].
        authorized_by: String,
    },
    /// Never write, but **keep observing.** Folds `mode: suspended` and a node
    /// pool's `writeEnabled: false` into one word. Distinct from
    /// `spec.suspend`, which stops reconciling the band entirely.
    Frozen,
}

impl WriteIntent {
    /// `true` iff this intent can ever authorize a write (on its own — an
    /// external freeze still overrides).
    #[must_use]
    pub fn may_write(&self) -> bool {
        matches!(self, Self::Write { .. } | Self::CalibrateThenWrite { .. })
    }
}

/// The four intents, as a plain string enum.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum IntentKind {
    /// Never write; decide, report and attest.
    Observe,
    /// Shadow until a clean-observation window elapses, then write.
    CalibrateThenWrite,
    /// Write now. Requires `authorizedBy`.
    Write,
    /// Never write, but keep observing.
    Frozen,
}

impl IntentKind {
    /// Every intent arm. Kept total by [`IntentKind::as_str`]'s exhaustive
    /// match (a new arm is `E0004` there) plus `intent_kind_all_is_total`.
    ///
    /// Exists because `IntentKind` is BOTH a CRD field (schemars 0.8, via kube)
    /// and an MCP tool input (schemars 1.x, via rmcp), and one type cannot carry
    /// both derives. Rather than mirror a second four-arm enum in `breathe-mcp` —
    /// the duplication that let the old five-arm `BandKind` fall behind — the MCP
    /// surface builds its input schema from this list.
    pub const ALL: [Self; 4] = [Self::Observe, Self::CalibrateThenWrite, Self::Write, Self::Frozen];

    /// The wire label, identical to the serde `camelCase` rename (pinned by
    /// `intent_kind_labels_match_serde`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Observe => "observe",
            Self::CalibrateThenWrite => "calibrateThenWrite",
            Self::Write => "write",
            Self::Frozen => "frozen",
        }
    }
}

/// The **wire shape** of [`WriteIntent`] — the shape that actually goes in a
/// CRD.
///
/// It is a flat struct, not a serde-tagged enum, and that is a *forced,
/// verified* choice rather than a stylistic one: `kube-core`'s structural-schema
/// hoister (`schema.rs`) panics on an internally-tagged enum, because k8s
/// structural schemas require every property used inside `oneOf` branches to
/// carry an *identical* schema outside them — which a discriminant, by
/// definition, cannot. This was confirmed by running `crdgen`, not assumed.
///
/// The cost is that the cross-field rule "`write` implies an author" is not
/// expressible in the generated OpenAPI schema, so the apiserver will accept a
/// malformed value. It is caught one layer in, at [`Self::parse`], and
/// surfaced as [`ShadowReason::IntentMalformed`] — which **fails safe** (never
/// writes) and stays visible in status.
///
/// Deliberately *not* enforced by making deserialization fail: a `kube` watcher
/// decodes a whole list at once, so one malformed band would break the watch
/// stream for every other band. Shadow-and-report is both safer and more
/// legible than a stalled controller. (Making the apiserver itself reject it —
/// a hand-written `anyOf` of `required` clauses, which *is* structural-legal —
/// is a named S4 item, not silently skipped.)
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WriteIntentSpec {
    /// `observe` | `calibrateThenWrite` | `write` | `frozen`.
    pub intent: IntentKind,
    /// Only meaningful for `calibrateThenWrite`; defaults to 1800s.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirm_after_seconds: Option<u64>,
    /// **Required for `write`** — who authorized going live, and ideally when
    /// and why. An empty or absent value on a `write` is rejected by
    /// [`Self::parse`]; an unattributed go-live is never granted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorized_by: Option<String>,
}

/// Why an authored `writeIntent` could not be parsed into a [`WriteIntent`].
/// Every variant resolves to a fail-safe shadow, never to a write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntentError {
    /// `{intent: write}` with no (or blank) `authorizedBy`.
    UnattributedWrite,
}

impl WriteIntentSpec {
    /// Parse the wire shape into the algebra, rejecting the one combination
    /// the flat schema cannot forbid on its own.
    ///
    /// # Errors
    /// [`IntentError::UnattributedWrite`] when `intent: write` names no author.
    pub fn parse(&self) -> Result<WriteIntent, IntentError> {
        match self.intent {
            IntentKind::Observe => Ok(WriteIntent::Observe),
            IntentKind::Frozen => Ok(WriteIntent::Frozen),
            IntentKind::CalibrateThenWrite => Ok(WriteIntent::CalibrateThenWrite {
                confirm_after_seconds: self.confirm_after_seconds.unwrap_or_else(d_confirm_after_seconds),
            }),
            IntentKind::Write => match self.authorized_by.as_deref().map(str::trim) {
                Some(a) if !a.is_empty() => Ok(WriteIntent::Write { authorized_by: a.to_owned() }),
                _ => Err(IntentError::UnattributedWrite),
            },
        }
    }
}

// ───────────────────────── the derived shadow reason ──────────────────────────

/// **Why a band is held in shadow this tick.** A lossless superset of
/// `outorga::ShadowReason`: its seven arms mirrored one-to-one (same names,
/// same meaning) plus [`Self::IntentMalformed`], which `outorga` has no notion
/// of because it never parses a CRD field. The mapping
/// `breathe_crd::map_shadow_reason` is therefore total and injective in the
/// direction that matters (outorga → here); the extra arm is only ever
/// produced by [`resolve_gate`].
///
/// This type exists separately from `outorga`'s because a CRD status field
/// needs the `JsonSchema` derive, which `outorga`'s deliberately does not
/// carry — the same already-blessed split as `PromotionMode::to_outorga`.
///
/// This is what makes an *accidentally* shadowed band distinguishable from a
/// *deliberately* shadowed one — the distinction that mattered on camelot-eks,
/// where six bands were shadowed by `NotReady`, not by any authored intent, and
/// every surface reported the cause as "dryRun".
///
/// Rich (it carries the confirm window's numbers) and therefore **not** a CRD
/// type: `kube-core`'s structural-schema hoister rejects any serde-tagged enum
/// whose variants carry differing data — verified by running `crdgen`, twice.
/// [`EffectiveGateReport`] is its flat, k8s-safe projection; [`Self::kind`] is
/// the discriminant that goes on the wire.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "reason")]
pub enum ShadowReason {
    /// An external freeze (pool / fleet master write switch) is engaged. This
    /// overrides every intent — the two-key rule.
    Frozen,
    /// Authored shadow: `writeIntent: observe`, or the retired `mode: shadow`.
    ModeShadow,
    /// Authored freeze: `writeIntent: frozen`, or the retired `mode: suspended`.
    Suspended,
    /// The band is not Ready (no metric / not yet observed). **Accidental**
    /// shadow, not an authored one.
    NotReady,
    /// The driving metric sample is too old to trust. Accidental shadow.
    Stale,
    /// Another field manager owns the target. Accidental shadow.
    Conflict,
    /// The authored `writeIntent` does not parse — today, `{intent: write}`
    /// naming no author. **Fails safe** (never writes) and stays visible,
    /// rather than breaking the watch stream on a deserialize error or
    /// silently granting an anonymous write.
    IntentMalformed,
    /// Calibrating, and the clean-observation window has not yet elapsed.
    #[serde(rename_all = "camelCase")]
    ConfirmPending {
        /// Seconds held Ready-and-healthy so far.
        held_secs: i64,
        /// Seconds required before promotion.
        need_secs: i64,
    },
}

/// The flat, k8s-safe discriminant of a [`ShadowReason`] — what an operator
/// reads in the `Why` printcolumn. The reason's numbers ride alongside it on
/// [`EffectiveGateReport`] rather than nested inside it, because a CRD schema
/// cannot express a tagged union.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ShadowReasonKind {
    /// An external freeze (pool / fleet master switch).
    Frozen,
    /// Authored shadow (`writeIntent: observe`, or the retired `mode: shadow`).
    ModeShadow,
    /// Authored freeze (`writeIntent: frozen`, or the retired `mode: suspended`).
    Suspended,
    /// Not Ready — an ACCIDENTAL shadow, not an authored one.
    NotReady,
    /// The metric sample is too old to trust — accidental.
    Stale,
    /// Another field manager owns the target — accidental.
    Conflict,
    /// The authored `writeIntent` does not parse; failing safe.
    IntentMalformed,
    /// Calibrating; the clean-observation window has not yet elapsed.
    ConfirmPending,
}

impl ShadowReason {
    /// The wire discriminant.
    #[must_use]
    pub fn kind(self) -> ShadowReasonKind {
        match self {
            Self::Frozen => ShadowReasonKind::Frozen,
            Self::ModeShadow => ShadowReasonKind::ModeShadow,
            Self::Suspended => ShadowReasonKind::Suspended,
            Self::NotReady => ShadowReasonKind::NotReady,
            Self::Stale => ShadowReasonKind::Stale,
            Self::Conflict => ShadowReasonKind::Conflict,
            Self::IntentMalformed => ShadowReasonKind::IntentMalformed,
            Self::ConfirmPending { .. } => ShadowReasonKind::ConfirmPending,
        }
    }
    /// `(held, needed)` seconds, when this reason is a pending confirm window.
    #[must_use]
    pub fn window(self) -> Option<(i64, i64)> {
        match self {
            Self::ConfirmPending { held_secs, need_secs } => Some((held_secs, need_secs)),
            _ => None,
        }
    }
    /// `true` iff an operator authored this hold, as opposed to the band
    /// falling into it. The distinction six camelot-eks bands needed and no
    /// surface could make.
    #[must_use]
    pub fn is_authored(self) -> bool {
        matches!(self, Self::ModeShadow | Self::Suspended)
    }
}

/// The flat, k8s-safe discriminant of a [`LegacyPath`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum LegacyPathKind {
    /// `spec.mode: effect`.
    ModeEffect,
    /// `spec.dryRun: false` on a two-state kind.
    TwoStateDryRun,
    /// The compiled `shadowConfirmEffect` default promoted it.
    ConfirmGate,
    /// The `breathe.pleme.io/confirmed` annotation promoted it.
    OperatorAnnotation,
}

impl LegacyPath {
    /// The wire discriminant.
    #[must_use]
    pub fn kind(self) -> LegacyPathKind {
        match self {
            Self::ModeEffect => LegacyPathKind::ModeEffect,
            Self::TwoStateDryRun => LegacyPathKind::TwoStateDryRun,
            Self::ConfirmGate { .. } => LegacyPathKind::ConfirmGate,
            Self::OperatorAnnotation => LegacyPathKind::OperatorAnnotation,
        }
    }
    /// The window this path had to satisfy, when it had one.
    #[must_use]
    pub fn required_secs(self) -> Option<i64> {
        match self {
            Self::ConfirmGate { required_secs } => Some(required_secs),
            _ => None,
        }
    }
}

// ─────────────────────────── the live-witness types ───────────────────────────

/// Which legacy path authorized a write on a band that has **no authored
/// `writeIntent`**. Every arm here is migration debt: the S5 burn-down is
/// complete when no band reports [`WitnessKind::LegacyDefault`].
///
/// Rich, and so — like [`ShadowReason`] — not itself a CRD type; the wire
/// carries [`LegacyPathKind`] plus a flat `needSecs`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "path")]
pub enum LegacyPath {
    /// `spec.mode: effect` — explicit, but through the retired field.
    ModeEffect,
    /// `spec.dryRun: false` on a two-state kind (`HostParamBand` /
    /// `KubeParamBand`) — the only kinds for which `dryRun` was ever read.
    TwoStateDryRun,
    /// No intent and no mode: the compiled `ShadowConfirmEffect` default
    /// promoted this band once its confirm window elapsed. **This is the path
    /// the ~70 live camelot-eks bands took**, under an authored `dryRun: true`
    /// that had no effect.
    #[serde(rename_all = "camelCase")]
    ConfirmGate {
        /// The window that was satisfied, in seconds.
        required_secs: i64,
    },
    /// No intent and no mode: the `breathe.pleme.io/confirmed` annotation
    /// promoted it.
    OperatorAnnotation,
}

/// The witness discriminant, exposed for matching and for status rendering
/// (the witness's own payload stays behind [`LiveWitness`]'s accessors).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum WitnessKind {
    /// A human authored `writeIntent: {intent: write, authorizedBy: …}`.
    ExplicitIntent,
    /// A calibration window elapsed cleanly.
    ConfirmGatePassed,
    /// The operator confirm annotation was set.
    OperatorAnnotation,
    /// No `writeIntent` was authored; a pre-2026-07 resolution path promoted
    /// the band. Migration debt — see [`LegacyPath`].
    LegacyDefault,
}

/// The private payload. Not `pub` — see the module doc: this is what makes
/// [`LiveWitness`] unforgeable outside this crate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase", tag = "witness")]
enum Witness {
    #[serde(rename_all = "camelCase")]
    ExplicitIntent { authorized_by: String },
    #[serde(rename_all = "camelCase")]
    ConfirmGatePassed { ready_since_epoch: i64, held_secs: i64, required_secs: i64 },
    #[serde(rename_all = "camelCase")]
    OperatorAnnotation { annotation: &'static str },
    #[serde(rename_all = "camelCase")]
    LegacyDefault { would_be: LegacyPath },
}

/// **The unforgeable evidence that a write is authorized.**
///
/// Deliberately `Serialize`-only and with **no public constructor**: the sole
/// way to obtain one outside this crate is [`resolve_gate`]. A `Deserialize`
/// impl would let any caller fabricate one out of JSON and is the one derive
/// this type must never grow — status serialization goes through the separate
/// [`EffectiveGateReport`] DTO instead, precisely so this type never needs it.
///
/// Once `ResourceProvider::assign` takes `&LiveWitness` (S2), possessing one is
/// the *only* way to reach the mutation door.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct LiveWitness(Witness);

impl LiveWitness {
    /// The witness discriminant.
    #[must_use]
    pub fn kind(&self) -> WitnessKind {
        match self.0 {
            Witness::ExplicitIntent { .. } => WitnessKind::ExplicitIntent,
            Witness::ConfirmGatePassed { .. } => WitnessKind::ConfirmGatePassed,
            Witness::OperatorAnnotation { .. } => WitnessKind::OperatorAnnotation,
            Witness::LegacyDefault { .. } => WitnessKind::LegacyDefault,
        }
    }

    /// Who authorized this write, when a human named themselves.
    #[must_use]
    pub fn authorized_by(&self) -> Option<&str> {
        match &self.0 {
            Witness::ExplicitIntent { authorized_by } => Some(authorized_by.as_str()),
            _ => None,
        }
    }

    /// The legacy resolution path, when this write is unauthored migration debt.
    #[must_use]
    pub fn legacy_path(&self) -> Option<LegacyPath> {
        match self.0 {
            Witness::LegacyDefault { would_be } => Some(would_be),
            _ => None,
        }
    }

    /// `(ready_since, held, required)` seconds, for a write authorized by a
    /// confirm window that elapsed.
    #[must_use]
    pub fn passed_window(&self) -> Option<(i64, i64, i64)> {
        match self.0 {
            Witness::ConfirmGatePassed { ready_since_epoch, held_secs, required_secs } => {
                Some((ready_since_epoch, held_secs, required_secs))
            }
            _ => None,
        }
    }

    /// `true` iff this write rests on a pre-2026-07 resolution path rather than
    /// an authored `writeIntent`. **The S5 burn-down metric.**
    #[must_use]
    pub fn is_legacy_default(&self) -> bool {
        matches!(self.0, Witness::LegacyDefault { .. })
    }

    pub(crate) fn explicit_intent(authorized_by: String) -> Self {
        Self(Witness::ExplicitIntent { authorized_by })
    }
    pub(crate) fn confirm_gate_passed(ready_since_epoch: i64, held_secs: i64, required_secs: i64) -> Self {
        Self(Witness::ConfirmGatePassed { ready_since_epoch, held_secs, required_secs })
    }
    pub(crate) fn operator_annotation() -> Self {
        Self(Witness::OperatorAnnotation { annotation: CONFIRMED_ANNOTATION })
    }
    pub(crate) fn legacy_default(would_be: LegacyPath) -> Self {
        Self(Witness::LegacyDefault { would_be })
    }
}

// ───────────────────────────── the derived verdict ────────────────────────────

/// **The controller's authorization verdict for one tick.**
///
/// `Shadow` names why it is held; `Live` carries the witness that authorizes
/// the write. Because [`LiveWitness`] cannot be constructed outside this crate,
/// neither can `EffectiveGate::Live` — the `Shadow` arm is freely constructible
/// and harmless (shadow means "do not write").
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase", tag = "state")]
pub enum EffectiveGate {
    /// Held: compute, report, attest — never write.
    Shadow {
        /// Why.
        reason: ShadowReason,
    },
    /// Authorized: a real write may run this tick.
    Live {
        /// What authorizes it.
        witness: LiveWitness,
    },
}

impl EffectiveGate {
    /// `true` iff no write is authorized this tick — the legacy
    /// `effectiveDryRun` bool, preserved exactly.
    #[must_use]
    pub fn is_shadow(&self) -> bool {
        matches!(self, Self::Shadow { .. })
    }
    /// `true` iff a real write is authorized this tick.
    #[must_use]
    pub fn is_live(&self) -> bool {
        matches!(self, Self::Live { .. })
    }
    /// The shadow reason, if held.
    #[must_use]
    pub fn shadow_reason(&self) -> Option<ShadowReason> {
        match self {
            Self::Shadow { reason } => Some(*reason),
            Self::Live { .. } => None,
        }
    }
    /// The authorizing witness, if live.
    #[must_use]
    pub fn witness(&self) -> Option<&LiveWitness> {
        match self {
            Self::Live { witness } => Some(witness),
            Self::Shadow { .. } => None,
        }
    }
    /// The flat, `Deserialize`-able status projection.
    #[must_use]
    pub fn report(&self) -> EffectiveGateReport {
        let mut r = EffectiveGateReport {
            state: GateState::Shadow,
            reason: None,
            witness: None,
            authorized_by: None,
            legacy_path: None,
            held_secs: None,
            need_secs: None,
            ready_since_epoch: None,
        };
        match self {
            Self::Shadow { reason } => {
                r.reason = Some(reason.kind());
                if let Some((held, need)) = reason.window() {
                    r.held_secs = Some(held);
                    r.need_secs = Some(need);
                }
            }
            Self::Live { witness } => {
                r.state = GateState::Live;
                r.witness = Some(witness.kind());
                r.authorized_by = witness.authorized_by().map(ToOwned::to_owned);
                if let Some(p) = witness.legacy_path() {
                    r.legacy_path = Some(p.kind());
                    r.need_secs = p.required_secs();
                }
                if let Some((since, held, need)) = witness.passed_window() {
                    r.ready_since_epoch = Some(since);
                    r.held_secs = Some(held);
                    r.need_secs = Some(need);
                }
            }
        }
        r
    }
}

/// Shadow or live — the headline an operator reads in `kubectl get`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum GateState {
    /// Observing only; nothing is written.
    Shadow,
    /// Writing.
    Live,
}

/// The **status projection** of an [`EffectiveGate`] — flat, `Deserialize`-able,
/// and k8s-structural-schema-safe, so `BandStatus` round-trips through the
/// apiserver without [`LiveWitness`] ever needing a `Deserialize` impl (which
/// would make it forgeable and defeat the whole design).
///
/// Flat by necessity as well as by taste: a CRD schema cannot carry a tagged
/// union (verified against `crdgen`), so the rich [`ShadowReason`] /
/// [`LegacyPath`] algebra is projected here as a discriminant plus its numbers
/// side by side.
///
/// A report read back off the wire is **data, never a key**: it cannot be
/// turned into a `LiveWitness` and cannot authorize anything.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EffectiveGateReport {
    /// `shadow` | `live` — the headline.
    pub state: GateState,
    /// Why it is held (shadow only). An authored `modeShadow`/`suspended` and
    /// an accidental `notReady`/`stale`/`conflict` are different answers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<ShadowReasonKind>,
    /// What authorizes it (live only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub witness: Option<WitnessKind>,
    /// Who authorized it, when a human named themselves.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorized_by: Option<String>,
    /// The pre-2026-07 resolution path, when this write rests on no authored
    /// `writeIntent`. **Non-empty here is the migration burn-down signal.**
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_path: Option<LegacyPathKind>,
    /// Seconds of clean observation held so far (a pending or just-passed
    /// confirm window).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub held_secs: Option<i64>,
    /// Seconds of clean observation required.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub need_secs: Option<i64>,
    /// When the target most recently became Ready, for a window that passed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_since_epoch: Option<i64>,
}

// ────────────────────────────── the resolver ──────────────────────────────────

/// The confirm-gate verdict, computed by the caller via
/// `outorga::PromotionPolicy::confirm_gate` and handed in. Modelled as an input
/// rather than re-derived here so this crate stays `outorga`-free and the fleet
/// keeps exactly one tested confirm-gate implementation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfirmVerdict {
    /// No calibrating intent this tick — the gate was not consulted.
    NotEvaluated,
    /// The operator annotation short-circuited the window.
    OperatorConfirmed,
    /// The clean-observation window elapsed.
    Passed {
        /// When the target most recently became Ready.
        ready_since_epoch: i64,
        /// How long it has held.
        held_secs: i64,
        /// How long it had to hold.
        required_secs: i64,
    },
    /// Ready-and-healthy, but the window has not yet elapsed.
    Pending {
        /// Seconds held so far.
        held_secs: i64,
        /// Seconds required.
        required_secs: i64,
    },
    /// A hard condition blocks promotion (not-ready / stale / conflict).
    Blocked(ShadowReason),
}

/// The pre-`writeIntent` resolution's verdict for this tick, computed by the
/// caller through `outorga::PromotionPolicy::decide` exactly as before. Used
/// **only** when no `writeIntent` is authored, which is what makes stage S1
/// byte-identical to the behavior it replaces.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LegacyDecision {
    /// The legacy chain authorized a write, via this path.
    Apply(LegacyPath),
    /// The legacy chain held the band, for this reason.
    Shadow(ShadowReason),
}

/// Everything [`resolve_gate`] needs. Flat and k8s-free, so the whole
/// authorization decision is unit-testable without a cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GateInputs {
    /// The authored intent, if any, as parsed by [`WriteIntentSpec::parse`].
    /// `None` ⇒ the legacy chain decides; `Some(Err(_))` ⇒ fail-safe shadow.
    pub intent: Option<Result<WriteIntent, IntentError>>,
    /// An external freeze (pool / fleet master switch). Overrides everything —
    /// the two-key rule.
    pub frozen: bool,
    /// The confirm-gate verdict, for a calibrating intent.
    pub confirm: ConfirmVerdict,
    /// The legacy chain's verdict, for an unauthored band.
    pub legacy: LegacyDecision,
}

/// **Resolve the authorization verdict for one tick — the only producer of a
/// [`LiveWitness`] in the fleet.**
///
/// Precedence, highest first:
/// 1. an external freeze ⇒ `Shadow { Frozen }` (the two-key rule);
/// 2. the authored [`WriteIntent`];
/// 3. the legacy chain (`mode` > compiled `ShadowConfirmEffect`).
///
/// Note what is absent from that list: **`spec.dryRun` is never consulted.**
/// It has been dead code for every k8s band kind since `76924b0`, and inferring
/// intent from a field whose meaning is contested is how this defect class is
/// born. It is retired explicitly (kept, documented, unread), not silently.
#[must_use]
pub fn resolve_gate(i: &GateInputs) -> EffectiveGate {
    // Key 1 — an external freeze shadows unconditionally, ahead of any intent.
    if i.frozen {
        return EffectiveGate::Shadow { reason: ShadowReason::Frozen };
    }
    match &i.intent {
        // ── authored but unparseable ⇒ FAIL SAFE, and say so ──────────────────
        Some(Err(IntentError::UnattributedWrite)) => {
            EffectiveGate::Shadow { reason: ShadowReason::IntentMalformed }
        }
        // ── authored ──────────────────────────────────────────────────────────
        Some(Ok(WriteIntent::Observe)) => EffectiveGate::Shadow { reason: ShadowReason::ModeShadow },
        Some(Ok(WriteIntent::Frozen)) => EffectiveGate::Shadow { reason: ShadowReason::Suspended },
        Some(Ok(WriteIntent::Write { authorized_by })) => {
            EffectiveGate::Live { witness: LiveWitness::explicit_intent(authorized_by.clone()) }
        }
        Some(Ok(WriteIntent::CalibrateThenWrite { confirm_after_seconds })) => match i.confirm {
            ConfirmVerdict::OperatorConfirmed => {
                EffectiveGate::Live { witness: LiveWitness::operator_annotation() }
            }
            ConfirmVerdict::Passed { ready_since_epoch, held_secs, required_secs } => {
                EffectiveGate::Live {
                    witness: LiveWitness::confirm_gate_passed(ready_since_epoch, held_secs, required_secs),
                }
            }
            ConfirmVerdict::Pending { held_secs, required_secs } => EffectiveGate::Shadow {
                reason: ShadowReason::ConfirmPending { held_secs, need_secs: required_secs },
            },
            ConfirmVerdict::Blocked(reason) => EffectiveGate::Shadow { reason },
            // A calibrating intent whose gate was never evaluated is a CALLER
            // BUG. Fail safe (never write) and say so honestly in the reason,
            // rather than promote on an unevaluated gate.
            ConfirmVerdict::NotEvaluated => EffectiveGate::Shadow {
                reason: ShadowReason::ConfirmPending {
                    held_secs: 0,
                    need_secs: i64::try_from(*confirm_after_seconds).unwrap_or(i64::MAX),
                },
            },
        },
        // ── unauthored: the retire-not-delete migration chain ─────────────────
        None => match i.legacy {
            LegacyDecision::Apply(path) => EffectiveGate::Live { witness: LiveWitness::legacy_default(path) },
            LegacyDecision::Shadow(reason) => EffectiveGate::Shadow { reason },
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `IntentKind::ALL` is every arm, and every label is exactly what serde
    /// writes. The MCP surface generates its tool-input schema from these two
    /// facts (it cannot derive schemars 1.x on a type kube already derives 0.8
    /// on), so a drift here would silently offer the model a value the CRD
    /// rejects.
    #[test]
    fn intent_kind_all_is_total_and_labels_match_serde() {
        assert_eq!(IntentKind::ALL.len(), 4);
        for k in IntentKind::ALL {
            assert_eq!(IntentKind::ALL.iter().filter(|x| **x == k).count(), 1, "{k:?} appears twice in ALL");
            let json = serde_json::to_value(k).unwrap();
            assert_eq!(json.as_str(), Some(k.as_str()), "serde label != as_str for {k:?}");
            let back: IntentKind = serde_json::from_value(json).unwrap();
            assert_eq!(back, k);
        }
    }

    fn inputs(intent: Option<WriteIntent>) -> GateInputs {
        raw(intent.map(Ok))
    }

    fn raw(intent: Option<Result<WriteIntent, IntentError>>) -> GateInputs {
        GateInputs {
            intent,
            frozen: false,
            confirm: ConfirmVerdict::NotEvaluated,
            legacy: LegacyDecision::Shadow(ShadowReason::NotReady),
        }
    }

    // ── the regression that 76924b0 would have failed ─────────────────────────

    /// THE ROOT-DEFECT ROW. A band with `dryRun: true`, no `mode` and no
    /// `writeIntent`, whose confirm window has elapsed, goes **LIVE** — and the
    /// verdict says so, naming the legacy path, instead of reporting "dryRun".
    /// This is the ~70-band camelot-eks reality, made visible.
    #[test]
    fn unauthored_band_past_its_window_is_live_via_a_named_legacy_path() {
        let g = resolve_gate(&GateInputs {
            legacy: LegacyDecision::Apply(LegacyPath::ConfirmGate { required_secs: 1800 }),
            ..inputs(None)
        });
        assert!(g.is_live(), "an unauthored, promoted band IS live — dryRun does not hold it");
        let w = g.witness().expect("live");
        assert!(w.is_legacy_default(), "and it is attributable as migration debt");
        assert_eq!(w.legacy_path(), Some(LegacyPath::ConfirmGate { required_secs: 1800 }));
        assert_eq!(w.kind(), WitnessKind::LegacyDefault);
    }

    /// The distinction the audit found missing: a band shadowed BY ACCIDENT
    /// (never Ready) must not report the same reason as one an operator
    /// deliberately held.
    #[test]
    fn accidental_shadow_is_distinguishable_from_authored_shadow() {
        let accidental = resolve_gate(&inputs(None));
        let authored = resolve_gate(&inputs(Some(WriteIntent::Observe)));
        assert_eq!(accidental.shadow_reason(), Some(ShadowReason::NotReady));
        assert_eq!(authored.shadow_reason(), Some(ShadowReason::ModeShadow));
        assert_ne!(accidental, authored);
    }

    // ── precedence ────────────────────────────────────────────────────────────

    #[test]
    fn an_external_freeze_overrides_even_an_explicit_write_intent() {
        let g = resolve_gate(&GateInputs {
            frozen: true,
            ..inputs(Some(WriteIntent::Write { authorized_by: "drzzln 2026-07-26".into() }))
        });
        assert!(g.is_shadow(), "the two-key rule: a freeze wins over any intent");
        assert_eq!(g.shadow_reason(), Some(ShadowReason::Frozen));
    }

    #[test]
    fn an_authored_intent_overrides_the_legacy_chain() {
        // The legacy chain would promote; the authored intent says observe.
        let g = resolve_gate(&GateInputs {
            legacy: LegacyDecision::Apply(LegacyPath::ModeEffect),
            ..inputs(Some(WriteIntent::Observe))
        });
        assert!(g.is_shadow(), "writeIntent > mode");
    }

    #[test]
    fn an_explicit_write_names_its_author() {
        let g = resolve_gate(&inputs(Some(WriteIntent::Write { authorized_by: "drzzln: neo4j sized by hand".into() })));
        assert_eq!(g.witness().and_then(LiveWitness::authorized_by), Some("drzzln: neo4j sized by hand"));
        assert!(!g.witness().unwrap().is_legacy_default(), "an authored write is not migration debt");
    }

    #[test]
    fn frozen_intent_holds_but_stays_distinguishable_from_a_pool_freeze() {
        let authored = resolve_gate(&inputs(Some(WriteIntent::Frozen)));
        assert_eq!(authored.shadow_reason(), Some(ShadowReason::Suspended));
        let external = resolve_gate(&GateInputs { frozen: true, ..inputs(None) });
        assert_eq!(external.shadow_reason(), Some(ShadowReason::Frozen));
    }

    // ── calibration ───────────────────────────────────────────────────────────

    #[test]
    fn calibration_shadows_until_the_window_elapses_then_witnesses_the_gate() {
        let pending = resolve_gate(&GateInputs {
            confirm: ConfirmVerdict::Pending { held_secs: 400, required_secs: 1800 },
            ..inputs(Some(WriteIntent::CalibrateThenWrite { confirm_after_seconds: 1800 }))
        });
        assert_eq!(
            pending.shadow_reason(),
            Some(ShadowReason::ConfirmPending { held_secs: 400, need_secs: 1800 }),
            "and the operator is told how much longer, not 'dryRun'"
        );

        let passed = resolve_gate(&GateInputs {
            confirm: ConfirmVerdict::Passed { ready_since_epoch: 1000, held_secs: 1801, required_secs: 1800 },
            ..inputs(Some(WriteIntent::CalibrateThenWrite { confirm_after_seconds: 1800 }))
        });
        assert_eq!(passed.witness().map(LiveWitness::kind), Some(WitnessKind::ConfirmGatePassed));
    }

    #[test]
    fn a_calibrating_intent_with_an_unevaluated_gate_fails_safe() {
        let g = resolve_gate(&inputs(Some(WriteIntent::CalibrateThenWrite { confirm_after_seconds: 900 })));
        assert!(g.is_shadow(), "an unevaluated gate must never promote");
    }

    #[test]
    fn a_blocked_gate_reports_the_hard_condition_not_a_pending_window() {
        for r in [ShadowReason::Stale, ShadowReason::Conflict, ShadowReason::NotReady] {
            let g = resolve_gate(&GateInputs {
                confirm: ConfirmVerdict::Blocked(r),
                ..inputs(Some(WriteIntent::CalibrateThenWrite { confirm_after_seconds: 1800 }))
            });
            assert_eq!(g.shadow_reason(), Some(r));
        }
    }

    #[test]
    fn the_operator_annotation_is_its_own_witness() {
        let g = resolve_gate(&GateInputs {
            confirm: ConfirmVerdict::OperatorConfirmed,
            ..inputs(Some(WriteIntent::CalibrateThenWrite { confirm_after_seconds: 1800 }))
        });
        assert_eq!(g.witness().map(LiveWitness::kind), Some(WitnessKind::OperatorAnnotation));
    }

    // ── the status projection ─────────────────────────────────────────────────

    #[test]
    fn the_report_round_trips_and_carries_the_burn_down_signal() {
        let g = resolve_gate(&GateInputs {
            legacy: LegacyDecision::Apply(LegacyPath::TwoStateDryRun),
            ..inputs(None)
        });
        let r = g.report();
        assert_eq!(r.state, GateState::Live);
        assert_eq!(r.legacy_path, Some(LegacyPathKind::TwoStateDryRun));
        let json = serde_json::to_string(&r).expect("serialize");
        let back: EffectiveGateReport = serde_json::from_str(&json).expect("the status DTO round-trips");
        assert_eq!(r, back);
    }

    #[test]
    fn a_shadow_report_carries_the_reason_and_no_witness() {
        let r = resolve_gate(&inputs(None)).report();
        assert_eq!(r.state, GateState::Shadow);
        assert_eq!(r.reason, Some(ShadowReasonKind::NotReady));
        assert!(r.witness.is_none() && r.authorized_by.is_none());
    }

    // ── the intent's own parse boundary ───────────────────────────────────────

    /// "Go live" without saying who is **rejected**, not silently granted as an
    /// anonymous write. Blank and whitespace-only authors are rejected too —
    /// something a plain required-`String` schema field would have accepted.
    #[test]
    fn a_write_intent_without_an_author_is_rejected() {
        for a in [None, Some(""), Some("   ")] {
            let spec = WriteIntentSpec {
                intent: IntentKind::Write,
                confirm_after_seconds: None,
                authorized_by: a.map(ToOwned::to_owned),
            };
            assert_eq!(spec.parse(), Err(IntentError::UnattributedWrite), "author {a:?} must not authorize");
        }
        let named = WriteIntentSpec {
            intent: IntentKind::Write,
            confirm_after_seconds: None,
            authorized_by: Some("  drzzln 2026-07-26  ".into()),
        };
        assert_eq!(named.parse(), Ok(WriteIntent::Write { authorized_by: "drzzln 2026-07-26".into() }));
    }

    /// An unattributed write must never reach the mutation door — it shadows,
    /// with a reason that names the malformation instead of masquerading as a
    /// pending window or (worse) a granted write.
    #[test]
    fn an_unparseable_intent_fails_safe_and_is_named() {
        let g = resolve_gate(&GateInputs {
            // The legacy chain WOULD have promoted this band.
            legacy: LegacyDecision::Apply(LegacyPath::ModeEffect),
            ..raw(Some(Err(IntentError::UnattributedWrite)))
        });
        assert!(g.is_shadow(), "a malformed intent must never authorize a write");
        assert_eq!(g.shadow_reason(), Some(ShadowReason::IntentMalformed));
    }

    /// The wire shape must stay a FLAT struct: `kube-core`'s structural-schema
    /// hoister rejects a tagged enum (verified by running crdgen), so a future
    /// refactor back to `#[serde(tag = ...)]` would break CRD generation.
    #[test]
    fn the_wire_shape_is_flat_and_camel_cased() {
        let spec: WriteIntentSpec =
            serde_json::from_str(r#"{"intent":"calibrateThenWrite","confirmAfterSeconds":900}"#).expect("flat");
        assert_eq!(spec.parse(), Ok(WriteIntent::CalibrateThenWrite { confirm_after_seconds: 900 }));
        let json = serde_json::to_value(&spec).expect("serialize");
        assert!(json.get("intent").is_some() && json.get("confirmAfterSeconds").is_some());
        assert!(json.get("authorizedBy").is_none(), "absent optionals stay absent");
    }

    #[test]
    fn calibration_defaults_its_window_and_absence_is_never_a_silent_intent() {
        let spec: WriteIntentSpec = serde_json::from_str(r#"{"intent":"calibrateThenWrite"}"#).expect("defaults");
        assert_eq!(spec.parse(), Ok(WriteIntent::CalibrateThenWrite { confirm_after_seconds: 1800 }));
        // `WriteIntent` has no `Default` impl (a `WriteIntent::default()` call
        // would not compile) and the wire shape has no default `intent`, so an
        // empty object cannot silently become an authorization.
        assert!(serde_json::from_str::<WriteIntentSpec>("{}").is_err(), "absence is never a silent intent");
    }

    #[test]
    fn may_write_partitions_the_intents() {
        assert!(WriteIntent::Write { authorized_by: "x".into() }.may_write());
        assert!(WriteIntent::CalibrateThenWrite { confirm_after_seconds: 1 }.may_write());
        assert!(!WriteIntent::Observe.may_write());
        assert!(!WriteIntent::Frozen.may_write());
    }
}
