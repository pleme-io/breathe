//! `breathe-admission` — validated-resource admission (docs/PROVISIONING.md §2.3).
//!
//! **The headline guarantee: an unvalidated resource is UNREPRESENTABLE in the
//! valid pool.** A candidate resource flows through a phantom-typestate FSM
//! ([`Recurso<P>`]); only a [`Recurso<Pronto>`] (every admission gate passed) can
//! mint an [`Admitido<T>`] proof-carrying certificate; and the [`Viveiro`] pool
//! accepts *only* `Admitido<T>`. Inserting a bare `Recurso<P>` is a **type
//! error** — not a runtime guard. An illegal lifecycle transition (e.g.
//! `Rejeitado → Pronto`) is an **absent method (E0599)**, because the forward
//! method to a non-legal next phase does not exist.
//!
//! The two reject/timeout terminals (`Rejeitado`, `Expirado`) are first-class —
//! a gate that rejects routes to `Rejeitado`, a gate that times out (or a
//! deferral budget that exhausts) routes to `Expirado`, so **no candidate sits in
//! `Validando` forever** and the convergence claim (every reachable phase reaches
//! a good terminal) is non-vacuous.
//!
//! Tier honesty (docs/PROVISIONING.md §6): the *library* path here is
//! truly-unrepresentable (E0599 / sealed ctor / `Admitido`-only pool). The
//! *wire/controller* path (a CRD-deserialized [`FaseRecurso`] reconstructing a
//! `Recurso`) is **parse-time-rejected** — phantom types erase at the serde
//! boundary, so the reconstruction is a `try_from` along a legal edge, the eclusa
//! §III.5 precedent. This crate ships the library path + the typed edge table the
//! wire path validates against.
//!
//! # The unrepresentability, proven mechanically (compile-fail)
//!
//! An **illegal lifecycle transition** is an absent method — you cannot jump
//! `Descoberto → Pronto`:
//! ```compile_fail
//! use breathe_admission::{Recurso, Descoberto, ResourceId};
//! use breathe_provider::Forma;
//! let r = Recurso::<Descoberto>::discover(ResourceId::new("n"), Forma::NodeOnDemand);
//! let _ = r.ready();           // E0599/E0624: no public `ready` on Recurso<Descoberto>
//! ```
//!
//! An **unvalidated resource cannot enter the pool** — the `Viveiro` accepts only
//! `Admitido<T>`, never a bare `Recurso`:
//! ```compile_fail
//! use breathe_admission::{Recurso, Descoberto, ResourceId, Viveiro};
//! use breathe_provider::Forma;
//! let r = Recurso::<Descoberto>::discover(ResourceId::new("n"), Forma::NodeOnDemand);
//! let mut pool: Viveiro<()> = Viveiro::new();
//! pool.admit(r);               // E0308: expected Admitido<()>, found Recurso<Descoberto>
//! ```
//!
//! An **`Admitido<T>` cannot be fabricated** — its sole constructor is
//! `Recurso<Pronto>::admit`; the private `_seal` field blocks struct-literal
//! construction:
//! ```compile_fail
//! use breathe_admission::Admitido;
//! let _bad: Admitido<()> = Admitido { inner: (), id: todo!(), forma: todo!(), evidence: vec![], _seal: () };
//! ```

use std::marker::PhantomData;

use breathe_provider::Forma;

// ============================================================================
// FaseRecurso — the serializable phase label (CRD status + the wire path).
// ============================================================================

/// The closed legal-state set of a candidate resource's lifecycle. This is the
/// *serializable* label (the CRD `status.phase`); the in-Rust typestate ([`Phase`]
/// markers + [`Recurso<P>`]) is the compile-time enforcement of the same FSM.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum FaseRecurso {
    /// A candidate the predictor wants but that does not yet exist.
    Descoberto,
    /// Provisioning dispatched (a magma Plan); not yet a real resource.
    Provisionando,
    /// The resource exists; not yet validated.
    Provisionado,
    /// Running the admission gates.
    Validando,
    /// Every gate passed; ready to admit (not yet in the pool).
    Pronto,
    /// In the [`Viveiro`] — usable. (Held as an [`Admitido<T>`], not a bare `Recurso`.)
    Admitido,
    /// Decommission begun — cordoned (no new work).
    Cordoado,
    /// Cordoned and draining existing work (PDB-aware).
    Drenando,
    /// Cleanly retired — a good terminal.
    Aposentado,
    /// A gate rejected the candidate — a good terminal (clean refusal).
    Rejeitado,
    /// Provisioning or a gate timed out / a defer budget exhausted — a good
    /// terminal (clean timeout). Without this the candidate could wedge in
    /// `Validando` forever (the FSM-completeness bug the critique caught).
    Expirado,
}

impl FaseRecurso {
    /// Every phase, for the reflection/BFS tests.
    pub const ALL: [FaseRecurso; 11] = [
        Self::Descoberto,
        Self::Provisionando,
        Self::Provisionado,
        Self::Validando,
        Self::Pronto,
        Self::Admitido,
        Self::Cordoado,
        Self::Drenando,
        Self::Aposentado,
        Self::Rejeitado,
        Self::Expirado,
    ];

    /// Absorbing terminals — no legal successor.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Aposentado | Self::Rejeitado | Self::Expirado)
    }

    /// Good terminals — every reachable phase must have a path to one (the
    /// convergence claim). All three terminals are "good": `Aposentado` is a
    /// clean retire, `Rejeitado` a clean refusal, `Expirado` a clean timeout. A
    /// phase with no path to one would be a *stuck* state — exactly what the
    /// reject/timeout exits out of `Validando` prevent.
    #[must_use]
    pub fn is_good_terminal(self) -> bool {
        self.is_terminal()
    }

    /// The legal forward edges — the canonical FSM. Drives the BFS reachability
    /// test AND the wire-path `try_from` (a CRD-deserialized phase reconstructs a
    /// `Recurso` only along a legal edge). The `Validando → Validando` self-loop
    /// is the *bounded* deferral (a gate said `Defer`); the deferral budget
    /// (see [`classify`]) forces `Expirado` once exhausted, so the self-loop
    /// cannot run forever.
    #[must_use]
    pub fn legal_successors(self) -> &'static [FaseRecurso] {
        use FaseRecurso::{
            Admitido, Aposentado, Cordoado, Descoberto, Drenando, Expirado, Pronto, Provisionado,
            Provisionando, Rejeitado, Validando,
        };
        match self {
            Descoberto => &[Provisionando, Rejeitado],
            Provisionando => &[Provisionado, Expirado, Rejeitado],
            Provisionado => &[Validando],
            Validando => &[Pronto, Validando, Rejeitado, Expirado],
            Pronto => &[Admitido],
            Admitido => &[Cordoado],
            Cordoado => &[Drenando],
            Drenando => &[Aposentado],
            Aposentado | Rejeitado | Expirado => &[],
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Descoberto => "descoberto",
            Self::Provisionando => "provisionando",
            Self::Provisionado => "provisionado",
            Self::Validando => "validando",
            Self::Pronto => "pronto",
            Self::Admitido => "admitido",
            Self::Cordoado => "cordoado",
            Self::Drenando => "drenando",
            Self::Aposentado => "aposentado",
            Self::Rejeitado => "rejeitado",
            Self::Expirado => "expirado",
        }
    }
}

impl std::fmt::Display for FaseRecurso {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The resource-admission lifecycle is a convergent typed FSM (the eclusa/galho
/// discipline): every reachable phase reaches a GOOD terminal over the legal
/// edges, and the only terminals are good ones. Implementing the shared
/// [`shigoto_fsm::ConvergentFsm`] trait replaces the hand-rolled BFS reachability
/// tests with the fleet harness — `assert_convergent_fsm::<FaseRecurso>()` proves
/// closed-graph + terminal-soundness + no-traps + universal convergence in one call.
impl shigoto_fsm::ConvergentFsm for FaseRecurso {
    fn all() -> &'static [Self] {
        &Self::ALL
    }
    fn successors(&self) -> Vec<Self> {
        self.legal_successors().to_vec()
    }
    fn is_terminal(&self) -> bool {
        (*self).is_terminal()
    }
    fn is_good_terminal(&self) -> bool {
        (*self).is_good_terminal()
    }
}

// ============================================================================
// The phantom typestate — the in-Rust enforcement (illegal transition = E0599).
// ============================================================================

mod sealed {
    pub trait Sealed {}
}

/// A typestate marker — a zero-sized phase. Sealed: only this crate's phases
/// implement it, so no external code can mint a new phase or bypass the FSM.
pub trait Phase: sealed::Sealed {
    /// The serializable label this marker corresponds to.
    const FASE: FaseRecurso;
}

macro_rules! phase {
    ($(#[$m:meta])* $name:ident => $fase:ident) => {
        $(#[$m])*
        #[derive(Debug, Clone, Copy)]
        pub struct $name;
        impl sealed::Sealed for $name {}
        impl Phase for $name {
            const FASE: FaseRecurso = FaseRecurso::$fase;
        }
    };
}

phase!(/// Phase marker: a wanted-but-not-yet-existing candidate.
    Descoberto => Descoberto);
phase!(/// Phase marker: provisioning dispatched.
    Provisionando => Provisionando);
phase!(/// Phase marker: the resource exists, unvalidated.
    Provisionado => Provisionado);
phase!(/// Phase marker: running the admission gates.
    Validando => Validando);
phase!(/// Phase marker: every gate passed; ready to admit.
    Pronto => Pronto);
phase!(/// Phase marker (terminal): cleanly refused by a gate.
    Rejeitado => Rejeitado);
phase!(/// Phase marker (terminal): provisioning/validation timed out.
    Expirado => Expirado);

/// A candidate resource whose lifecycle phase is encoded in the type `P`. **Only
/// the forward method to a LEGAL next phase exists** — an illegal transition is
/// an absent method (E0599), never a runtime branch. Construct with
/// [`Recurso::<Descoberto>::discover`]; advance with the per-phase methods.
pub struct Recurso<P: Phase> {
    id: ResourceId,
    forma: Forma,
    evidence: Vec<ReciboGate>,
    _p: PhantomData<P>,
}

/// A candidate's stable identity (a node name, an instance id).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct ResourceId(pub String);

impl ResourceId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

impl<P: Phase> Recurso<P> {
    /// PRIVATE — the only way to move phase is the legal per-phase methods, so an
    /// illegal jump cannot be expressed by an external caller.
    fn advance<Q: Phase>(self) -> Recurso<Q> {
        Recurso { id: self.id, forma: self.forma, evidence: self.evidence, _p: PhantomData }
    }

    fn into_rejected(mut self, reason: impl Into<String>) -> Recurso<Rejeitado> {
        self.evidence.push(ReciboGate {
            kind: PortaoKind::ConformanceBinding,
            decision: GateDecision::Reject { reason: reason.into() },
        });
        self.advance()
    }

    #[must_use]
    pub fn id(&self) -> &ResourceId {
        &self.id
    }
    #[must_use]
    pub fn forma(&self) -> Forma {
        self.forma
    }
    /// The runtime phase label of this typed value — `P::FASE`.
    #[must_use]
    pub fn fase(&self) -> FaseRecurso {
        P::FASE
    }
    #[must_use]
    pub fn evidence(&self) -> &[ReciboGate] {
        &self.evidence
    }
}

impl Recurso<Descoberto> {
    /// Discover a candidate — the FSM's only entry point.
    #[must_use]
    pub fn discover(id: ResourceId, forma: Forma) -> Self {
        Recurso { id, forma, evidence: Vec::new(), _p: PhantomData }
    }
    /// Descoberto → Provisionando.
    #[must_use]
    pub fn begin_provision(self) -> Recurso<Provisionando> {
        self.advance()
    }
    /// Descoberto → Rejeitado (refused before provisioning — e.g. over budget).
    #[must_use]
    pub fn reject(self, reason: impl Into<String>) -> Recurso<Rejeitado> {
        self.into_rejected(reason)
    }
}

impl Recurso<Provisionando> {
    /// Provisionando → Provisionado.
    #[must_use]
    pub fn provisioned(self) -> Recurso<Provisionado> {
        self.advance()
    }
    /// Provisionando → Expirado (provisioning timed out).
    #[must_use]
    pub fn expire(self) -> Recurso<Expirado> {
        self.advance()
    }
    /// Provisionando → Rejeitado (provisioning failed permanently).
    #[must_use]
    pub fn reject(self, reason: impl Into<String>) -> Recurso<Rejeitado> {
        self.into_rejected(reason)
    }
}

impl Recurso<Provisionado> {
    /// Provisionado → Validando.
    #[must_use]
    pub fn begin_validation(self) -> Recurso<Validando> {
        self.advance()
    }
}

impl Recurso<Validando> {
    /// Append a gate receipt (the validate beat accumulates evidence).
    #[must_use]
    pub fn record(mut self, recibo: ReciboGate) -> Self {
        self.evidence.push(recibo);
        self
    }
    /// Validando → Pronto. **`pub(crate)` — the ONLY caller is [`classify`]**, so
    /// external code cannot shortcut a candidate to `Pronto` (and thence the pool)
    /// without going through the gate classification. `Pronto` is the only door to
    /// [`Admitido`](Admitido); there is no method from `Validando` straight to the
    /// pool. (M1 blocks *accidental* bypass structurally; *deliberate* forgery of
    /// a `Pass` receipt is blocked at M3 by the `AttestationBinding` gate's
    /// Ed25519-signed receipts — see docs/PROVISIONING.md §6 row 1's honest tier.)
    #[must_use]
    pub(crate) fn ready(self) -> Recurso<Pronto> {
        self.advance()
    }
    /// Validando → Rejeitado (a gate rejected).
    #[must_use]
    pub fn reject(self, reason: impl Into<String>) -> Recurso<Rejeitado> {
        self.into_rejected(reason)
    }
    /// Validando → Expirado (a gate timed out / the defer budget exhausted).
    #[must_use]
    pub fn expire(self) -> Recurso<Expirado> {
        self.advance()
    }
}

impl Recurso<Pronto> {
    /// **The SOLE constructor of [`Admitido<T>`]** — minting the proof-carrying
    /// certificate. Consumes the `Recurso<Pronto>` (every gate passed) and wraps
    /// the caller's `inner` handle (a node ref, an instance descriptor) together
    /// with the accumulated gate evidence. After this, the resource lives in the
    /// [`Viveiro`]; an unvalidated resource cannot reach the pool because this is
    /// the only path that mints the wrapper the pool accepts.
    #[must_use]
    pub fn admit<T>(self, inner: T) -> Admitido<T> {
        Admitido { inner, id: self.id, forma: self.forma, evidence: self.evidence, _seal: () }
    }
}

// ============================================================================
// Admitido<T> — the sealed, proof-carrying admission certificate.
// ============================================================================

/// A proof-carrying admission certificate. **Its sole constructor is
/// [`Recurso<Pronto>::admit`]** — so *holding* an `Admitido<T>` IS proof the
/// resource cleared every admission gate. The fields are private and a private
/// `_seal` unit field blocks struct-literal construction, so no external code can
/// fabricate one. The [`Viveiro`] accepts only `Admitido<T>`.
pub struct Admitido<T> {
    inner: T,
    id: ResourceId,
    forma: Forma,
    evidence: Vec<ReciboGate>,
    /// Private unit field — blocks external struct-literal construction. The only
    /// mint is `Recurso<Pronto>::admit`.
    _seal: (),
}

impl<T> Admitido<T> {
    #[must_use]
    pub fn id(&self) -> &ResourceId {
        &self.id
    }
    #[must_use]
    pub fn forma(&self) -> Forma {
        self.forma
    }
    /// The admission proof — the sealed chain of passing gate receipts.
    #[must_use]
    pub fn evidence(&self) -> &[ReciboGate] {
        &self.evidence
    }
    /// The phase of an admitted resource is always [`FaseRecurso::Admitido`].
    #[must_use]
    pub fn fase(&self) -> FaseRecurso {
        FaseRecurso::Admitido
    }
    #[must_use]
    pub fn get(&self) -> &T {
        &self.inner
    }
    #[must_use]
    pub fn into_inner(self) -> T {
        self.inner
    }
}

// ============================================================================
// Viveiro — the valid-resource pool (accepts ONLY Admitido<T>).
// ============================================================================

/// The valid-resource pool (Portuguese *nursery*). **Accepts only [`Admitido<T>`]
/// — inserting a bare [`Recurso<P>`] is a type error (E0308).** This is the
/// headline unrepresentability: "an unvalidated resource is in the pool" cannot
/// be expressed. breathe bands + the scheduler read *only* from here, never the
/// raw kube Node list.
pub struct Viveiro<T> {
    admitted: std::collections::BTreeMap<ResourceId, Admitido<T>>,
}

impl<T> Default for Viveiro<T> {
    fn default() -> Self {
        Self { admitted: std::collections::BTreeMap::new() }
    }
}

impl<T> Viveiro<T> {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    /// Admit a validated resource into the pool. The signature is the guarantee:
    /// only an `Admitido<T>` is accepted.
    pub fn admit(&mut self, cert: Admitido<T>) {
        self.admitted.insert(cert.id.clone(), cert);
    }
    #[must_use]
    pub fn get(&self, id: &ResourceId) -> Option<&Admitido<T>> {
        self.admitted.get(id)
    }
    #[must_use]
    pub fn len(&self) -> usize {
        self.admitted.len()
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.admitted.is_empty()
    }
    pub fn iter(&self) -> impl Iterator<Item = &Admitido<T>> {
        self.admitted.values()
    }
    /// Retire a resource (the decommission path's terminal — `Aposentado`).
    pub fn retire(&mut self, id: &ResourceId) -> Option<Admitido<T>> {
        self.admitted.remove(id)
    }
}

// ============================================================================
// The admission gates (Portao) + the partial-failure classification.
// ============================================================================

/// The nine admission-gate kinds (docs/PROVISIONING.md §2.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum PortaoKind {
    /// Real at M1: the node's allocatable capacity proves it can host the floor.
    CapacidadeProof,
    ConformanceBinding,
    HealthLiveness,
    SchedulerReadiness,
    NodeCondition,
    AttestationBinding,
    AffinityFeasibility,
    QuotaCheck,
    CostEnvelope,
}

impl PortaoKind {
    pub const ALL: [PortaoKind; 9] = [
        Self::CapacidadeProof,
        Self::ConformanceBinding,
        Self::HealthLiveness,
        Self::SchedulerReadiness,
        Self::NodeCondition,
        Self::AttestationBinding,
        Self::AffinityFeasibility,
        Self::QuotaCheck,
        Self::CostEnvelope,
    ];
}

/// A gate's verdict. `Defer` requeues (bounded by the defer budget); `Reject` is
/// terminal. (Crypto attestation — BLAKE3 + Ed25519 — lands with the
/// `AttestationBinding` gate at M3; M1 carries the typed verdict only.)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateDecision {
    Pass,
    Defer { reason: String },
    Reject { reason: String },
}

/// A per-gate receipt — the typed evidence one gate emits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReciboGate {
    pub kind: PortaoKind,
    pub decision: GateDecision,
}

impl ReciboGate {
    #[must_use]
    pub fn pass(kind: PortaoKind) -> Self {
        Self { kind, decision: GateDecision::Pass }
    }
    #[must_use]
    pub fn reject(kind: PortaoKind, reason: impl Into<String>) -> Self {
        Self { kind, decision: GateDecision::Reject { reason: reason.into() } }
    }
    #[must_use]
    pub fn defer(kind: PortaoKind, reason: impl Into<String>) -> Self {
        Self { kind, decision: GateDecision::Defer { reason: reason.into() } }
    }
}

/// An admission gate — a pluggable check run during `Validando`. M1 ships the
/// trait + the nine kinds; `CapacidadeProof` is real, the other eight are honest
/// stubs that `Defer` with a "not yet implemented" reason (fail-SAFE: nothing is
/// admitted until every gate is real, so no unvalidated resource sneaks in while
/// the gates are being built — never a silent `Pass`).
#[async_trait::async_trait]
pub trait Portao<T>: Send + Sync {
    fn kind(&self) -> PortaoKind;
    async fn check(&self, candidate: &Recurso<Validando>, inner: &T) -> ReciboGate;
}

/// The typed outcome of classifying a round of gate receipts against the
/// partial-failure rule. **No candidate can wedge in `Validando`**: a reject →
/// `Rejected`, an exhausted defer budget → `Expired`, an in-budget defer →
/// `Deferred` (requeue), all gates pass → `Ready`.
pub enum ValidationStep {
    /// Every gate passed — advanced to `Pronto`, ready to `admit`.
    Ready(Recurso<Pronto>),
    /// A gate rejected — terminal.
    Rejected(Recurso<Rejeitado>),
    /// A gate deferred and the budget is exhausted — terminal (timeout).
    Expired(Recurso<Expirado>),
    /// A gate deferred with budget remaining — requeue with the decremented budget.
    Deferred(Recurso<Validando>, u32),
}

/// Apply the partial-failure rule to a round of gate receipts. **The order is
/// load-bearing:** any `Reject` is terminal first (fail-closed); else any `Defer`
/// requeues until the budget hits zero, then `Expired`; else (all `Pass`) `Ready`.
#[must_use]
pub fn classify(candidate: Recurso<Validando>, receipts: &[ReciboGate], defer_budget: u32) -> ValidationStep {
    if let Some(r) = receipts.iter().find(|r| matches!(r.decision, GateDecision::Reject { .. })) {
        let reason = match &r.decision {
            GateDecision::Reject { reason } => format!("{}: {reason}", kind_str(r.kind)),
            _ => unreachable!(),
        };
        return ValidationStep::Rejected(candidate.reject(reason));
    }
    let deferred = receipts.iter().any(|r| matches!(r.decision, GateDecision::Defer { .. }));
    if deferred {
        if defer_budget == 0 {
            return ValidationStep::Expired(candidate.expire());
        }
        let mut c = candidate;
        for r in receipts {
            c = c.record(r.clone());
        }
        return ValidationStep::Deferred(c, defer_budget - 1);
    }
    // every gate passed
    let mut c = candidate;
    for r in receipts {
        c = c.record(r.clone());
    }
    ValidationStep::Ready(c.ready())
}

fn kind_str(k: PortaoKind) -> &'static str {
    match k {
        PortaoKind::CapacidadeProof => "capacidade-proof",
        PortaoKind::ConformanceBinding => "conformance-binding",
        PortaoKind::HealthLiveness => "health-liveness",
        PortaoKind::SchedulerReadiness => "scheduler-readiness",
        PortaoKind::NodeCondition => "node-condition",
        PortaoKind::AttestationBinding => "attestation-binding",
        PortaoKind::AffinityFeasibility => "affinity-feasibility",
        PortaoKind::QuotaCheck => "quota-check",
        PortaoKind::CostEnvelope => "cost-envelope",
    }
}

// ============================================================================
// M1 gate impls — CapacidadeProof real; the other eight are fail-safe stubs.
// ============================================================================

/// Real at M1: proves the candidate's allocatable capacity covers the required
/// floor (the never-swap floor-from-peak check, BREATHABILITY-MATH §4.3). A node
/// that cannot host the floor is rejected at admission — VPA/predictor asking for
/// a too-big size becomes parse-time-rejected, not a post-facto OOM.
pub struct CapacidadeProof {
    /// The floor this shape must be able to host (in the forma's unit).
    pub required_floor: u64,
}

/// What `CapacidadeProof` inspects on the candidate's inner handle.
pub trait Allocatable {
    /// The candidate's proven allocatable capacity (in the forma's unit).
    fn allocatable(&self) -> u64;
}

#[async_trait::async_trait]
impl<T: Allocatable + Send + Sync> Portao<T> for CapacidadeProof {
    fn kind(&self) -> PortaoKind {
        PortaoKind::CapacidadeProof
    }
    async fn check(&self, _candidate: &Recurso<Validando>, inner: &T) -> ReciboGate {
        if inner.allocatable() >= self.required_floor {
            ReciboGate::pass(PortaoKind::CapacidadeProof)
        } else {
            ReciboGate::reject(
                PortaoKind::CapacidadeProof,
                format!("allocatable {} < required floor {}", inner.allocatable(), self.required_floor),
            )
        }
    }
}

/// An honest M1 stub gate — `Defer`s with a "not yet implemented" reason for one
/// of the eight not-yet-real gate kinds. Fail-safe: a candidate cannot be admitted
/// while any gate is a stub (it stays in the deferral loop until the budget
/// expires), so no unvalidated resource is ever admitted by omission. **Never a
/// silent `Pass`.**
pub struct StubGate(pub PortaoKind);

#[async_trait::async_trait]
impl<T: Send + Sync> Portao<T> for StubGate {
    fn kind(&self) -> PortaoKind {
        self.0
    }
    async fn check(&self, _candidate: &Recurso<Validando>, _inner: &T) -> ReciboGate {
        ReciboGate::defer(self.0, "gate not yet implemented (M1) — fail-safe defer")
    }
}

#[cfg(test)]
mod tests;
