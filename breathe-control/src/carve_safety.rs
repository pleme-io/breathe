//! `carve_safety` — the STRUCTURAL storage-carve safety triad.
//!
//! The storage carve mutates a live PVC / EBS volume. Three ways it could do
//! harm — overstep a volume it does not own, release a block that is in use or
//! holds durable data, or take an unbounded/destructive autonomous step — were
//! previously held by DISCIPLINE (a grow-only config flag, a namespace filter, a
//! careful regenerable-vs-SaaS classification a reviewer had to keep correct).
//! Discipline leaks: a filter can be miswritten, a flag can be flipped, a
//! reviewer tires. This module removes the leaks by removing the STATES — the
//! bad actions have no code path.
//!
//! ## The three invariants, made structural
//!
//! **1. Never overstep other structures (footprint ownership).** The cluster has
//! resident non-camelot volumes (rabbitmq, profiling, the resident akeyless
//! workloads). The carve commit takes an [`OwnedPvc`], never a raw ref. An
//! `OwnedPvc` is constructible ONLY through [`FootprintPolicy::try_own`], which
//! re-parses the volume's live identity against the footprint predicate (label
//! `role: camelot` / namespace ∈ the owned set / the ownership tag). A resident
//! volume yields no `OwnedPvc` — acting on it is a PARSE-TIME REJECTION, not a
//! filter that could be miswritten. **Tier: parse-time-rejected** (the ownership
//! parse is the sole ingress; fields private; re-parsed every tick).
//!
//! **2. Never release a block in use — or a durable one.** Releasing/shrinking a
//! volume needs TWO witnesses that do not exist for an unsafe target:
//! - a `Regenerable` TYPE — only [`RegenerableVolume`] exposes
//!   [`release`](RegenerableVolume::release)/[`recreate`](RegenerableVolume::recreate).
//!   [`DurableVolume`] (SaaS state — mysql / rustfs) has NO such method, so
//!   releasing durable data is a COMPILE ERROR. **Tier: truly-unrepresentable**
//!   (no method to call). The classification is SAFE-BY-DEFAULT: an
//!   unknown/ambiguous volume is `Durable` (no release path) — we can never
//!   destroy data we failed to classify.
//! - a [`NotInUse`] proof — minted only from live reachability facts
//!   ([`ConsumerReachability`]: PVC→pod — if any live pod references the PVC,
//!   there is no witness). Orphan (no live consumer + detached) = unreachable =
//!   safe. **Tier: parse-time-rejected** (the `release` signature demands the
//!   witness; an in-use volume yields none).
//!
//! **3. Autonomous adjustments are small + atomic + grow-only.** The ongoing
//! live band's only autonomous action is a [`SmallAtomicGrow`] — a bounded
//! increment (`Δ ≤ min(+2Gi, +20%)`), applied as an EBS online-expand
//! ([`AtomicGrowCommand`] = `ModifyVolume`: in-place, atomic, no data movement,
//! no delete). The autonomous path is [`AutonomousStorageCarve`], a `GrowOnly`
//! typestate that has NO `shrink`/`release`/unbounded-jump method — those live
//! ONLY on the operator-gated path behind the two release witnesses. **Tier:
//! truly-unrepresentable** that the autonomous loop shrinks/releases (no method);
//! **parse-time-rejected** that a step exceeds the cap (the sole constructor
//! clamps). The grow-only-ness that was a config flag ([`Directionality::GrowOnly`])
//! is now the TYPE of the path.
//!
//! ## Per-tick re-verification (SUPER-SAFE — verified against live state, not once)
//!
//! Every witness carries a [`TickId`] and is RE-MINTED from live state each tick
//! — never cached. The [`commit_small_atomic_grow`] Act step CONSUMES the
//! fresh-this-tick witnesses (they are `!Clone`, move-only) and re-verifies every
//! boundary THIS tick: is it still owned, is the step still bounded, is the volume
//! not mid-operation (no in-flight `ModifyVolume`). A boundary that was true last
//! tick but flipped (a pod attached, ownership changed, an op in-flight) is caught
//! THIS tick and the action is refused. So "verified per tick" is structural: a
//! witness cannot drive two Acts (move-consumed — E0382), and a stale witness
//! (carried from a prior tick) is rejected by the freshness re-check.
//! **Tier: parse-time-rejected per tick** for the re-mint (a flipped boundary
//! yields no witness this tick); **truly-unrepresentable** for reuse (move-only);
//! the tick-id equality in the commit is a runtime `==` (**only-mitigated** for
//! the comparison itself), but no Act path exists that does not consume a witness
//! minted from live facts.
//!
//! ## Data-corruption unrepresentable
//!
//! Only ATOMIC, in-place, no-data-movement ops reach live data: the autonomous
//! path emits ONLY an [`AtomicGrowCommand`] (`ModifyVolume`). There is no
//! multi-step / copy / delete op TYPE on the autonomous path, so a half-completed
//! operation cannot be represented there (**truly-unrepresentable** in Rust; the
//! atomicity of the external `ModifyVolume` is a **C2 external-world ceiling** —
//! we trust EBS's complete-or-not-started guarantee). No autonomous path mutates
//! durable data. The single data-destroying op ([`ReleaseCommand`]) is
//! operator-gated, regenerable-only, `NotInUse`-witnessed, and fresh-this-tick.
//!
//! Pure + dependency-free + fully unit- and property-tested, like the band law it
//! guards. `/algorithmic-prowess-seal`: the bound is a typed cap, the witnesses
//! are refined types, the classification is a sum over two method-disjoint types.

use crate::Directionality;
use std::collections::BTreeSet;

/// One GiB, the storage base unit (bytes).
const GI: u64 = 1 << 30;

/// The absolute cap on ONE autonomous grow step: `+2Gi`. The `min(+2Gi, +20%)`
/// bound never lets a single autonomous adjustment jump more than this.
pub const GROW_STEP_ABS_CAP_BYTES: u64 = 2 * GI;

/// The proportional cap on ONE autonomous grow step, in basis points: `+20%`.
/// `grow_step_cap` takes `min(GROW_STEP_ABS_CAP_BYTES, current·20%)`.
pub const GROW_STEP_PCT_CAP_BPS: u64 = 2_000;

// ─────────────────────────────────────────────────────────────────────────────
// PER-TICK FRESHNESS — the marker every witness carries
// ─────────────────────────────────────────────────────────────────────────────

/// A monotonic per-reconcile-tick identity. Every safety witness is stamped with
/// the tick it was minted in; the Act step re-verifies the stamp equals the
/// current tick, so a witness carried across ticks (stale live state) is refused.
/// `Copy` — the marker is compared, not owned; the WITNESSES that carry it are
/// move-only.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TickId(u64);

impl TickId {
    /// The tick with the given ordinal.
    #[must_use]
    pub const fn new(ordinal: u64) -> Self {
        Self(ordinal)
    }

    /// The ordinal.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The next tick (saturating — the reconcile loop never wraps in practice).
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// INVARIANT 1 — FOOTPRINT OWNERSHIP (OwnedPvc — parse-time-rejected)
// ─────────────────────────────────────────────────────────────────────────────

/// A volume's LIVE identity, read from cluster state each tick — the input to the
/// ownership parse. Pure data; the parse ([`FootprintPolicy::try_own`]) decides
/// ownership, never the caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeIdentity {
    /// The PVC namespace.
    pub namespace: String,
    /// The PVC name.
    pub name: String,
    /// The value of the volume's `role` label, if present (the ownership label is
    /// `role: camelot`).
    pub role_label: Option<String>,
    /// The camelot ownership tag/annotation is present (an explicit owned marker).
    pub ownership_tag: bool,
}

impl VolumeIdentity {
    /// A bare identity carrying neither the role label nor the ownership tag — the
    /// resident-volume shape (rabbitmq / profiling / akeyless), owned iff its
    /// namespace is in the footprint's owned set.
    #[must_use]
    pub fn resident(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self { namespace: namespace.into(), name: name.into(), role_label: None, ownership_tag: false }
    }
}

/// The footprint predicate — which volumes the carve is ALLOWED to touch. A
/// volume is owned iff it carries the ownership tag, OR its `role` label equals
/// the owner value, OR its namespace is in the owned set. Everything else is a
/// resident structure the carve must never overstep.
#[derive(Clone, Debug)]
pub struct FootprintPolicy {
    owned_namespaces: BTreeSet<String>,
    owner_role_value: String,
}

impl FootprintPolicy {
    /// A custom footprint.
    #[must_use]
    pub fn new(owned_namespaces: impl IntoIterator<Item = String>, owner_role_value: impl Into<String>) -> Self {
        Self {
            owned_namespaces: owned_namespaces.into_iter().collect(),
            owner_role_value: owner_role_value.into(),
        }
    }

    /// The camelot footprint: namespaces `{camelot, camelot-build, tendril}` and
    /// the `role: camelot` label. The resident non-camelot namespaces
    /// (`rabbitmq`, `profiling`, the akeyless workloads) are NOT in the set, so a
    /// volume in them yields no [`OwnedPvc`].
    #[must_use]
    pub fn camelot() -> Self {
        Self::new(
            ["camelot".to_owned(), "camelot-build".to_owned(), "tendril".to_owned()],
            "camelot",
        )
    }

    /// The raw ownership predicate (module-internal; the public ingress is
    /// [`try_own`](Self::try_own), which additionally stamps the tick).
    fn owns(&self, id: &VolumeIdentity) -> bool {
        id.ownership_tag
            || id.role_label.as_deref() == Some(self.owner_role_value.as_str())
            || self.owned_namespaces.contains(&id.namespace)
    }

    /// **THE ownership parse — the ONLY ingress to [`OwnedPvc`].** Re-run EVERY
    /// TICK against live identity. A volume outside the footprint yields `None`
    /// (parse-time rejection) — acting on a resident structure has no code path.
    /// The returned witness is stamped with `tick`, so a later Act can prove it
    /// was minted from THIS tick's live state.
    #[must_use]
    pub fn try_own(&self, id: &VolumeIdentity, tick: TickId) -> Option<OwnedPvc> {
        if self.owns(id) {
            Some(OwnedPvc { namespace: id.namespace.clone(), name: id.name.clone(), tick })
        } else {
            None
        }
    }
}

/// A witness that a volume is in the carve's footprint, minted THIS tick. The
/// fields are PRIVATE and the only constructor is [`FootprintPolicy::try_own`],
/// so a non-owned volume is unrepresentable as an `OwnedPvc` (parse-time-rejected).
/// **NOT `Clone`** — move-only, so the Act consumes it and it cannot silently
/// drive two mutations; re-obtaining one requires re-parsing live identity.
#[derive(Debug)]
pub struct OwnedPvc {
    namespace: String,
    name: String,
    tick: TickId,
}

impl OwnedPvc {
    /// The owned PVC's namespace.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
    /// The owned PVC's name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// The tick this witness was minted in.
    #[must_use]
    pub fn tick(&self) -> TickId {
        self.tick
    }
    /// Was this witness minted in `current` (i.e. re-parsed from live state THIS
    /// tick)? A `false` means the ownership was proven in an earlier tick and
    /// must not drive an Act now.
    #[must_use]
    pub fn is_fresh(&self, current: TickId) -> bool {
        self.tick == current
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// INVARIANT 2 — RELEASE WITNESSES (Regenerable/Durable + NotInUse)
// ─────────────────────────────────────────────────────────────────────────────

/// The data role a durability classification rests on. The classification is
/// SAFE-BY-DEFAULT: only [`RebuildableFromSource`](DataRole::RebuildableFromSource)
/// yields a [`RegenerableVolume`]; everything else (including
/// [`Unknown`](DataRole::Unknown)) is [`DurableVolume`] — no release path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataRole {
    /// SaaS state — the bytes ARE the source of truth (mysql / rustfs / a
    /// database data dir). Durable; never releasable autonomously or by witness.
    SaasState,
    /// Rebuildable from an external source of truth (a cache, a scratch volume, a
    /// re-derivable index). The ONE role a release is safe for — and only with a
    /// `NotInUse` proof.
    RebuildableFromSource,
    /// Unclassified / ambiguous. Treated as durable (the safe default — we never
    /// destroy data we failed to classify).
    Unknown,
}

impl DataRole {
    /// Is this role PROVABLY regenerable? Only `RebuildableFromSource` is; the
    /// safe default (`SaasState`, `Unknown`) is not.
    #[must_use]
    pub const fn is_provably_regenerable(self) -> bool {
        matches!(self, Self::RebuildableFromSource)
    }
}

/// The facts a durability classification reads. Kept minimal + explicit — the
/// classification must be careful, so it rests on a declared/derived role, not a
/// guess.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DurabilityFacts {
    /// The volume's data role.
    pub role: DataRole,
}

impl DurabilityFacts {
    /// Facts for a given role.
    #[must_use]
    pub const fn of(role: DataRole) -> Self {
        Self { role }
    }
}

/// The classification of an owned volume by durability — a sum over two
/// METHOD-DISJOINT types. Pattern-matching it is the only way to reach a release:
/// the [`Regenerable`](VolumeClass::Regenerable) arm has one, the
/// [`Durable`](VolumeClass::Durable) arm structurally does not.
#[derive(Debug)]
pub enum VolumeClass {
    /// Rebuildable — carries a [`RegenerableVolume`] (has `release`/`recreate`).
    Regenerable(RegenerableVolume),
    /// Durable — carries a [`DurableVolume`] (NO release/recreate method).
    Durable(DurableVolume),
}

/// **THE durability parse.** Classify an owned volume; SAFE-BY-DEFAULT — a volume
/// is `Durable` unless PROVEN regenerable. Consumes the [`OwnedPvc`] (move — the
/// class carries the ownership witness forward, so a release still re-verifies
/// ownership freshness).
#[must_use]
pub fn classify_durability(owned: OwnedPvc, facts: &DurabilityFacts) -> VolumeClass {
    if facts.role.is_provably_regenerable() {
        VolumeClass::Regenerable(RegenerableVolume { owned })
    } else {
        VolumeClass::Durable(DurableVolume { owned })
    }
}

/// A volume classified REGENERABLE — its bytes can be rebuilt from an external
/// source of truth. The ONE type that exposes a destructive
/// [`release`](Self::release)/[`recreate`](Self::recreate), and only behind a
/// [`NotInUse`] proof + a per-tick freshness re-check.
#[derive(Debug)]
pub struct RegenerableVolume {
    owned: OwnedPvc,
}

impl RegenerableVolume {
    /// The ownership witness this classification carries.
    #[must_use]
    pub fn owned(&self) -> &OwnedPvc {
        &self.owned
    }

    /// **THE release — the operator-gated, data-destroying op.** Requires the
    /// [`OwnedPvc`] (carried by `self`) AND a [`NotInUse`] proof, and both must be
    /// fresh THIS tick. A [`DurableVolume`] has NO such method → releasing durable
    /// data is a compile error; an in-use volume mints no `NotInUse` → releasing
    /// an in-use block has no code path. Consumes both witnesses (move), so the
    /// release cannot be replayed. Emits a [`ReleaseCommand`] the operator path
    /// executes atomically through the workload's lifecycle (never a raw
    /// concurrent delete).
    ///
    /// # Errors
    /// [`CarveRefused::StaleOwnershipWitness`] / [`CarveRefused::StaleNotInUseWitness`]
    /// if either witness was minted in an earlier tick (stale live state).
    pub fn release(self, proof: NotInUse, current: TickId) -> Result<ReleaseCommand, CarveRefused> {
        if !self.owned.is_fresh(current) {
            return Err(CarveRefused::StaleOwnershipWitness { minted: self.owned.tick(), current });
        }
        if !proof.is_fresh(current) {
            return Err(CarveRefused::StaleNotInUseWitness { minted: proof.tick(), current });
        }
        Ok(ReleaseCommand {
            namespace: self.owned.namespace,
            name: self.owned.name,
            recreate_to: None,
            tick: current,
        })
    }

    /// **RECREATE — release then re-provision at `new_size`.** The reclaim path
    /// for an externally over-provisioned regenerable volume (grow-only cannot
    /// shrink in place). Same witnesses + freshness as [`release`](Self::release);
    /// the resulting [`ReleaseCommand`] carries the target size so the operator
    /// path recreates atomically after the release.
    ///
    /// # Errors
    /// As [`release`](Self::release).
    pub fn recreate(self, proof: NotInUse, current: TickId, new_size: u64) -> Result<ReleaseCommand, CarveRefused> {
        let mut cmd = self.release(proof, current)?;
        cmd.recreate_to = Some(new_size);
        Ok(cmd)
    }
}

/// A volume classified DURABLE — SaaS state whose bytes are the source of truth.
/// It carries an [`OwnedPvc`] (so it can still be GROWN — grow is atomic +
/// non-destructive) but has NO `release`/`recreate` method: releasing durable
/// data is a COMPILE ERROR (truly-unrepresentable — there is no method to call).
#[derive(Debug)]
pub struct DurableVolume {
    owned: OwnedPvc,
}

impl DurableVolume {
    /// The ownership witness this classification carries (for a grow — never a
    /// release; no release method exists here).
    #[must_use]
    pub fn owned(&self) -> &OwnedPvc {
        &self.owned
    }

    /// Consume the class back into its [`OwnedPvc`] to drive a grow (the only safe
    /// mutation for durable data). There is deliberately NO release/recreate here.
    #[must_use]
    pub fn into_owned(self) -> OwnedPvc {
        self.owned
    }
}

/// The live PVC→pod reachability facts, read EACH TICK — the input to the
/// [`NotInUse`] proof. A PVC is IN USE iff some live pod references it (reachable
/// in the PVC→pod dependency graph). An orphan — zero live consumers AND detached
/// — is unreachable, hence safe to release. This composes the isolation
/// reachability seam (orphan = unreachable): if a pod uses it, there is no proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConsumerReachability {
    /// The number of live pods currently referencing this PVC (reachable
    /// consumers). `> 0` ⇒ in use ⇒ no [`NotInUse`] witness.
    pub live_consumer_pods: u32,
    /// Is the volume currently attached to a node? `true` ⇒ in use.
    pub attached: bool,
}

impl ConsumerReachability {
    /// Is this volume an ORPHAN — no live consumer pod AND detached — hence
    /// unreachable and safe to release?
    #[must_use]
    pub const fn is_orphan(self) -> bool {
        self.live_consumer_pods == 0 && !self.attached
    }
}

/// A proof that a volume is DETACHED and has NO live consumer pod THIS tick.
/// Minted ONLY from live reachability facts via [`prove_not_in_use`]; a volume
/// with any live consumer yields none. **NOT `Clone`** — move-only, tick-fresh,
/// consumed by [`RegenerableVolume::release`].
#[derive(Debug)]
pub struct NotInUse {
    tick: TickId,
}

impl NotInUse {
    /// The tick this proof was minted in.
    #[must_use]
    pub fn tick(&self) -> TickId {
        self.tick
    }
    /// Was this proof minted THIS tick (re-proven against live reachability)?
    #[must_use]
    pub fn is_fresh(&self, current: TickId) -> bool {
        self.tick == current
    }
}

/// **THE not-in-use proof.** Prove a volume is detached + has no live consumer
/// THIS tick, from live reachability facts. An in-use volume yields `None` — the
/// release path has no witness to present. Re-run every tick, never cached.
#[must_use]
pub fn prove_not_in_use(reach: ConsumerReachability, tick: TickId) -> Option<NotInUse> {
    if reach.is_orphan() {
        Some(NotInUse { tick })
    } else {
        None
    }
}

/// The operator-gated, data-destroying plan a witnessed release emits. It is a
/// VALUE (declare, not execute) — the operator path realizes it through the
/// workload's lifecycle (drain → detach → delete → optional recreate) atomically,
/// never a raw concurrent delete. Produced ONLY by [`RegenerableVolume`] behind
/// both witnesses; a durable volume can never construct one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReleaseCommand {
    /// The PVC namespace.
    pub namespace: String,
    /// The PVC name.
    pub name: String,
    /// `Some(size)` ⇒ recreate at that size after release; `None` ⇒ pure release.
    pub recreate_to: Option<u64>,
    /// The tick this release was authorized in.
    pub tick: TickId,
}

// ─────────────────────────────────────────────────────────────────────────────
// INVARIANT 3 — SMALL + ATOMIC + GROW-ONLY (the autonomous path)
// ─────────────────────────────────────────────────────────────────────────────

/// The typed cap on ONE autonomous grow step: `min(+2Gi, +20% of current)` — the
/// smaller of the absolute and proportional caps. Never a big jump. Pure.
#[must_use]
pub fn grow_step_cap(current: u64) -> u64 {
    // 20% == current * 2000 / 10000 == current / 5. Integer, no float.
    let proportional = current / (10_000 / GROW_STEP_PCT_CAP_BPS); // current / 5
    proportional.min(GROW_STEP_ABS_CAP_BYTES)
}

/// A bounded, atomic, GROW-ONLY step — the only autonomous storage action. Its
/// fields are PRIVATE and the only constructor is [`AutonomousStorageCarve::plan_grow`],
/// which clamps `to ≤ from + grow_step_cap(from)`, so a `SmallAtomicGrow` whose
/// delta exceeds the cap is unrepresentable (parse-time-rejected). `to > from`
/// always (it is a grow). **NOT `Clone`** — move-only, tick-fresh, consumed by the
/// atomic commit.
#[derive(Debug)]
pub struct SmallAtomicGrow {
    from: u64,
    to: u64,
    tick: TickId,
}

impl SmallAtomicGrow {
    /// The size before the grow.
    #[must_use]
    pub fn from(&self) -> u64 {
        self.from
    }
    /// The size after the grow (`> from`, `≤ from + grow_step_cap(from)`).
    #[must_use]
    pub fn to(&self) -> u64 {
        self.to
    }
    /// The increment (`> 0`, `≤ grow_step_cap(from)`).
    #[must_use]
    pub fn delta(&self) -> u64 {
        self.to - self.from
    }
    /// The tick this step was planned in.
    #[must_use]
    pub fn tick(&self) -> TickId {
        self.tick
    }
    /// Was this step planned THIS tick?
    #[must_use]
    pub fn is_fresh(&self, current: TickId) -> bool {
        self.tick == current
    }
}

/// The AUTONOMOUS storage carve path — the ongoing live band, expressed as the
/// `GrowOnly` TYPESTATE. By construction it can ONLY emit a [`SmallAtomicGrow`]:
/// there is no `shrink`, `release`, or unbounded-jump method on this type. The
/// grow-only-ness that used to be a config flag ([`Directionality::GrowOnly`]) is
/// now the type of the path, so the ongoing loop cannot overstep or release by
/// construction (truly-unrepresentable — the methods do not exist). Shrink /
/// release live ONLY on the operator-gated [`RegenerableVolume`] path behind the
/// two release witnesses.
#[derive(Clone, Copy, Debug)]
pub struct AutonomousStorageCarve;

impl AutonomousStorageCarve {
    /// The directionality this path enforces — always [`Directionality::GrowOnly`].
    /// The runtime witness that the typestate and the config-level clamp agree.
    #[must_use]
    pub const fn directionality(self) -> Directionality {
        Directionality::GrowOnly
    }

    /// Plan ONE bounded, grow-only step toward `desired_target` (typically the
    /// `provision_target` the band computed). Returns `None` when no grow is
    /// warranted (`desired_target ≤ current` — grow-only NEVER shrinks) or when
    /// the current size is too small to make a bounded step (`grow_step_cap == 0`,
    /// only for sub-`5`-byte sizes a real storage volume never has). Otherwise a
    /// [`SmallAtomicGrow`] with `to = min(desired_target, from + cap)` — a bounded
    /// increment, minted THIS tick. NEVER a shrink, release, or big jump.
    #[must_use]
    pub fn plan_grow(self, current: u64, desired_target: u64, tick: TickId) -> Option<SmallAtomicGrow> {
        if desired_target <= current {
            return None; // grow-only: a shrink/hold has no autonomous code path
        }
        let cap = grow_step_cap(current);
        if cap == 0 {
            return None; // cannot make a bounded step (degenerate sub-5-byte size)
        }
        let step_ceiling = current.saturating_add(cap);
        let to = desired_target.min(step_ceiling);
        Some(SmallAtomicGrow { from: current, to, tick })
    }
}

/// The live in-flight-operation state of a volume, read EACH TICK. A volume with
/// a `ModifyVolume` / CSI resize already in flight must never be acted on again —
/// double-acting a mid-operation volume is the corruption hazard the per-tick
/// commit refuses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VolumeOpState {
    /// No operation in flight — safe to start an atomic grow.
    Idle,
    /// A `ModifyVolume` / resize is already in flight — do NOT act again.
    ResizeInProgress,
}

/// The ONLY executable storage mutation the actuator runs — an EBS online-expand
/// (`ModifyVolume`): in-place, atomic (complete-or-not-started), no data movement,
/// no delete. There is deliberately no copy/recreate/multi-step op TYPE on the
/// autonomous path, so a half-completed operation cannot be represented here.
/// Constructed ONLY by [`commit_small_atomic_grow`] after the per-tick re-verify.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AtomicGrowCommand {
    /// The PVC namespace.
    pub namespace: String,
    /// The PVC name.
    pub name: String,
    /// The size before the expand.
    pub from: u64,
    /// The size after the expand.
    pub to: u64,
    /// The tick this command was committed in.
    pub tick: TickId,
}

/// Why a carve action was refused at the per-tick commit — a typed, observable
/// reason, never a silent no-op.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CarveRefused {
    /// The ownership witness was minted in an earlier tick — ownership may have
    /// flipped; re-parse live identity before acting.
    StaleOwnershipWitness { minted: TickId, current: TickId },
    /// The grow step was planned in an earlier tick — re-plan against live usage.
    StaleGrowWitness { minted: TickId, current: TickId },
    /// The not-in-use proof was minted in an earlier tick — a consumer may have
    /// attached; re-prove against live reachability.
    StaleNotInUseWitness { minted: TickId, current: TickId },
    /// A `ModifyVolume` / resize is already in flight — acting again could
    /// double-apply; wait for the current op to settle.
    VolumeMidOperation,
}

impl std::fmt::Display for CarveRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CarveRefused::StaleOwnershipWitness { minted, current } => write!(
                f,
                "ownership witness is stale (minted tick {}, current tick {}) — re-parse live identity",
                minted.get(),
                current.get()
            ),
            CarveRefused::StaleGrowWitness { minted, current } => write!(
                f,
                "grow step is stale (planned tick {}, current tick {}) — re-plan against live usage",
                minted.get(),
                current.get()
            ),
            CarveRefused::StaleNotInUseWitness { minted, current } => write!(
                f,
                "not-in-use proof is stale (proven tick {}, current tick {}) — re-prove reachability",
                minted.get(),
                current.get()
            ),
            CarveRefused::VolumeMidOperation => {
                write!(f, "a ModifyVolume/resize is already in flight — never double-act a mid-operation volume")
            }
        }
    }
}

impl std::error::Error for CarveRefused {}

/// **THE per-tick atomic commit.** Consumes the [`OwnedPvc`] (minted this tick)
/// and the [`SmallAtomicGrow`] (planned this tick), re-verifies EVERY boundary
/// against live state THIS tick, and only then yields an [`AtomicGrowCommand`] the
/// actuator may execute. Refuses if either witness is stale (a boundary that
/// flipped since it was minted) or the volume is mid-operation (never double-act).
/// Because both witnesses are moved in (`!Clone`), a witness cannot drive two
/// Acts, and a stale witness carried from a prior tick is caught by the freshness
/// re-check — so "verified per tick" is structural, not a hope.
///
/// # Errors
/// [`CarveRefused`] when a boundary does not re-verify against live state this tick.
pub fn commit_small_atomic_grow(
    current: TickId,
    owned: OwnedPvc,
    grow: SmallAtomicGrow,
    op_state: VolumeOpState,
) -> Result<AtomicGrowCommand, CarveRefused> {
    if !owned.is_fresh(current) {
        return Err(CarveRefused::StaleOwnershipWitness { minted: owned.tick(), current });
    }
    if !grow.is_fresh(current) {
        return Err(CarveRefused::StaleGrowWitness { minted: grow.tick(), current });
    }
    if matches!(op_state, VolumeOpState::ResizeInProgress) {
        return Err(CarveRefused::VolumeMidOperation);
    }
    Ok(AtomicGrowCommand {
        namespace: owned.namespace,
        name: owned.name,
        from: grow.from,
        to: grow.to,
        tick: current,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const MI: u64 = 1 << 20;

    fn t(n: u64) -> TickId {
        TickId::new(n)
    }

    // ── INVARIANT 1: footprint ownership (parse-time-rejected) ─────────────────

    #[test]
    fn a_camelot_volume_yields_an_owned_pvc() {
        let fp = FootprintPolicy::camelot();
        // by namespace
        assert!(fp.try_own(&VolumeIdentity::resident("camelot", "data-mysql-0"), t(1)).is_some());
        // by role label
        let labeled = VolumeIdentity { role_label: Some("camelot".into()), ..VolumeIdentity::resident("other", "v") };
        assert!(fp.try_own(&labeled, t(1)).is_some());
        // by ownership tag
        let tagged = VolumeIdentity { ownership_tag: true, ..VolumeIdentity::resident("other", "v") };
        assert!(fp.try_own(&tagged, t(1)).is_some());
    }

    #[test]
    fn a_resident_non_camelot_volume_yields_no_owned_pvc() {
        // THE overstep guard: rabbitmq / profiling / akeyless volumes have NO
        // OwnedPvc — acting on them is unrepresentable (no witness to pass the
        // actuator commit).
        let fp = FootprintPolicy::camelot();
        for ns in ["rabbitmq", "profiling", "akeyless", "kube-system", "default"] {
            let id = VolumeIdentity::resident(ns, "some-data");
            assert!(fp.try_own(&id, t(1)).is_none(), "{ns} volume must not be ownable");
        }
    }

    #[test]
    fn owned_pvc_carries_the_tick_it_was_minted_in() {
        let fp = FootprintPolicy::camelot();
        let owned = fp.try_own(&VolumeIdentity::resident("camelot", "v"), t(7)).unwrap();
        assert!(owned.is_fresh(t(7)));
        assert!(!owned.is_fresh(t(8)));
    }

    // ── INVARIANT 2: release witnesses (durable-no-method + NotInUse) ──────────

    #[test]
    fn a_durable_volume_has_no_release_method() {
        // Compile-time proof: DurableVolume exposes no release/recreate. The
        // classification of SaaS state is Durable, so releasing durable data has
        // no code path. (If a `release` were added to DurableVolume, this test's
        // sibling doc-test / the type would change — the invariant is the ABSENCE
        // of the method.)
        let fp = FootprintPolicy::camelot();
        let owned = fp.try_own(&VolumeIdentity::resident("camelot", "data-mysql-0"), t(1)).unwrap();
        match classify_durability(owned, &DurabilityFacts::of(DataRole::SaasState)) {
            VolumeClass::Durable(d) => {
                // The ONLY thing we can do is read ownership / grow — no release.
                assert_eq!(d.owned().name(), "data-mysql-0");
            }
            VolumeClass::Regenerable(_) => panic!("SaaS state must classify Durable"),
        }
    }

    #[test]
    fn unknown_and_saas_roles_default_to_durable() {
        let fp = FootprintPolicy::camelot();
        for role in [DataRole::SaasState, DataRole::Unknown] {
            let owned = fp.try_own(&VolumeIdentity::resident("camelot", "v"), t(1)).unwrap();
            assert!(
                matches!(classify_durability(owned, &DurabilityFacts::of(role)), VolumeClass::Durable(_)),
                "{role:?} must be durable (safe default)"
            );
        }
    }

    #[test]
    fn a_regenerable_volume_releases_only_with_a_not_in_use_proof() {
        let fp = FootprintPolicy::camelot();
        let owned = fp.try_own(&VolumeIdentity::resident("camelot", "scratch-cache"), t(3)).unwrap();
        let VolumeClass::Regenerable(vol) =
            classify_durability(owned, &DurabilityFacts::of(DataRole::RebuildableFromSource))
        else {
            panic!("rebuildable-from-source must classify Regenerable");
        };
        // orphan → a NotInUse proof exists → release succeeds this tick.
        let proof = prove_not_in_use(ConsumerReachability { live_consumer_pods: 0, attached: false }, t(3)).unwrap();
        let cmd = vol.release(proof, t(3)).unwrap();
        assert_eq!(cmd.name, "scratch-cache");
        assert_eq!(cmd.recreate_to, None);
    }

    #[test]
    fn an_in_use_volume_mints_no_not_in_use_proof() {
        // THE in-use guard: any live consumer OR an attached volume → no witness,
        // so the release path cannot be entered (no proof to pass).
        assert!(prove_not_in_use(ConsumerReachability { live_consumer_pods: 1, attached: false }, t(1)).is_none());
        assert!(prove_not_in_use(ConsumerReachability { live_consumer_pods: 0, attached: true }, t(1)).is_none());
        assert!(prove_not_in_use(ConsumerReachability { live_consumer_pods: 3, attached: true }, t(1)).is_none());
        // only a true orphan mints one.
        assert!(prove_not_in_use(ConsumerReachability { live_consumer_pods: 0, attached: false }, t(1)).is_some());
    }

    #[test]
    fn recreate_carries_the_target_size() {
        let fp = FootprintPolicy::camelot();
        let owned = fp.try_own(&VolumeIdentity::resident("camelot", "idx"), t(2)).unwrap();
        let VolumeClass::Regenerable(vol) =
            classify_durability(owned, &DurabilityFacts::of(DataRole::RebuildableFromSource))
        else {
            unreachable!()
        };
        let proof = prove_not_in_use(ConsumerReachability { live_consumer_pods: 0, attached: false }, t(2)).unwrap();
        let cmd = vol.recreate(proof, t(2), 8 * GI).unwrap();
        assert_eq!(cmd.recreate_to, Some(8 * GI));
    }

    #[test]
    fn a_stale_not_in_use_proof_is_refused() {
        // Per-tick: a proof minted last tick cannot authorize a release this tick.
        let fp = FootprintPolicy::camelot();
        let owned = fp.try_own(&VolumeIdentity::resident("camelot", "cache"), t(5)).unwrap();
        let VolumeClass::Regenerable(vol) =
            classify_durability(owned, &DurabilityFacts::of(DataRole::RebuildableFromSource))
        else {
            unreachable!()
        };
        let stale_proof = prove_not_in_use(ConsumerReachability { live_consumer_pods: 0, attached: false }, t(4)).unwrap();
        let err = vol.release(stale_proof, t(5)).unwrap_err();
        assert!(matches!(err, CarveRefused::StaleNotInUseWitness { .. }));
    }

    #[test]
    fn a_stale_ownership_witness_is_refused_on_release() {
        let fp = FootprintPolicy::camelot();
        let owned = fp.try_own(&VolumeIdentity::resident("camelot", "cache"), t(4)).unwrap();
        let VolumeClass::Regenerable(vol) =
            classify_durability(owned, &DurabilityFacts::of(DataRole::RebuildableFromSource))
        else {
            unreachable!()
        };
        // ownership was proven tick 4; this tick is 6.
        let proof = prove_not_in_use(ConsumerReachability { live_consumer_pods: 0, attached: false }, t(6)).unwrap();
        let err = vol.release(proof, t(6)).unwrap_err();
        assert!(matches!(err, CarveRefused::StaleOwnershipWitness { .. }));
    }

    // ── INVARIANT 3: small + atomic + grow-only ────────────────────────────────

    #[test]
    fn the_grow_step_cap_is_min_of_2gi_and_20pct() {
        // small volume: 20% binds (2Gi is larger).
        assert_eq!(grow_step_cap(2 * GI), (2 * GI) / 5); // 20% of 2Gi ≈ 410Mi
        // large volume: the 2Gi absolute binds (20% is larger).
        assert_eq!(grow_step_cap(100 * GI), 2 * GI);
        // boundary: 10Gi → 20% = 2Gi == abs cap.
        assert_eq!(grow_step_cap(10 * GI), 2 * GI);
        // 11Gi → 20% = 2.2Gi > 2Gi ⇒ abs cap binds.
        assert_eq!(grow_step_cap(11 * GI), 2 * GI);
    }

    #[test]
    fn an_autonomous_step_is_always_a_bounded_grow_never_a_shrink() {
        let carve = AutonomousStorageCarve;
        // grow-only: a desired target at or below current yields NO step.
        assert!(carve.plan_grow(10 * GI, 10 * GI, t(1)).is_none());
        assert!(carve.plan_grow(10 * GI, 4 * GI, t(1)).is_none(), "never shrinks");
        // a modest grow lands exactly at the desired target (within the cap).
        let g = carve.plan_grow(10 * GI, 11 * GI, t(1)).unwrap();
        assert_eq!(g.to(), 11 * GI);
        assert_eq!(g.delta(), GI);
        // a BIG desired jump is CLAMPED to from + cap (never an unbounded jump).
        let g = carve.plan_grow(10 * GI, 500 * GI, t(1)).unwrap();
        assert_eq!(g.to(), 10 * GI + 2 * GI, "clamped to +2Gi cap");
        assert_eq!(g.delta(), 2 * GI);
    }

    #[test]
    fn the_directionality_typestate_agrees_with_the_config_clamp() {
        assert_eq!(AutonomousStorageCarve.directionality(), Directionality::GrowOnly);
    }

    #[test]
    fn an_atomic_grow_commits_only_when_every_boundary_re_verifies_this_tick() {
        let fp = FootprintPolicy::camelot();
        let owned = fp.try_own(&VolumeIdentity::resident("camelot", "data-0"), t(9)).unwrap();
        let grow = AutonomousStorageCarve.plan_grow(10 * GI, 12 * GI, t(9)).unwrap();
        let cmd = commit_small_atomic_grow(t(9), owned, grow, VolumeOpState::Idle).unwrap();
        assert_eq!(cmd.from, 10 * GI);
        assert_eq!(cmd.to, 12 * GI);
        assert_eq!(cmd.name, "data-0");
    }

    #[test]
    fn a_stale_ownership_witness_cannot_drive_an_act() {
        // ownership minted tick 9, grow planned tick 10, committing at tick 10 →
        // the ownership witness is stale → refused (re-parse required).
        let fp = FootprintPolicy::camelot();
        let owned = fp.try_own(&VolumeIdentity::resident("camelot", "data-0"), t(9)).unwrap();
        let grow = AutonomousStorageCarve.plan_grow(10 * GI, 12 * GI, t(10)).unwrap();
        let err = commit_small_atomic_grow(t(10), owned, grow, VolumeOpState::Idle).unwrap_err();
        assert!(matches!(err, CarveRefused::StaleOwnershipWitness { .. }));
    }

    #[test]
    fn a_stale_grow_witness_cannot_drive_an_act() {
        let fp = FootprintPolicy::camelot();
        let owned = fp.try_own(&VolumeIdentity::resident("camelot", "data-0"), t(10)).unwrap();
        let grow = AutonomousStorageCarve.plan_grow(10 * GI, 12 * GI, t(9)).unwrap(); // stale
        let err = commit_small_atomic_grow(t(10), owned, grow, VolumeOpState::Idle).unwrap_err();
        assert!(matches!(err, CarveRefused::StaleGrowWitness { .. }));
    }

    #[test]
    fn a_mid_operation_volume_is_never_double_acted() {
        // fresh witnesses, but a ModifyVolume is already in flight → refused.
        let fp = FootprintPolicy::camelot();
        let owned = fp.try_own(&VolumeIdentity::resident("camelot", "data-0"), t(3)).unwrap();
        let grow = AutonomousStorageCarve.plan_grow(10 * GI, 12 * GI, t(3)).unwrap();
        let err = commit_small_atomic_grow(t(3), owned, grow, VolumeOpState::ResizeInProgress).unwrap_err();
        assert_eq!(err, CarveRefused::VolumeMidOperation);
    }

    // ── property tests ─────────────────────────────────────────────────────────

    #[test]
    fn prop_an_autonomous_plan_is_always_a_bounded_grow() {
        // For a wide sweep of (current, desired), an autonomous plan is either
        // None (no grow warranted) or a SmallAtomicGrow with 0 < delta ≤ cap and
        // to ≤ desired. NEVER a shrink, never an unbounded jump.
        let carve = AutonomousStorageCarve;
        let currents = [2 * GI, 3 * GI, 10 * GI, 47 * GI, 200 * GI, 1024 * GI];
        for &cur in &currents {
            for mult in 0..40u64 {
                let desired = (cur * mult) / 8; // sweep below and far above `cur`
                match carve.plan_grow(cur, desired, t(1)) {
                    None => assert!(desired <= cur, "None only when no grow warranted"),
                    Some(g) => {
                        assert!(g.to() > g.from(), "always a grow");
                        assert!(g.delta() > 0);
                        assert!(g.delta() <= grow_step_cap(cur), "delta {} exceeds cap {}", g.delta(), grow_step_cap(cur));
                        assert!(g.to() <= desired, "never overshoots the desired target");
                        assert!(g.to() <= cur + grow_step_cap(cur), "never an unbounded jump");
                    }
                }
            }
        }
    }

    #[test]
    fn prop_a_release_requires_both_witnesses_no_witness_no_release() {
        // Only an orphan (no consumer + detached) mints a NotInUse; without it,
        // the release path cannot be constructed. Sweep the reachability space.
        for consumers in 0..4u32 {
            for attached in [false, true] {
                let reach = ConsumerReachability { live_consumer_pods: consumers, attached };
                let witness = prove_not_in_use(reach, t(1));
                assert_eq!(
                    witness.is_some(),
                    consumers == 0 && !attached,
                    "a NotInUse proof exists iff the volume is a true orphan"
                );
            }
        }
    }

    #[test]
    fn prop_a_non_owned_volume_yields_no_owned_pvc() {
        // Sweep resident namespaces: none are ownable under the camelot footprint.
        let fp = FootprintPolicy::camelot();
        for ns in ["rabbitmq", "profiling", "akeyless", "monitoring", "istio-system", "flux-system"] {
            assert!(fp.try_own(&VolumeIdentity::resident(ns, "d"), t(1)).is_none());
        }
        // and every owned namespace is ownable.
        for ns in ["camelot", "camelot-build", "tendril"] {
            assert!(fp.try_own(&VolumeIdentity::resident(ns, "d"), t(1)).is_some());
        }
    }

    #[test]
    fn prop_grow_step_cap_never_exceeds_either_bound() {
        for kib in [1u64, 512, 2 * MI, 100 * MI, GI, 7 * GI, 10 * GI, 40 * GI, 4096 * GI] {
            let cap = grow_step_cap(kib);
            assert!(cap <= GROW_STEP_ABS_CAP_BYTES, "cap {cap} exceeds abs bound");
            assert!(cap <= kib / 5, "cap {cap} exceeds 20% of {kib}");
        }
    }
}
