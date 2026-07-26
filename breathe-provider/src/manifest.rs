// Same rationale as `request.rs`: this module's prose names Flux concepts
// ("ImagePolicy", "ImageUpdateAutomation", "Setters") and English acronyms
// ("QoS", "YAML") that are NOT code items in this crate. Backticking them would
// render as code spans and make the prose worse.
#![allow(clippy::doc_markdown)]

//! **DOOR 2 — the durable, git-visible write.** The half of the REQUEST
//! dimension that makes a converged value survive a rollout.
//!
//! # The problem this module exists to close
//!
//! The in-place resize subresource mutates the **pod**, not the
//! Deployment/StatefulSet **template**. So a request change written that way is
//!
//! 1. **lost on the next rollout**, and
//! 2. **invisible in git** — a straight violation of GITOPS-NATIVE, where the
//!    cluster is a projection of the git tree.
//!
//! For a LIMIT the fleet has tolerated that. For a REQUEST it is materially
//! worse, because the request is what sets `oom_score_adj` and the QoS class:
//! the protection would silently evaporate at exactly the moment (a rollout)
//! when things are already moving. That is illegal state **I5**, and nothing in
//! the in-place door can fix it — the durable value has to reach the committed
//! manifest.
//!
//! # Why this is a new primitive rather than a reused one
//!
//! Every candidate was checked before writing a line, per Operating Principle
//! #1 (extend the near-miss; never invent a second mechanism):
//!
//! | candidate | verdict |
//! |---|---|
//! | `outorga::PromotionPolicy` (formigueiro) | **REUSED, already in-tree** — `breathe-crd` depends on it and `resolve_gate` already runs its shadow→confirm→effect FSM. The durable door rides the *same* gate every band rides; this module adds no second promotion algebra. |
//! | tend's `GitOps` trait | **SHAPE reused** — a mockable seam with an is-clean refusal. Narrowed here to the three methods a writer actually needs, rather than copied whole. |
//! | Flux `ImageUpdateAutomation` | **CANNOT be reused.** Its only strategy is `Setters`, which resolves a `$imagepolicy` marker to an ImagePolicy's image/tag/digest. It cannot write `"384Mi"`. Its *shape* is imitated — trailing marker on the value line, an allowed-path prefix as a hard blast-radius wall, the commit log as the audit trail. |
//! | a `serde_yaml` round-trip | **FORBIDDEN.** It destroys comments — which on a Flux-managed manifest means deleting adjacent `$imagepolicy` markers and silently breaking image automation. A durable-write mechanism that breaks the *other* automation on the same file is not a fix. |
//!
//! So the one genuinely new piece of algebra is
//! [`set_scalar_at_marker`]: a **byte-exact, comment-preserving,
//! marker-anchored scalar setter**. It never parses the document. It finds one
//! line, replaces one span inside it, and leaves every other byte in the file
//! identical — which is the only way to guarantee the surrounding comments (and
//! therefore the neighbouring automation) survive.
//!
//! # The marker
//!
//! ```yaml
//!         resources:
//!           requests:
//!             memory: 512Mi # {"$breathe": "camelot-build/sui-request"}
//!           limits:
//!             memory: 6Gi
//! ```
//!
//! The marker is an explicit, operator-authored opt-in. There is no inference,
//! no path-walking, and no "breathe found a likely-looking field": **a file
//! nobody marked cannot be written to**, because the setter's only anchor is a
//! marker that a human put there. That is the blast-radius floor, below the
//! allowed-prefix wall.
//!
//! # What is NOT built, said plainly
//!
//! The **transport** — an actual git client or Contents-API caller — is not in
//! this module and not in this crate. [`ManifestRepo`] is the seam; the only
//! implementations that exist are the in-memory one used by the tests and
//! whatever a controller later injects. So this module is provable end-to-end
//! with no cluster and no network, and it commits nothing on its own.
//!
//! A **class transition** still cannot be committed by a band authored with a
//! single marker, and that is now a *typed* refusal rather than a silence:
//! promoting Burstable→Guaranteed means setting every request equal to every
//! limit across every container simultaneously, so it needs one marker per
//! changed scalar. [`AddressedProposal::transition`] demands exactly that and
//! returns [`CoordinateGap`] naming each scalar it has no marker for.

use async_trait::async_trait;

use crate::gate::LiveWitness;
use crate::request::{ClassTransitionProposal, CommitReceipt, ContentAddr, WriterError};

// ─────────────────────────────────────────────────────────────────────────────
// The coordinate — WHERE in git a value lives
// ─────────────────────────────────────────────────────────────────────────────

/// Where in the git tree a band's carved value comes to rest.
///
/// Both halves are operator-authored. `path` is repo-relative; `marker` is the
/// id that must appear in a `{"$breathe": "<marker>"}` comment on the value
/// line. Neither is inferred, and there is deliberately no default: a band with
/// no coordinate reports [`crate::request::ClassTransitionBlocked`] rather than
/// guessing at a file.
///
/// Carries `JsonSchema` and is used **directly** as the `spec.manifestRef`
/// field type — deliberately not mirrored into `breathe-crd` the way
/// `QosClass`/`WorkloadClass` are. Those need a mirror because their upstream
/// (`breathe-invariant`) has no schemars dep; this type is born here, so a
/// mirror would be a second declaration of one shape, buying nothing and owing
/// a drift test forever.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ManifestCoordinate {
    /// Repo-relative path to the manifest, e.g.
    /// `clusters/camelot/apps/sui/release.yaml`.
    pub path: String,
    /// The marker id anchoring the exact scalar, e.g. `camelot-build/sui-request`.
    pub marker: String,
}

impl ManifestCoordinate {
    #[must_use]
    pub fn new(path: impl Into<String>, marker: impl Into<String>) -> Self {
        Self { path: path.into(), marker: marker.into() }
    }
}

/// One `marker → value` assignment inside a single file.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScalarAssignment {
    pub marker: String,
    /// The rendered scalar exactly as it should appear, e.g. `384Mi` or `250m`.
    pub value: String,
}

/// What produced a durable write — carried into the commit message so the git
/// log answers "why did this number move" without a second lookup.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase", tag = "origin")]
pub enum ProposalOrigin {
    /// A within-class request carve — the common case, and the one that closes
    /// I5 (the converged value surviving a rollout).
    Carve { resource: String, container: String },
    /// A QoS-class transition. Needs one marker per changed scalar.
    Transition { from: String, to: String },
}

// ─────────────────────────────────────────────────────────────────────────────
// The addressed proposal — the ONLY currency the durable door accepts
// ─────────────────────────────────────────────────────────────────────────────

/// **A durable write that knows where it lands.**
///
/// The fields are private and the constructors are the only producers, so an
/// [`AddressedProposal`] cannot exist without a path, a non-empty assignment
/// list, and a content address over both. That is what makes "a proposal with
/// no manifest coordinate reaches the writer" *unrepresentable at the caller*
/// rather than a runtime `None` check inside every writer implementation.
///
/// It is also **not** an [`crate::SsaPatch`] and never converts to one — the
/// same disjointness the in-place door relies on, held on this side too.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AddressedProposal {
    path: String,
    assignments: Vec<ScalarAssignment>,
    origin: ProposalOrigin,
    addr: ContentAddr,
}

impl AddressedProposal {
    /// A within-class carve: one marker, one value. Infallible — a single
    /// coordinate is exactly what one scalar needs.
    #[must_use]
    pub fn carve(coord: &ManifestCoordinate, value: impl Into<String>, resource: &str, container: &str) -> Self {
        let assignments =
            vec![ScalarAssignment { marker: coord.marker.clone(), value: value.into() }];
        let origin =
            ProposalOrigin::Carve { resource: resource.to_owned(), container: container.to_owned() };
        let addr = address_of(&coord.path, &assignments, &origin);
        Self { path: coord.path.clone(), assignments, origin, addr }
    }

    /// A class transition: **one marker per changed scalar**, or nothing.
    ///
    /// This is where the M0 boundary is stated as a type rather than a caveat.
    /// A band authored with a single `manifestRef` supplies one marker, so a
    /// Guaranteed promotion touching three scalars gets a [`CoordinateGap`]
    /// naming the two it cannot address — instead of a partial commit that
    /// leaves the workload in a class nobody asked for.
    ///
    /// # Errors
    ///
    /// [`CoordinateGap`] listing every changed scalar with no marker, and
    /// [`CoordinateGap::empty`] when the block changes nothing at all.
    pub fn transition(
        path: impl Into<String>,
        proposal: &ClassTransitionProposal,
        markers: &[(String, String)],
    ) -> Result<Self, CoordinateGap> {
        let want = required_scalars(proposal);
        if want.is_empty() {
            return Err(CoordinateGap::empty());
        }
        let mut assignments = Vec::with_capacity(want.len());
        let mut missing = Vec::new();
        for (key, value) in want {
            match markers.iter().find(|(k, _)| *k == key) {
                Some((_, marker)) => {
                    assignments.push(ScalarAssignment { marker: marker.clone(), value });
                }
                None => missing.push(key),
            }
        }
        if !missing.is_empty() {
            return Err(CoordinateGap { missing });
        }
        let origin = ProposalOrigin::Transition {
            from: proposal.from.as_str().to_owned(),
            to: proposal.to.as_str().to_owned(),
        };
        let path = path.into();
        let addr = address_of(&path, &assignments, &origin);
        Ok(Self { path, assignments, origin, addr })
    }

    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    #[must_use]
    pub fn assignments(&self) -> &[ScalarAssignment] {
        &self.assignments
    }

    #[must_use]
    pub const fn origin(&self) -> &ProposalOrigin {
        &self.origin
    }

    /// The content address over `(path, assignments, origin)` — what a status
    /// `pendingProposal` publishes, and what a receipt echoes so the commit
    /// that later lands is auditable against the proposal that asked for it.
    #[must_use]
    pub const fn addr(&self) -> ContentAddr {
        self.addr
    }
}

/// A class transition with no marker for one or more of the scalars it changes.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CoordinateGap {
    /// `container.requests.memory`-shaped keys that changed but have no marker.
    pub missing: Vec<String>,
}

impl CoordinateGap {
    /// The degenerate case: a "transition" whose block changes no scalar.
    #[must_use]
    fn empty() -> Self {
        Self { missing: Vec::new() }
    }
}

impl std::fmt::Display for CoordinateGap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.missing.is_empty() {
            return f.write_str("the proposed block changes no scalar — nothing to commit");
        }
        f.write_str("no manifest marker for: ")?;
        for (i, m) in self.missing.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            f.write_str(m)?;
        }
        Ok(())
    }
}

impl std::error::Error for CoordinateGap {}

/// The `container.requests.<resource>` scalars a transition must set.
///
/// **Every declared request in the desired block**, not a diff against the
/// observed one. Two reasons, both deliberate:
///
/// * A class transition is a *relation* — Guaranteed holds only while every
///   request equals every limit. Committing just the subset that happens to
///   differ today would leave the manifest one unrelated edit away from
///   silently falling out of the class it declares.
/// * Idempotence is already handled downstream: [`apply_all`] reports a value
///   that is already correct as `already_correct` and produces no change, so
///   enumerating a stable scalar costs a comparison, not a commit.
///
/// Only **requests** are enumerated. A durable door that could also rewrite
/// limits would be able to lower a blast-radius bound as a side effect of a QoS
/// change — a strictly wider power than this dimension is allowed to have.
fn required_scalars(p: &ClassTransitionProposal) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for c in &p.block.containers {
        for (res, want) in [("memory", c.memory_request), ("cpu", c.cpu_request)] {
            let Some(want) = want else { continue };
            let mut key = String::new();
            key.push_str(&c.name);
            key.push_str(".requests.");
            key.push_str(res);
            let mut value = String::new();
            render_scalar(&mut value, res, want);
            out.push((key, value));
        }
    }
    out
}

/// Render a raw scalar back into the unit the manifest speaks: bytes stay
/// bytes, millicores get the `m` suffix. Deliberately not `format!`-composed
/// prose — this is a typed rendering of one number into one field's unit.
fn render_scalar(into: &mut String, resource: &str, raw: u64) {
    use std::fmt::Write as _;
    if resource == "cpu" {
        let _ = write!(into, "{raw}m");
    } else {
        let _ = write!(into, "{raw}");
    }
}

/// BLAKE3 over a canonical encoding of `(path, assignments, origin)`.
fn address_of(path: &str, assignments: &[ScalarAssignment], origin: &ProposalOrigin) -> ContentAddr {
    #[derive(serde::Serialize)]
    struct Canonical<'a> {
        path: &'a str,
        assignments: &'a [ScalarAssignment],
        origin: &'a ProposalOrigin,
    }
    let bytes = serde_json::to_vec(&Canonical { path, assignments, origin }).unwrap_or_default();
    ContentAddr::of(&bytes)
}

// ─────────────────────────────────────────────────────────────────────────────
// THE SETTER — byte-exact, comment-preserving, marker-anchored
// ─────────────────────────────────────────────────────────────────────────────

/// The token every breathe marker comment carries.
const MARKER_TOKEN: &str = "$breathe";

/// What one scalar edit did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScalarEdit {
    /// 1-indexed line the value sits on — the audit trail's coordinate.
    pub line: usize,
    pub from: String,
    pub to: String,
}

/// The result of applying every assignment to a file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyOutcome {
    /// The full file after the edits. Byte-identical to the input except for
    /// the replaced scalar spans.
    pub rendered: Vec<u8>,
    /// Edits that actually moved a value. **Empty means nothing to commit** —
    /// the idempotence gate, computed before the repo is ever touched.
    pub changes: Vec<ScalarEdit>,
    /// Assignments whose value was already correct.
    pub already_correct: usize,
}

/// Why a marker-anchored edit could not be made.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase", tag = "editError")]
pub enum EditError {
    /// The manifest is not valid UTF-8. Refused rather than lossily converted:
    /// a lossy round-trip would silently rewrite bytes outside the edit span,
    /// which is the one thing this setter exists to never do.
    NotUtf8,
    /// No line carries this marker. The file was never opted in — or the marker
    /// was removed and the band was not updated.
    MarkerNotFound { marker: String },
    /// More than one line carries this marker. **Refused, never
    /// first-match-wins**: an ambiguous anchor picked silently is precisely the
    /// wrong-resource class ★★ INDIRECT RESOURCE REFERENCES names — loud
    /// ambiguity beats a silent wrong pick.
    MarkerAmbiguous { marker: String, lines: Vec<usize> },
    /// The marked line is not a `key: value` scalar assignment, so there is no
    /// span to replace.
    NotAScalarAssignment { marker: String, line: usize },
}

impl std::fmt::Display for EditError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotUtf8 => f.write_str("manifest is not valid UTF-8"),
            Self::MarkerNotFound { marker } => write!(f, "no line carries the breathe marker {marker:?}"),
            Self::MarkerAmbiguous { marker, lines } => {
                write!(f, "the breathe marker {marker:?} appears on {} lines ({lines:?}) — refusing to guess", lines.len())
            }
            Self::NotAScalarAssignment { marker, line } => {
                write!(f, "line {line} (marker {marker:?}) is not a `key: value` scalar assignment")
            }
        }
    }
}

impl std::error::Error for EditError {}

/// Replace the scalar on the line carrying `marker` with `new_value`, leaving
/// **every other byte of the file identical**.
///
/// The document is never parsed and never re-serialized. That is not an
/// optimization — it is the correctness property: a `serde_yaml` round-trip
/// would drop the comments, and on a Flux-managed manifest those comments
/// include `$imagepolicy` markers whose loss silently disables image
/// automation. A durable-write mechanism that breaks the neighbouring
/// automation is not a fix.
///
/// Preserved exactly: indentation, the key, the whitespace either side of the
/// value, the entire trailing comment (marker included), the line terminator,
/// every other line, and the value's quoting style.
///
/// # Errors
///
/// [`EditError`] — see its arms. Notably an ambiguous marker is *refused*, not
/// resolved by taking the first match.
pub fn set_scalar_at_marker(source: &[u8], marker: &str, new_value: &str) -> Result<EditOutcome, EditError> {
    let text = std::str::from_utf8(source).map_err(|_| EditError::NotUtf8)?;

    // The quoted form, so marker "sui" never matches marker "sui-cache".
    let mut needle = String::with_capacity(marker.len() + 2);
    needle.push('"');
    needle.push_str(marker);
    needle.push('"');

    let mut hits = Vec::new();
    for (idx, line) in text.split_inclusive('\n').enumerate() {
        if line.contains(MARKER_TOKEN) && line.contains(needle.as_str()) {
            hits.push(idx);
        }
    }
    match hits.len() {
        0 => return Err(EditError::MarkerNotFound { marker: marker.to_owned() }),
        1 => {}
        _ => {
            return Err(EditError::MarkerAmbiguous {
                marker: marker.to_owned(),
                lines: hits.iter().map(|i| i + 1).collect(),
            })
        }
    }
    let hit = hits[0];

    // Rebuild in one pass. Structured so the hit line is handled OUTSIDE the
    // copy loop rather than found again inside it: that removes the
    // `Option::expect` an in-loop capture would need, so this function has no
    // panic path at all — worth stating, because a panicking manifest editor
    // would take a reconcile loop down over a malformed file.
    let mut rendered = String::with_capacity(text.len() + new_value.len());
    let mut edited = None;
    for (idx, line) in text.split_inclusive('\n').enumerate() {
        if idx == hit {
            let e = replace_scalar_in_line(line, marker, idx + 1, new_value)?;
            rendered.push_str(&e.line);
            edited = Some(e);
        } else {
            rendered.push_str(line);
        }
    }
    // `hits` was collected from this exact iteration, so this is unreachable —
    // but it is expressed as a total match rather than an `expect`, because a
    // refactor that broke the invariant should be a typed error, not a panic.
    let Some(edited) = edited else {
        return Err(EditError::MarkerNotFound { marker: marker.to_owned() });
    };
    if edited.from == edited.to {
        return Ok(EditOutcome::Unchanged { line: hit + 1, value: edited.from });
    }
    Ok(EditOutcome::Changed {
        edit: ScalarEdit { line: hit + 1, from: edited.from, to: edited.to },
        rendered: rendered.into_bytes(),
    })
}

/// One line's replacement result.
struct EditedLine {
    line: String,
    from: String,
    to: String,
}

fn replace_scalar_in_line(line: &str, marker: &str, line_no: usize, new_value: &str) -> Result<EditedLine, EditError> {
    let bad = || EditError::NotAScalarAssignment { marker: marker.to_owned(), line: line_no };

    // The comment starts at the LAST '#' at or before the marker token — walking
    // back from the token rather than taking the first '#' in the line, so a '#'
    // inside the value (a colour, a fragment) does not truncate it.
    let token_at = line.find(MARKER_TOKEN).ok_or_else(bad)?;
    let hash_at = line[..token_at].rfind('#').ok_or_else(bad)?;
    let (head, comment) = line.split_at(hash_at);

    let colon_at = head.find(':').ok_or_else(bad)?;
    let (key_part, rest) = head.split_at(colon_at + 1);

    // rest = ws1 + value + ws2
    let ws1_len = rest.len() - rest.trim_start_matches([' ', '\t']).len();
    let (ws1, tail) = rest.split_at(ws1_len);
    let trimmed = tail.trim_end_matches([' ', '\t']);
    let ws2 = &tail[trimmed.len()..];
    if trimmed.is_empty() {
        return Err(bad());
    }

    // Preserve the quoting style the author chose.
    let (open, bare, close) = split_quotes(trimmed);
    let mut out = String::with_capacity(line.len() + new_value.len());
    out.push_str(key_part);
    out.push_str(ws1);
    out.push_str(open);
    out.push_str(new_value);
    out.push_str(close);
    out.push_str(ws2);
    out.push_str(comment);

    Ok(EditedLine { line: out, from: bare.to_owned(), to: new_value.to_owned() })
}

/// Split `"512Mi"` into `("\"", "512Mi", "\"")`, or `512Mi` into `("", "512Mi", "")`.
fn split_quotes(v: &str) -> (&str, &str, &str) {
    for q in ["\"", "'"] {
        if v.len() >= 2 * q.len() && v.starts_with(q) && v.ends_with(q) {
            return (q, &v[q.len()..v.len() - q.len()], q);
        }
    }
    ("", v, "")
}

/// The result of one marker-anchored set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EditOutcome {
    /// The value was already correct. **No bytes were produced** — an
    /// already-correct manifest must not yield a rewritten file, because an
    /// identical-content commit is still a commit, and a reconciler that
    /// commits every tick is a worse failure than one that never commits.
    Unchanged { line: usize, value: String },
    Changed { edit: ScalarEdit, rendered: Vec<u8> },
}

/// Apply every assignment to one file, in order, all-or-nothing.
///
/// # Errors
///
/// The first [`EditError`] any assignment hits — no partial render escapes.
pub fn apply_all(source: &[u8], assignments: &[ScalarAssignment]) -> Result<ApplyOutcome, EditError> {
    let mut current = source.to_vec();
    let mut changes = Vec::new();
    let mut already_correct = 0usize;
    for a in assignments {
        match set_scalar_at_marker(&current, &a.marker, &a.value)? {
            EditOutcome::Unchanged { .. } => already_correct += 1,
            EditOutcome::Changed { edit, rendered } => {
                changes.push(edit);
                current = rendered;
            }
        }
    }
    Ok(ApplyOutcome { rendered: current, changes, already_correct })
}

// ─────────────────────────────────────────────────────────────────────────────
// The repo seam — mockable by construction
// ─────────────────────────────────────────────────────────────────────────────

/// Why a repo operation failed.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase", tag = "repoError")]
pub enum RepoError {
    NotFound { path: String },
    Io { detail: String },
    /// The remote moved under us. The writer surfaces this rather than forcing:
    /// breathe never clobbers a field another writer owns.
    Conflict { detail: String },
}

impl std::fmt::Display for RepoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound { path } => write!(f, "no such manifest: {path}"),
            Self::Io { detail } => write!(f, "manifest repo I/O failed: {detail}"),
            Self::Conflict { detail } => write!(f, "manifest repo conflict: {detail}"),
        }
    }
}

impl std::error::Error for RepoError {}

/// **The transport seam.** Three methods, each with one job.
///
/// Narrowed deliberately from tend's `GitOps` (which has nine): a writer needs
/// to read a file, refuse a dirty tree, and land one file as one commit. The
/// stage/has-staged-changes idempotence dance tend performs is *subsumed* here
/// by [`apply_all`] computing `changes.is_empty()` before the repo is touched
/// at all — strictly better, because a no-op never becomes a write.
#[async_trait]
pub trait ManifestRepo: Send + Sync {
    /// Read a repo-relative path at the current HEAD.
    ///
    /// # Errors
    /// [`RepoError`] from the underlying transport.
    async fn read(&self, path: &str) -> Result<Vec<u8>, RepoError>;

    /// Is the working tree clean? A dirty tree means someone else is mid-edit;
    /// the writer refuses rather than committing on top of unknown work.
    ///
    /// # Errors
    /// [`RepoError`] from the underlying transport.
    async fn is_clean(&self) -> Result<bool, RepoError>;

    /// Write, commit and publish one file as one commit; return its sha.
    ///
    /// # Errors
    /// [`RepoError`] from the underlying transport.
    async fn commit_file(&self, path: &str, bytes: &[u8], message: &str) -> Result<String, RepoError>;
}

// ─────────────────────────────────────────────────────────────────────────────
// The writer
// ─────────────────────────────────────────────────────────────────────────────

/// What a durable write did.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase", tag = "outcome")]
pub enum CommitOutcome {
    /// The manifest already carried every value. **Nothing was committed.**
    ///
    /// A distinct arm rather than a receipt with an empty sha: re-running a
    /// converged proposal must be observably a no-op, not a commit that looks
    /// real in the audit trail.
    AlreadyCommitted { addr: ContentAddr },
    Committed(CommitReceipt),
}

/// **The real [`crate::ManifestWriter`]** — a marker-anchored, blast-radius-walled,
/// idempotent, git-visible scalar writer over an injected [`ManifestRepo`].
///
/// # The safety properties, and their honest tiers
///
/// | property | mechanism | tier |
/// |---|---|---|
/// | cannot write outside its subtree | `allowed_prefix` is a **construction-time** bound on the instance | truly-unrepresentable at the caller — there is no per-call path override |
/// | cannot write a file nobody opted in | the only anchor is an operator-authored marker | truly-unrepresentable — no marker, no span, [`EditError::MarkerNotFound`] |
/// | cannot pick the wrong one of two candidates | [`EditError::MarkerAmbiguous`] refuses | truly-unrepresentable — the ambiguous branch produces no bytes |
/// | cannot commit a no-op | `changes.is_empty()` short-circuits before the repo is touched | truly-unrepresentable — the commit path is not reached |
/// | cannot destroy neighbouring comments | never parses, never re-serializes | truly-unrepresentable — untouched bytes are copied verbatim |
/// | cannot commit onto someone else's edit | [`ManifestRepo::is_clean`] refusal | **only-mitigated** — a world-fact, true at read time only |
/// | the commit actually lands what was proposed | the receipt echoes both addresses | **only-mitigated** — an implementation that lies is still writable |
pub struct GitManifestWriter<R: ManifestRepo> {
    repo: R,
    allowed_prefix: String,
}

impl<R: ManifestRepo> GitManifestWriter<R> {
    /// Bind a writer to one subtree. The prefix is the hard wall: it is fixed
    /// at construction and there is no per-call override, so a writer built for
    /// `clusters/camelot/` cannot address `clusters/prod/` by any argument.
    #[must_use]
    pub fn new(repo: R, allowed_prefix: impl Into<String>) -> Self {
        Self { repo, allowed_prefix: allowed_prefix.into() }
    }

    /// Commit an addressed proposal.
    ///
    /// # Errors
    ///
    /// [`WriterError`] — blast-radius refusal, a dirty tree, an edit that could
    /// not be anchored, or a transport failure.
    pub async fn commit(&self, _live: &LiveWitness, p: &AddressedProposal) -> Result<CommitOutcome, WriterError> {
        // 1 — the wall, before anything is read.
        if !within_prefix(p.path(), &self.allowed_prefix) {
            return Err(WriterError::OutsideBlastRadius {
                path: p.path().to_owned(),
                allowed_prefix: self.allowed_prefix.clone(),
            });
        }

        // 2 — never commit on top of unknown work.
        if !self.repo.is_clean().await.map_err(transport)? {
            return Err(WriterError::RepoNotClean);
        }

        // 3 — read, edit, and decide idempotence BEFORE any write.
        let source = self.repo.read(p.path()).await.map_err(transport)?;
        let outcome = apply_all(&source, p.assignments())
            .map_err(|e| WriterError::Unanchorable { detail: e.to_string() })?;
        if outcome.changes.is_empty() {
            return Ok(CommitOutcome::AlreadyCommitted { addr: p.addr() });
        }

        // 4 — one file, one commit.
        let message = commit_message(p, &outcome);
        let sha = self.repo.commit_file(p.path(), &outcome.rendered, &message).await.map_err(transport)?;

        Ok(CommitOutcome::Committed(CommitReceipt {
            commit_sha: sha,
            addr: p.addr(),
            rendered_addr: ContentAddr::of(&outcome.rendered),
        }))
    }
}

fn transport(e: RepoError) -> WriterError {
    match e {
        RepoError::Conflict { detail } => WriterError::Conflict { detail },
        other => WriterError::Transport { detail: other.to_string() },
    }
}

/// Prefix containment on **path segments**, not on raw bytes — so an allowed
/// prefix of `clusters/camelot` does not admit `clusters/camelot-prod/…`.
fn within_prefix(path: &str, prefix: &str) -> bool {
    let prefix = prefix.trim_end_matches('/');
    if prefix.is_empty() {
        return false; // an empty wall is not a wall; refuse rather than allow-all.
    }
    if path == prefix {
        return true;
    }
    path.strip_prefix(prefix).is_some_and(|rest| rest.starts_with('/'))
}

/// The commit message — the audit trail Flux's own automation taught us to
/// write: what moved, from what to what, and on whose authority.
fn commit_message(p: &AddressedProposal, outcome: &ApplyOutcome) -> String {
    use std::fmt::Write as _;
    let mut m = String::new();
    match p.origin() {
        ProposalOrigin::Carve { resource, container } => {
            let _ = write!(m, "breathe: reserve {resource} for {container}");
        }
        ProposalOrigin::Transition { from, to } => {
            let _ = write!(m, "breathe: {from} -> {to}");
        }
    }
    let _ = write!(m, "\n\n");
    for e in &outcome.changes {
        let _ = writeln!(m, "{}:{} {} -> {}", p.path(), e.line, e.from, e.to);
    }
    let _ = writeln!(m, "\nproposal: {}", p.addr());
    m
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── the setter: byte-exactness is the whole property ─────────────────────

    const MANIFEST: &str = "\
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: sui            # the build cache
spec:
  values:
    image:
      tag: amd64-r412-9c1f2a # {\"$imagepolicy\": \"flux-system:sui:tag\"}
    resources:
      requests:
        memory: 512Mi # {\"$breathe\": \"camelot-build/sui-request\"}
        cpu: 200m
      limits:
        memory: 6Gi
";

    fn set(src: &str, marker: &str, v: &str) -> Result<EditOutcome, EditError> {
        set_scalar_at_marker(src.as_bytes(), marker, v)
    }

    #[test]
    fn the_edit_touches_exactly_one_span_and_nothing_else() {
        let EditOutcome::Changed { edit, rendered } =
            set(MANIFEST, "camelot-build/sui-request", "3Gi").unwrap()
        else {
            panic!("512Mi -> 3Gi is a change")
        };
        assert_eq!(edit.from, "512Mi");
        assert_eq!(edit.to, "3Gi");

        let out = String::from_utf8(rendered).unwrap();
        // The ONE difference, proven line-by-line rather than by eyeballing.
        let diffs: Vec<_> = MANIFEST.lines().zip(out.lines()).filter(|(a, b)| a != b).collect();
        assert_eq!(diffs.len(), 1, "exactly one line may differ, got {diffs:?}");
        assert_eq!(diffs[0].1, "        memory: 3Gi # {\"$breathe\": \"camelot-build/sui-request\"}");
        assert_eq!(MANIFEST.lines().count(), out.lines().count());
    }

    #[test]
    fn the_neighbouring_imagepolicy_marker_survives() {
        // The reason serde_yaml is banned here: losing this line's comment
        // silently disables Flux image automation on the same file.
        let EditOutcome::Changed { rendered, .. } = set(MANIFEST, "camelot-build/sui-request", "3Gi").unwrap() else {
            panic!()
        };
        let out = String::from_utf8(rendered).unwrap();
        assert!(out.contains("# {\"$imagepolicy\": \"flux-system:sui:tag\"}"));
        assert!(out.contains("  name: sui            # the build cache"), "unrelated comments + spacing survive");
    }

    #[test]
    fn an_already_correct_value_produces_no_bytes() {
        // Idempotence at the source: a converged manifest must not yield a
        // rewritten file, or the reconciler commits every tick forever.
        let o = set(MANIFEST, "camelot-build/sui-request", "512Mi").unwrap();
        assert!(matches!(o, EditOutcome::Unchanged { value, .. } if value == "512Mi"));
    }

    #[test]
    fn a_missing_marker_is_refused_not_guessed() {
        assert_eq!(
            set(MANIFEST, "nope", "1Gi"),
            Err(EditError::MarkerNotFound { marker: "nope".into() })
        );
    }

    #[test]
    fn an_ambiguous_marker_is_refused_never_first_match_wins() {
        let dup = "\
a: 1 # {\"$breathe\": \"dup\"}
b: 2 # {\"$breathe\": \"dup\"}
";
        let Err(EditError::MarkerAmbiguous { lines, .. }) = set(dup, "dup", "9") else {
            panic!("two anchors must refuse")
        };
        assert_eq!(lines, vec![1, 2]);
    }

    #[test]
    fn a_marker_is_matched_whole_never_as_a_prefix() {
        let src = "m: 1 # {\"$breathe\": \"sui-cache\"}\n";
        // "sui" must NOT match "sui-cache" — that is the wrong-resource class.
        assert!(matches!(set(src, "sui", "2"), Err(EditError::MarkerNotFound { .. })));
        assert!(matches!(set(src, "sui-cache", "2"), Ok(EditOutcome::Changed { .. })));
    }

    #[test]
    fn quoting_style_and_spacing_are_preserved() {
        for (src, want) in [
            ("  memory:   \"512Mi\"   # {\"$breathe\": \"m\"}\n", "  memory:   \"3Gi\"   # {\"$breathe\": \"m\"}\n"),
            ("  memory: '512Mi' # {\"$breathe\": \"m\"}\n", "  memory: '3Gi' # {\"$breathe\": \"m\"}\n"),
            ("\tmemory: 512Mi\t# {\"$breathe\": \"m\"}\n", "\tmemory: 3Gi\t# {\"$breathe\": \"m\"}\n"),
        ] {
            let EditOutcome::Changed { rendered, .. } = set(src, "m", "3Gi").unwrap() else { panic!() };
            assert_eq!(String::from_utf8(rendered).unwrap(), want);
        }
    }

    #[test]
    fn a_file_with_no_trailing_newline_keeps_not_having_one() {
        let src = "memory: 512Mi # {\"$breathe\": \"m\"}";
        let EditOutcome::Changed { rendered, .. } = set(src, "m", "1Gi").unwrap() else { panic!() };
        assert_eq!(String::from_utf8(rendered).unwrap(), "memory: 1Gi # {\"$breathe\": \"m\"}");
    }

    #[test]
    fn a_marked_line_that_is_not_an_assignment_is_refused() {
        let src = "- item # {\"$breathe\": \"m\"}\n";
        assert!(matches!(set(src, "m", "1Gi"), Err(EditError::NotAScalarAssignment { .. })));
    }

    #[test]
    fn non_utf8_is_refused_not_lossily_converted() {
        assert_eq!(set_scalar_at_marker(&[0xff, 0xfe], "m", "1"), Err(EditError::NotUtf8));
    }

    // ── the addressed proposal ───────────────────────────────────────────────

    fn coord() -> ManifestCoordinate {
        ManifestCoordinate::new("clusters/camelot/apps/sui/release.yaml", "camelot-build/sui-request")
    }

    #[test]
    fn a_carve_addresses_deterministically_and_a_different_value_readdresses() {
        let a = AddressedProposal::carve(&coord(), "3Gi", "memory", "sui");
        let b = AddressedProposal::carve(&coord(), "3Gi", "memory", "sui");
        let c = AddressedProposal::carve(&coord(), "4Gi", "memory", "sui");
        assert_eq!(a.addr(), b.addr(), "same inputs, same address");
        assert_ne!(a.addr(), c.addr(), "a different value is a different proposal");
        assert_eq!(a.assignments().len(), 1);
    }

    // ── the writer ───────────────────────────────────────────────────────────

    use std::sync::Mutex;

    #[derive(Default)]
    struct MockRepo {
        files: Mutex<Vec<(String, Vec<u8>)>>,
        clean: bool,
        commits: Mutex<Vec<(String, Vec<u8>, String)>>,
    }

    impl MockRepo {
        fn with(path: &str, body: &str) -> Self {
            Self {
                files: Mutex::new(vec![(path.to_owned(), body.as_bytes().to_vec())]),
                clean: true,
                commits: Mutex::new(Vec::new()),
            }
        }
        fn commits(&self) -> usize {
            self.commits.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl ManifestRepo for MockRepo {
        async fn read(&self, path: &str) -> Result<Vec<u8>, RepoError> {
            self.files
                .lock()
                .unwrap()
                .iter()
                .find(|(p, _)| p == path)
                .map(|(_, b)| b.clone())
                .ok_or_else(|| RepoError::NotFound { path: path.to_owned() })
        }
        async fn is_clean(&self) -> Result<bool, RepoError> {
            Ok(self.clean)
        }
        async fn commit_file(&self, path: &str, bytes: &[u8], message: &str) -> Result<String, RepoError> {
            self.commits.lock().unwrap().push((path.to_owned(), bytes.to_vec(), message.to_owned()));
            Ok("deadbeef".to_owned())
        }
    }

    fn witness() -> LiveWitness {
        crate::gate::authored_write_gate("drzzln: test").witness().expect("an authored write resolves Live").clone()
    }

    const PATH: &str = "clusters/camelot/apps/sui/release.yaml";

    #[tokio::test]
    async fn a_carve_lands_as_exactly_one_commit_carrying_the_edited_bytes() {
        let repo = MockRepo::with(PATH, MANIFEST);
        let w = GitManifestWriter::new(repo, "clusters/camelot");
        let p = AddressedProposal::carve(&coord(), "3Gi", "memory", "sui");

        let CommitOutcome::Committed(r) = w.commit(&witness(), &p).await.unwrap() else {
            panic!("a real change commits")
        };
        assert_eq!(r.commit_sha, "deadbeef");
        assert_eq!(r.addr, p.addr(), "the receipt echoes the proposal it discharged");

        let commits = w.repo.commits.lock().unwrap();
        assert_eq!(commits.len(), 1);
        let body = String::from_utf8(commits[0].1.clone()).unwrap();
        assert!(body.contains("memory: 3Gi # {\"$breathe\""), "the committed bytes carry the edit");
        assert!(body.contains("$imagepolicy"), "and still carry the neighbour's marker");
        assert!(commits[0].2.contains("512Mi -> 3Gi"), "the message names what moved");
    }

    #[tokio::test]
    async fn re_proposing_a_converged_value_commits_nothing() {
        let repo = MockRepo::with(PATH, MANIFEST);
        let w = GitManifestWriter::new(repo, "clusters/camelot");
        let p = AddressedProposal::carve(&coord(), "512Mi", "memory", "sui");

        assert!(matches!(w.commit(&witness(), &p).await.unwrap(), CommitOutcome::AlreadyCommitted { .. }));
        assert_eq!(w.repo.commits(), 0, "an idempotent tick must not produce a commit");
    }

    #[tokio::test]
    async fn the_blast_radius_wall_refuses_before_reading_anything() {
        let repo = MockRepo::with(PATH, MANIFEST);
        let w = GitManifestWriter::new(repo, "clusters/prod");
        let p = AddressedProposal::carve(&coord(), "3Gi", "memory", "sui");

        assert!(matches!(w.commit(&witness(), &p).await, Err(WriterError::OutsideBlastRadius { .. })));
        assert_eq!(w.repo.commits(), 0);
    }

    #[test]
    fn the_wall_is_segment_wise_so_a_sibling_prefix_never_slips_through() {
        assert!(within_prefix("clusters/camelot/a.yaml", "clusters/camelot"));
        assert!(within_prefix("clusters/camelot/a.yaml", "clusters/camelot/"));
        // The trap a raw `starts_with` would fall into:
        assert!(!within_prefix("clusters/camelot-prod/a.yaml", "clusters/camelot"));
        // An empty wall is refused rather than treated as allow-all.
        assert!(!within_prefix("anything", ""));
    }

    #[tokio::test]
    async fn a_dirty_tree_is_refused_rather_than_committed_on_top_of() {
        let mut repo = MockRepo::with(PATH, MANIFEST);
        repo.clean = false;
        let w = GitManifestWriter::new(repo, "clusters/camelot");
        let p = AddressedProposal::carve(&coord(), "3Gi", "memory", "sui");
        assert_eq!(w.commit(&witness(), &p).await, Err(WriterError::RepoNotClean));
        assert_eq!(w.repo.commits(), 0);
    }

    #[tokio::test]
    async fn an_unanchorable_edit_never_reaches_the_commit_path() {
        let repo = MockRepo::with(PATH, "spec: {}\n"); // no marker at all
        let w = GitManifestWriter::new(repo, "clusters/camelot");
        let p = AddressedProposal::carve(&coord(), "3Gi", "memory", "sui");
        assert!(matches!(w.commit(&witness(), &p).await, Err(WriterError::Unanchorable { .. })));
        assert_eq!(w.repo.commits(), 0);
    }
}
