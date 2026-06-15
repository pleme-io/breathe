//! `breathe-provision` — the closed reconcile loop for a [`Forma`]
//! (docs/PROVISIONING.md §1, §8). The single-row pipeline that proves the whole
//! provisioning substrate composes end-to-end:
//!
//! ```text
//!   observe ─► predict ─► decide ─► act
//!   (Provedor) (Previsor) (Leiloeiro) │
//!                                     ├─ Manter            → hold
//!                                     ├─ Crescer{δ}        → provision(δ) ─► for each new unit:
//!                                     │                       discover → gates → classify → Viveiro
//!                                     ├─ Encolher{δ}       → deprovision(δ) (drain) → retire
//!                                     └─ EnvelopeExausto   → escalate (never silently under-provision)
//! ```
//!
//! The decision is the proven band law (via [`Leiloeiro`]); the candidates only
//! reach the [`Viveiro`] through the [`breathe_admission`] validated-admission FSM
//! — so a provisioned-but-unvalidated unit is *never* usable. Provisioning I/O is
//! behind the [`Provedor`] trait (a magma Plan at M2; a `DryRun` mock here), so
//! this loop is testable with zero cluster.

use breathe_admission::{
    classify, Allocatable, Descoberto, Portao, Recurso, ResourceId, ValidationStep, Viveiro,
};
use breathe_auction::{DecisaoForma, Leiloeiro, Previsor};
use breathe_control::BandConfig;
use breathe_provider::{Forma, Provedor};

/// A typed witness of one forma reconcile tick — the provisioning peer of
/// breathe's `TickReceipt`. Every arm is a proof of what the loop did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormaTick {
    /// In-band — nothing provisioned.
    Held,
    /// Grew the shape: `requested` units provisioned, `admitted` cleared the
    /// admission gates into the `Viveiro`, `rejected` did not.
    Grew { forma: Forma, requested: u64, admitted: u64, rejected: u64 },
    /// Shrank the shape by `released` units (drain-first).
    Shrank { forma: Forma, released: u64 },
    /// Demand exceeds the envelope — escalated, never silently under-provisioned.
    EnvelopeExhausted { forma: Forma, shortfall: u64 },
    /// The observe step failed — hold + surface (never decide on no data).
    ObserveError(String),
}

/// Run ONE reconcile tick for `forma`. Generic over the provisioning boundary
/// ([`Provedor`]), the decision ([`Previsor`] + [`Leiloeiro`]), and the admission
/// gates ([`Portao`]). `mint_inner` turns a provisioned unit's id into the typed
/// handle the gates inspect + the `Viveiro` stores (a node ref at M2). `cfg` is
/// the band config — its `ceiling` is the `Densa` envelope wall.
///
/// The loop never lets an unvalidated unit into the pool: a provisioned unit
/// becomes a `Recurso<Descoberto>`, runs the gates, and is admitted *only* if
/// [`classify`] returns `Ready` (the M1 guarantee, reused).
pub async fn reconcile_forma<P, R, L, T>(
    forma: Forma,
    provedor: &P,
    previsor: &R,
    leiloeiro: &L,
    cfg: &BandConfig,
    gates: &[Box<dyn Portao<T>>],
    defer_budget: u32,
    viveiro: &mut Viveiro<T>,
    mint_inner: impl Fn(&ResourceId) -> T,
) -> FormaTick
where
    P: Provedor,
    R: Previsor,
    L: Leiloeiro,
    T: Allocatable + Send + Sync,
{
    let sample = match provedor.observe().await {
        Ok(s) => s,
        Err(e) => return FormaTick::ObserveError(format!("{e}")),
    };
    let previsao = previsor.predict(sample.used, sample.capacity);

    match leiloeiro.decide(forma, &previsao, cfg) {
        DecisaoForma::Manter => FormaTick::Held,

        DecisaoForma::EnvelopeExausto { forma, shortfall } => {
            FormaTick::EnvelopeExhausted { forma, shortfall }
        }

        DecisaoForma::Crescer { forma, delta } => {
            // Dispatch the provision (a magma Plan at M2; a DryRun mock at M0).
            // Non-fatal: a provision error still lets the admission loop run on
            // whatever did come up (idempotent provision is retried next tick).
            let _ = provedor.provision(delta).await;

            let mut admitted = 0u64;
            let mut rejected = 0u64;
            for i in 0..delta {
                let id = ResourceId::new(format!("{forma}-{i}"));
                let inner = mint_inner(&id);
                let candidate = Recurso::<Descoberto>::discover(id, forma)
                    .begin_provision()
                    .provisioned()
                    .begin_validation();

                // Run every gate, accumulate the receipts, classify.
                let mut receipts = Vec::with_capacity(gates.len());
                for gate in gates {
                    receipts.push(gate.check(&candidate, &inner).await);
                }
                match classify(candidate, &receipts, defer_budget) {
                    ValidationStep::Ready(pronto) => {
                        viveiro.admit(pronto.admit(inner));
                        admitted += 1;
                    }
                    // Rejected / Expired / Deferred all keep the unit OUT of the
                    // pool (an unvalidated unit is never usable). Deferred would
                    // requeue in the controller; in one tick it counts as not-yet.
                    _ => rejected += 1,
                }
            }
            FormaTick::Grew { forma, requested: delta, admitted, rejected }
        }

        DecisaoForma::Encolher { forma, delta, drain: _ } => {
            let _ = provedor.deprovision(delta).await;
            FormaTick::Shrank { forma, released: delta }
        }

        // Replace (spot→on-demand on interruption) lands at M3; the single-forma
        // BandLeiloeiro never emits it, so this is unreachable in M0.
        DecisaoForma::Reformar { .. } => FormaTick::Held,
    }
}

pub mod sim;

#[cfg(test)]
mod tests;
