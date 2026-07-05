//! `cost` — the OFFLINE half of the 100%-spot flex-window (BREATHABILITY §II.6).
//!
//! The aggressive-spot posture ("100% spot, even the databases") auctions every
//! node from the interruptible pool and **widens the instance-family set on
//! scarcity**, bounded by a monthly `$` variance budget. Two parts of that are
//! buildable with **zero live cluster**, and they live here as typed data:
//!
//! 1. the **diversified instance-family list** — the widen-on-scarcity menu the
//!    auction draws from (more families ⇒ deeper spot capacity ⇒ fewer reclaims);
//! 2. a **`CostBudget` promessa template** — the Viggy `(defpromessa)` that
//!    ATTESTS "100% spot within `$X`/mo variance" onto an OutcomeChain.
//!
//! What is NOT here (tier-honest — a `LiveTODO`, not rounded up): the live spot
//! **auction** itself (Karpenter/ASG NodePool + the widen loop), `retirada`
//! drain-ahead, and the nervous-system-grafted interruption sensing. Those need
//! the `CamelotNodeGroup` — the Pangea isolation floor — which is live infra. The
//! budget number here is an INTERIM setpoint the live auction tunes; "provably
//! cheapest" is **attested** (this CostBudget + its OutcomeChain), never
//! compile-proven.

/// The flex-window cost envelope for the 100%-spot posture — the diversified
/// instance-family menu the auction widens across, bounded by a monthly `$`
/// variance budget. Referenced by [`crate::preset::BreatheDefaults::flex_window`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FlexWindow {
    /// The monthly `$` spend-variance budget the 100%-spot auction stays within.
    /// The bound on how far a spot-price swing may move the bill before the widen
    /// loop is expected to have restored the cheaper families. INTERIM: the live
    /// auction tunes this; here it is a declared setpoint, not a measured fact.
    pub monthly_usd_variance_budget: f64,
    /// The diversified spot instance-family menu (the widen-on-scarcity list). More
    /// families across generations/sizes ⇒ deeper interruptible capacity ⇒ fewer
    /// reclaims. Ordered cheapest-preference-first; the auction widens down the list.
    pub instance_families: &'static [&'static str],
}

/// The minimum family count that makes "diversified" a real claim (deep-enough
/// spot pools for a 100%-spot posture to hold). Below this, a single family's
/// reclaim wave can drain the whole footprint — the whole point of the menu.
pub const MIN_DIVERSIFIED_FAMILIES: usize = 4;

/// The Camelot diversified instance-family menu — general-purpose (`m*`), compute
/// (`c*`), and memory (`r*`) families across two Intel/AMD generations, so the
/// mixed stateless + DB workload set always has a deep interruptible pool. The
/// live auction widens across this list; a scarcity on one family falls through
/// to the next. Data (Pillar 12), never a hand-tuned per-workload node group.
pub const CAMELOT_INSTANCE_FAMILIES: &[&str] =
    &["m6i", "m6a", "m7i", "m5", "m5a", "c6i", "c6a", "r6i", "r6a"];

/// The Camelot flex-window — the offline-buildable envelope. The variance budget
/// is an INTERIM setpoint (the live auction tunes it); the family menu is the
/// widen-on-scarcity list.
pub const CAMELOT_FLEX_WINDOW: FlexWindow = FlexWindow {
    monthly_usd_variance_budget: 400.0,
    instance_families: CAMELOT_INSTANCE_FAMILIES,
};

/// A Viggy `(defpromessa)` template that ATTESTS the 100%-spot cost posture —
/// the OUTCOME the auction is held to, continuously proven on an OutcomeChain
/// (not compile-proven). This is a typed authoring template, not the running
/// controller: a `PromessaController` reconciling it is the LiveTODO destination.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CostBudget {
    /// The promessa name (`metadata.name`).
    pub name: &'static str,
    /// The Viggy promessa kind — always `"CostBudget"` for this template.
    pub promessa_kind: &'static str,
    /// The `$`/mo variance the promessa holds the spend within (mirrors the
    /// flex-window budget; the invariant test cross-checks the two agree).
    pub target_monthly_usd_variance: f64,
    /// The spot fraction the promessa asserts (`1.0` = 100% spot).
    pub spot_fraction: f64,
    /// One line: how the promise is proven (attested, never compile-proven).
    pub attest: &'static str,
}

/// The Camelot cost-budget promessa — "100% spot within the monthly `$` variance
/// budget", attested on an OutcomeChain. Tier-honest: an ATTESTED promise, never
/// a compile-time theorem; the reconciling controller is a LiveTODO.
pub const CAMELOT_COST_BUDGET: CostBudget = CostBudget {
    name: "camelot-100pct-spot",
    promessa_kind: "CostBudget",
    target_monthly_usd_variance: 400.0,
    spot_fraction: 1.0,
    attest: "100% spot within the monthly $ variance budget — attested on OutcomeChain, never compile-proven",
};

#[cfg(test)]
mod tests {
    use super::{
        CAMELOT_COST_BUDGET, CAMELOT_FLEX_WINDOW, CAMELOT_INSTANCE_FAMILIES, MIN_DIVERSIFIED_FAMILIES,
    };

    /// The instance-family menu must be genuinely diversified — a single-family
    /// menu is not a 100%-spot posture (one reclaim wave drains everything).
    #[test]
    fn instance_families_are_diversified() {
        assert!(
            CAMELOT_INSTANCE_FAMILIES.len() >= MIN_DIVERSIFIED_FAMILIES,
            "a 100%-spot menu needs at least {MIN_DIVERSIFIED_FAMILIES} families for spot depth"
        );
    }

    /// No duplicate families (a duplicate is not extra depth; it is a typo).
    #[test]
    fn instance_families_are_unique() {
        let mut seen: Vec<&str> = CAMELOT_INSTANCE_FAMILIES.to_vec();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), CAMELOT_INSTANCE_FAMILIES.len(), "duplicate instance family in the menu");
    }

    /// The menu must span at least the three general/compute/memory shapes so a
    /// mixed stateless + DB workload set always has a deep pool for its shape.
    #[test]
    fn instance_families_span_the_workload_shapes() {
        let has = |p: &str| CAMELOT_INSTANCE_FAMILIES.iter().any(|f| f.starts_with(p));
        assert!(has("m"), "no general-purpose (m*) family");
        assert!(has("c"), "no compute (c*) family");
        assert!(has("r"), "no memory (r*) family");
    }

    /// The flex-window points at the menu, and its budget is a positive bound.
    #[test]
    fn flex_window_is_well_formed() {
        assert!(CAMELOT_FLEX_WINDOW.monthly_usd_variance_budget > 0.0, "budget must be a positive $ bound");
        assert_eq!(
            CAMELOT_FLEX_WINDOW.instance_families.len(),
            CAMELOT_INSTANCE_FAMILIES.len(),
            "the flex-window must carry the diversified menu"
        );
    }

    /// THE cross-consistency invariant: the CostBudget promessa asserts EXACTLY the
    /// flex-window's budget and a full 100%-spot fraction. The promise and the
    /// envelope can never silently disagree.
    #[test]
    fn cost_budget_matches_the_flex_window() {
        assert!(
            (CAMELOT_COST_BUDGET.target_monthly_usd_variance
                - CAMELOT_FLEX_WINDOW.monthly_usd_variance_budget)
                .abs()
                < f64::EPSILON,
            "the CostBudget target must mirror the flex-window budget"
        );
        assert!(
            (CAMELOT_COST_BUDGET.spot_fraction - 1.0).abs() < f64::EPSILON,
            "the aggressive posture is 100% spot"
        );
        assert_eq!(CAMELOT_COST_BUDGET.promessa_kind, "CostBudget");
    }

    /// The promessa is honest about its tier — an ATTESTED promise, never a
    /// compile-time theorem. The `attest` line must say so (guards against a future
    /// edit rounding the claim up).
    #[test]
    fn cost_budget_is_tier_honest() {
        let a = CAMELOT_COST_BUDGET.attest;
        assert!(a.contains("attested"), "the CostBudget must name itself attested");
        assert!(a.contains("never compile-proven"), "the CostBudget must NOT claim a compile-time proof");
    }
}
