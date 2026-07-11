use crate::fsm::{
    AwsTerminationConfirmed, FaseNode, InstanceId, K8sNodeRef, LifecycleError, Node, NodeId,
};

#[test]
fn full_happy_path_reaches_reaped() {
    let witness = AwsTerminationConfirmed::new(InstanceId::new("i-1"), 1_000);
    let reaped = Node::request(NodeId::new("n-1"))
        .provision()
        .joined(InstanceId::new("i-1"))
        .ready(K8sNodeRef::new("k-1"))
        .drain()
        .issue_termination()
        .confirm_reaped(witness.clone())
        .expect("matching witness must confirm");

    assert_eq!(reaped.id(), &NodeId::new("n-1"));
    assert_eq!(reaped.witness(), &witness);
    assert_eq!(reaped.fase(), FaseNode::Reaped);
}

#[test]
fn abort_from_requested_carries_no_instance() {
    let aborted = Node::request(NodeId::new("n-1")).abort("over budget");
    assert!(aborted.aws_instance_id().is_none());
    assert_eq!(aborted.fase(), FaseNode::Aborted);
}

#[test]
fn abort_from_provisioning_carries_no_instance() {
    let aborted = Node::request(NodeId::new("n-1"))
        .provision()
        .abort("RunInstances failed");
    assert!(aborted.aws_instance_id().is_none());
}

#[test]
fn abort_from_joining_structurally_carries_the_instance_it_must_reclaim() {
    // The load-bearing structural fact: an abort AFTER `joined()` has a real
    // AWS liability, and the record proves it by still carrying `Some(_)` —
    // the drift reconciler (crate::drift) treats this class as an immediate
    // reclaim target, distinct from an organic mark-and-sweep orphan.
    let aborted = Node::request(NodeId::new("n-1"))
        .provision()
        .joined(InstanceId::new("i-1"))
        .abort("kubelet never registered");
    assert_eq!(aborted.aws_instance_id(), Some(&InstanceId::new("i-1")));
}

#[test]
fn active_has_no_aborted_instance_ever_because_it_only_reaches_reaped_or_stays_active() {
    // Documents the type-level fact exercised as a compile_fail in the module
    // doc: Node<Active> has no `abort` method at all. This test proves the
    // ONLY way out of Active is through Draining → TerminationIssued → Reaped.
    let active = Node::request(NodeId::new("n-1"))
        .provision()
        .joined(InstanceId::new("i-1"))
        .ready(K8sNodeRef::new("k-1"));
    assert_eq!(active.fase(), FaseNode::Active);
    assert_eq!(active.k8s_node_ref(), Some(&K8sNodeRef::new("k-1")));
}

#[test]
fn confirm_reaped_rejects_a_mismatched_witness() {
    let ti = Node::request(NodeId::new("n-1"))
        .provision()
        .joined(InstanceId::new("i-1"))
        .ready(K8sNodeRef::new("k-1"))
        .drain()
        .issue_termination();
    let wrong_witness = AwsTerminationConfirmed::new(InstanceId::new("i-WRONG"), 1_000);
    let err = ti.confirm_reaped(wrong_witness).unwrap_err();
    assert_eq!(
        err,
        LifecycleError::WitnessInstanceMismatch {
            expected: InstanceId::new("i-1"),
            got: InstanceId::new("i-WRONG"),
        }
    );
}

#[test]
fn witness_carries_the_observation_it_was_constructed_from() {
    let w = AwsTerminationConfirmed::new(InstanceId::new("i-9"), 42);
    assert_eq!(w.instance_id(), &InstanceId::new("i-9"));
    assert_eq!(w.confirmed_at_unix(), 42);
}

// ── FSM convergence (the shared shigoto-fsm forcing-function) ──

#[test]
fn node_lifecycle_fsm_is_convergent() {
    shigoto_fsm::assert_convergent_fsm::<FaseNode>()
        .expect("the node lifecycle FSM must be convergent (every phase reaches a good terminal)");
}

#[test]
fn exactly_two_terminals_both_good() {
    let terminals: Vec<_> = FaseNode::ALL
        .into_iter()
        .filter(|f| f.is_terminal())
        .collect();
    assert_eq!(
        terminals.len(),
        2,
        "expected exactly 2 terminals (Aborted, Reaped)"
    );
    assert!(
        terminals.iter().all(|f| f.is_good_terminal()),
        "both terminals must be GOOD terminals"
    );
}

#[test]
fn active_has_no_edge_to_aborted() {
    assert!(
        !FaseNode::Active
            .legal_successors()
            .contains(&FaseNode::Aborted)
    );
    assert_eq!(FaseNode::Active.legal_successors(), &[FaseNode::Draining]);
}

#[test]
fn draining_cannot_be_skipped() {
    assert_eq!(FaseNode::Active.legal_successors(), &[FaseNode::Draining]);
    assert_eq!(
        FaseNode::Draining.legal_successors(),
        &[FaseNode::TerminationIssued]
    );
    assert_eq!(
        FaseNode::TerminationIssued.legal_successors(),
        &[FaseNode::Reaped]
    );
}

#[test]
fn fase_node_serde_round_trips_kebab_case() {
    let json = serde_json::to_string(&FaseNode::TerminationIssued).unwrap();
    assert_eq!(json, "\"termination-issued\"");
    let back: FaseNode = serde_json::from_str(&json).unwrap();
    assert_eq!(back, FaseNode::TerminationIssued);
}

#[test]
fn display_matches_as_str_for_every_phase() {
    for f in FaseNode::ALL {
        assert_eq!(f.to_string(), f.as_str());
    }
}
