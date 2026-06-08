---
title: P/D Disaggregated Autoscaling API
authors:
- @LiZhenCheng9527
- @hzxuzhonghu
reviewers:
- TBD
approvers:
- TBD

creation-date: 2025-05-09

---

## P/D Disaggregated Autoscaling API

### Summary

This proposal redesigns the autoscaling API with two goals:

1. **Merge `AutoscalingPolicyBinding` into `AutoscalingPolicy`** — today users must create two resources (policy + binding) and cross-reference them. Merging eliminates the indirection, removes split configuration across objects, and gives users a single resource that fully describes "what to scale, on what signal, and how."
2. **Add first-class `DisaggregatedTarget`** — replace the generic `SubTarget` mechanism with a purpose-built structure for coordinated multi-role scaling, including independent per-role metrics, per-role metric sources (Pod or Prometheus), replica bounds, and role-to-role ratio constraints.

The `AutoscalingPolicyBinding` CRD and the `SubTarget` type are removed.

### Motivation

In disaggregated prefill/decode inference architectures, the prefill and decode stages have fundamentally different resource profiles:

- **Prefill** is compute-bound and bursty — it processes the full prompt in one forward pass.
- **Decode** is memory-bandwidth-bound and long-running — it generates tokens autoregressively.

Scaling these two stages independently is essential for cost-efficient serving. However, independent scaling alone is insufficient — the P/D ratio must be coordinated. Too many prefill replicas starve decode capacity (growing queues); too many decode replicas waste GPU memory on idle KV caches. A healthy system keeps the ratio within an operator-defined range.

**Problems with the current two-resource model (AutoscalingPolicy + AutoscalingPolicyBinding):**

1. **Unnecessary indirection** — the user always creates a 1:1 pair (policy + binding). The binding adds a `policyRef` that points to a policy in the same namespace. This indirection provides no reuse benefit in practice (policies are rarely shared across multiple bindings) and doubles the number of objects to manage.
2. **Configuration split across two resources** — metric targets live in `AutoscalingPolicy.spec.metrics`, while metric retrieval details (`Pod`/`Prometheus` query and endpoint) live in `AutoscalingPolicyBinding.spec.*.target.metricSources`. Users must keep two resources in sync (metric names in policy and map keys in binding), which is error-prone.
3. **Fragmented view** — operators must read two resources to understand the complete autoscaling configuration for a single ModelServing.

**Problems with `SubTarget` for P/D disaggregation:**

1. **No coordination** — each binding scales its target independently; there is no concept of a ratio constraint between prefill and decode.
2. **Fragile coupling** — two bindings must manually agree on `targetRef`, and there is no validation that they reference the same ModelServing.
3. **Generic abstraction** — `SubTarget` is a generic kind/name pair. It provides no schema-level guidance, validation, or defaulting for P/D use cases.

#### Goals

- Merge `AutoscalingPolicyBinding` into `AutoscalingPolicy` to provide a single-resource UX.
- Provide a single `AutoscalingPolicy` resource that drives coordinated P/D scaling for one ModelServing.
- Allow independent `minReplicas` / `maxReplicas` per role to set per-stage capacity boundaries.
- Introduce `ratioConstraints` so the controller can enforce healthy role-to-role ratios.
- Support per-role metrics and per-role metric sources, reusing current `MetricSource` semantics (`Pod` and `Prometheus`).
- Remove the `AutoscalingPolicyBinding` CRD and the generic `SubTarget` type.

#### Non-Goals

- Controller implementation and reconciliation loop design (covered separately).
- Multi-ModelServing (heterogeneous hardware) P/D scaling — that remains in `HeterogeneousTarget`.

### Proposal

#### User Stories

##### Story 1: Single-resource autoscaling

As an ML platform operator, I want to define the complete autoscaling configuration — metrics, behavior, and target — in a single `AutoscalingPolicy` resource instead of maintaining a policy and a separate binding that cross-reference each other.

##### Story 2: Independent P/D scaling with ratio guardrails

As an ML platform operator, I deploy a vLLM disaggregated model with prefill and decode roles. I want the autoscaler to scale prefill replicas between 1–8 and decode replicas between 2–16, while always maintaining a P:D ratio between 1:1 and 1:4. This means if I have 2 prefill replicas, the decode replicas must be between 2 and 8.

##### Story 3: Per-role metrics and sources

As a platform engineer, I want each role (for example, prefill/decode now and rerank in the future) to define its own scaling metrics and metric sources independently in one policy.

##### Story 4: Migration from Policy + Binding

As an existing user with an `AutoscalingPolicy` and one or more `AutoscalingPolicyBinding` objects, I want to consolidate into a single `AutoscalingPolicy` resource.

#### Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Breaking change: removes `AutoscalingPolicyBinding` CRD | Both CRDs are alpha-level. Provide a migration guide and conversion tooling. The merged API is strictly simpler. |
| Breaking change for users currently using `SubTarget` | `SubTarget` was alpha-level and only used for P/D roles — the replacement `DisaggregatedTarget` is strictly more capable. |
| Loss of policy reuse across bindings | In practice policies are rarely shared. If reuse is needed, users can use templating tools (Helm, Kustomize). The UX win of a single resource outweighs the theoretical reuse loss. |
| Ratio enforcement may conflict with per-role min/max bounds | Webhook validates that each `ratioConstraints` entry is achievable given min/max replica bounds at admission time. |
| Increased controller complexity | Ratio enforcement is a bounded constraint-satisfaction problem; design details are deferred to the controller proposal. |

### Design Details

#### API Changes Overview

| Change | Description |
|--------|-------------|
| Delete `AutoscalingPolicyBinding` CRD | All target/binding fields move into `AutoscalingPolicy`. |
| Delete `SubTarget` type | Replaced by `DisaggregatedTarget`. |
| Expand `AutoscalingPolicySpec` | Add target fields (`homogeneousTarget`, `heterogeneousTarget`, `disaggregatedTarget`) directly. Metrics become the default; per-role metrics can override them. |
| Preserve `MetricSource` model | Keep current `MetricSource` discriminated union (`Pod` / `Prometheus`) and move per-target/per-role `metricSources` into `AutoscalingPolicy`. |
| Add `DisaggregatedTarget` | New first-class multi-role scaling type with `roles` and `ratioConstraints`. |
| Simplify `Target` | Remove `SubTarget` field. |

##### 1. Merged `AutoscalingPolicy`

```go
// AutoscalingPolicySpec defines the desired state of AutoscalingPolicy.
// +kubebuilder:validation:XValidation:rule="[has(self.heterogeneousTarget), has(self.homogeneousTarget), has(self.disaggregatedTarget)].exists_one(x, x).size() == 1",message="Exactly one of heterogeneousTarget, homogeneousTarget, or disaggregatedTarget must be set."
type AutoscalingPolicySpec struct {
    // ...

    // --- Target (exactly one must be set) ---
    // HomogeneousTarget enables traditional metric-based scaling for a
    // single ModelServing deployment (whole-deployment granularity).
    // +optional
    HomogeneousTarget *HomogeneousTarget `json:"homogeneousTarget,omitempty"`

    // HeterogeneousTarget enables optimization-based scaling across multiple
    // ModelServing deployments with different hardware capabilities.
    // +optional
    HeterogeneousTarget *HeterogeneousTarget `json:"heterogeneousTarget,omitempty"`

    // DisaggregatedTarget enables coordinated autoscaling of roles
    // within a single ModelServing that uses disaggregated serving.
    // +optional
    DisaggregatedTarget *DisaggregatedTarget `json:"disaggregatedTarget,omitempty"`
}
```

##### 2. Remove `SubTarget` and simplify `Target`

Delete the `SubTarget` struct. `Target` is simplified to:

```go
// Target defines a ModelServing deployment that can be monitored and scaled.
type Target struct {
    // TargetRef references the target object to be monitored and scaled.
    TargetRef corev1.ObjectReference `json:"targetRef"`
    // MetricSources declares how to fetch specific metrics for this target.
    // Keys must match AutoscalingPolicy.spec.metrics[].name.
    // Missing keys are treated as missing metrics for that reconcile loop.
    // For example, a key "podinfo_rps" here must correspond to a metric named
    // "podinfo_rps" in the referenced AutoscalingPolicy.
    // +optional
    MetricSources map[string]MetricSource `json:"metricSources,omitempty"`
}
```

`Target` remains in use by `HomogeneousTarget` (whole-ModelServing scaling) and `HeterogeneousTarget` (multi-ModelServing optimization). Both operate at the ModelServing level and never used `SubTarget` meaningfully.

##### 2.1 Preserve `MetricSource` and Prometheus semantics

The merged API keeps the existing metric-source model from `AutoscalingPolicyBinding` unchanged:

- `MetricSource.type: Pod | Prometheus` (default `Pod`)
- `MetricSource.pod` for direct pod scraping (`name`/`uri`/`port`/`labelSelector`)
- `MetricSource.prometheus` for external Prometheus query (`serverURL` + `query`)

`PrometheusMetricSource.auth` remains part of the API surface and continues to be reserved for follow-up runtime implementation, same as today.

##### 3. `DisaggregatedTarget` and supporting types

```go
// DisaggregatedTarget defines coordinated autoscaling for disaggregated
// serving roles within a single ModelServing deployment.
type DisaggregatedTarget struct {
    // TargetRef references the ModelServing deployment that contains
    // all scalable roles.
    TargetRef corev1.ObjectReference `json:"targetRef"`

    // Roles defines per-role scaling parameters. The map key is roleName
    // from ModelServing.spec.template.roles[].name.
    // +kubebuilder:validation:MinProperties=2
    Roles map[string]RoleScalingParam `json:"roles"`

    // RatioConstraints defines acceptable ratio ranges between role pairs.
    // Each constraint enforces:
    //   minRatio <= replicas[numeratorRole] / replicas[denominatorRole] <= maxRatio
    // when denominator replicas is non-zero.
    //
    // +optional
    RatioConstraints []RoleRatioConstraint `json:"ratioConstraints,omitempty"`
}

// RoleScalingParam defines the scaling configuration for one role.
type RoleScalingParam struct {
    // MinReplicas defines the minimum number of replicas for this role.
    // +kubebuilder:validation:Minimum=0
    // +kubebuilder:validation:Maximum=1000000
    MinReplicas int32 `json:"minReplicas"`

    // MaxReplicas defines the maximum number of replicas for this role.
    // +kubebuilder:validation:Minimum=1
    // +kubebuilder:validation:Maximum=1000000
    MaxReplicas int32 `json:"maxReplicas"`

    // Metrics overrides the policy-level metrics for this specific role.
    // This allows different roles to scale on different signals.
    // If not set, the top-level spec.metrics are used.
    // +optional
    // +kubebuilder:validation:MinItems=1
    Metrics []AutoscalingPolicyMetric `json:"metrics,omitempty"`

    // MetricSources declares how each metric is fetched for this role.
    // Keys must match role-level metrics when present, otherwise top-level
    // spec.metrics[].name.
    // Missing keys default to pod scraping behavior equivalent to an empty
    // PodMetricSource (uri=/metrics, port=8100, metric name defaults to key).
    // +optional
    MetricSources map[string]MetricSource `json:"metricSources,omitempty"`
}

// RoleRatioConstraint defines the acceptable ratio range between two roles.
type RoleRatioConstraint struct {
    // NumeratorRole is the role on the numerator side of the ratio.
    NumeratorRole string `json:"numeratorRole"`

    // DenominatorRole is the role on the denominator side of the ratio.
    DenominatorRole string `json:"denominatorRole"`

    // MinRatio is the minimum allowed value of
    // replicas[numeratorRole] / replicas[denominatorRole].
    // +kubebuilder:validation:Minimum=0
    MinRatio resource.Quantity `json:"minRatio"`

    // MaxRatio is the maximum allowed value of
    // replicas[numeratorRole] / replicas[denominatorRole].
    MaxRatio resource.Quantity `json:"maxRatio"`
}
```

> **Why `resource.Quantity` for ratios?** Kubernetes does not support native `float` fields in CRDs. `resource.Quantity` is the idiomatic way to express decimal values in the Kubernetes API (e.g., `"0.25"`, `"1"`, `"2.5"`). It avoids floating-point imprecision and is already used throughout the Kubernetes and Kthena APIs for similar purposes.
>
> **Caveat**: `resource.Quantity` carries unit/suffix semantics (e.g., `"250m"` is parsed as `0.25`), which can be surprising when the value is meant as a pure ratio. An integer-pair representation that avoids this ambiguity is discussed in [Alternative 5](#alternative-5-integer-pair-ratio-instead-of-resourcequantity).

##### 4. `HomogeneousTarget` (unchanged, except `SubTarget` removed from `Target`)

```go
type HomogeneousTarget struct {
    // Target defines the object to be monitored and scaled.
    Target Target `json:"target,omitempty"`
    // MinReplicas defines the minimum number of replicas to maintain.
    // +kubebuilder:validation:Minimum=0
    // +kubebuilder:validation:Maximum=1000000
    MinReplicas int32 `json:"minReplicas"`
    // MaxReplicas defines the maximum number of replicas allowed.
    // +kubebuilder:validation:Minimum=1
    // +kubebuilder:validation:Maximum=1000000
    MaxReplicas int32 `json:"maxReplicas"`
}
```

##### 5. Delete `AutoscalingPolicyBinding` CRD

The entire `AutoscalingPolicyBinding`, `AutoscalingPolicyBindingSpec`, `AutoscalingPolicyBindingStatus`, and `AutoscalingPolicyBindingList` types are removed. The `policyRef` indirection is eliminated.

##### 6. `AutoscalingPolicyStatus`

Because the target now lives in `AutoscalingPolicy` itself (previously the binding carried the binding-side status), `AutoscalingPolicy` needs a status subresource that reports the observed scaling state. This is especially important for `DisaggregatedTarget`, where the user must be able to observe the current per-role replica counts, the actual P/D ratio, and whether the ratio constraint forced an adjustment.

```go
// AutoscalingPolicyStatus defines the observed state of AutoscalingPolicy.
type AutoscalingPolicyStatus struct {
    // ObservedGeneration is the most recent generation observed by the controller.
    // +optional
    ObservedGeneration int64 `json:"observedGeneration,omitempty"`

    // Conditions represents the latest available observations of the policy's state.
    // Well-known condition types include:
    //   - "Ready":                   the policy is actively reconciled.
    //   - "TargetFound":             the referenced ModelServing (and roles) exist.
    //   - "RatioConstraintViolated": the desired counts could not satisfy ratioConstraints
    //                                given the per-role min/max bounds.
    // +optional
    // +listType=map
    // +listMapKey=type
    Conditions []metav1.Condition `json:"conditions,omitempty"`

    // HomogeneousStatus reports the observed state when HomogeneousTarget is used.
    // +optional
    HomogeneousStatus *TargetScalingStatus `json:"homogeneousStatus,omitempty"`

    // DisaggregatedStatus reports the observed state when DisaggregatedTarget is used.
    // +optional
    DisaggregatedStatus *DisaggregatedScalingStatus `json:"disaggregatedStatus,omitempty"`

    // HeterogeneousStatus reports the per-target observed state when
    // HeterogeneousTarget is used.
    // +optional
    HeterogeneousStatus []TargetScalingStatus `json:"heterogeneousStatus,omitempty"`
}

// TargetScalingStatus reports the observed scaling state of a single scalable
// unit (a whole ModelServing, or one role within it).
type TargetScalingStatus struct {
    // Name identifies the unit. For HomogeneousTarget it is the ModelServing
    // name; for a role it is the role name.
    Name string `json:"name"`

    // CurrentReplicas is the number of replicas currently observed.
    CurrentReplicas int32 `json:"currentReplicas"`

    // DesiredReplicas is the number of replicas the controller computed from
    // metrics, before ratio enforcement.
    DesiredReplicas int32 `json:"desiredReplicas"`

    // Mode reports whether the unit is currently in "Stable" or "Panic" mode.
    // +optional
    Mode string `json:"mode,omitempty"`

    // LastScaleTime is the last time the unit was scaled by the controller.
    // +optional
    LastScaleTime *metav1.Time `json:"lastScaleTime,omitempty"`
}

// DisaggregatedScalingStatus reports the observed state of a DisaggregatedTarget.
type DisaggregatedScalingStatus struct {
    // Roles reports the observed scaling state per role.
    Roles []TargetScalingStatus `json:"roles"`

    // RatioStatuses reports the observed value per configured ratio constraint.
    // +optional
    RatioStatuses []RoleRatioStatus `json:"ratioStatuses,omitempty"`

    // RatioAdjusted is true when the most recent reconcile had to override the
    // metric-derived replica counts to satisfy one or more ratio constraints.
    // +optional
    RatioAdjusted bool `json:"ratioAdjusted,omitempty"`
}

// RoleRatioStatus reports the observed value for one ratio constraint.
type RoleRatioStatus struct {
  NumeratorRole   string `json:"numeratorRole"`
  DenominatorRole string `json:"denominatorRole"`
  CurrentRatio    string `json:"currentRatio,omitempty"`
}
```

Recommended printer columns for `kubectl get autoscalingpolicy`:

| Column | Source |
|--------|--------|
| `ROLES` | `len(status.disaggregatedStatus.roles)` |
| `RATIOS` | `len(status.disaggregatedStatus.ratioStatuses)` |
| `READY` | `status.conditions[type=Ready].status` |

#### Full YAML Examples

##### Disaggregated P/D scaling (single resource)

```yaml
apiVersion: workload.serving.volcano.sh/v1alpha1
kind: AutoscalingPolicy
metadata:
  name: llm-pd-scaling
  namespace: default
spec:
  tolerancePercent: 10
  # Default metrics — used as fallback when a role doesn't specify its own
  metrics:
    - name: pending_requests
      targetValue: "5"
  behavior:
    scaleUp:
      stablePolicy:
        instances: 2
        period: 30s
        stabilizationWindow: 60s
      panicPolicy:
        period: 10s
        panicThresholdPercent: 200
        panicModeHold: 120s
    scaleDown:
      instances: 1
      period: 60s
      stabilizationWindow: 300s
  disaggregatedTarget:
    targetRef:
      kind: ModelServing
      name: llm-vllm-disagg
      apiVersion: workload.serving.volcano.sh/v1alpha1
    roles:
      prefill:
        minReplicas: 1
        maxReplicas: 8
        metrics:                           # override default metrics for prefill
          - name: num_requests_waiting
            targetValue: "5"
        metricSources:
          num_requests_waiting:
            type: Pod
            pod:
              name: num_requests_waiting
              uri: /metrics
              port: 8100
              labelSelector:
                matchLabels:
                  role: prefill
      decode:
        minReplicas: 2
        maxReplicas: 16
        metrics:                           # override default metrics for decode
          - name: gpu_kv_cache_usage_percent
            targetValue: "80"
        metricSources:
          gpu_kv_cache_usage_percent:
            type: Prometheus
            prometheus:
              serverURL: http://kube-prometheus-stack-prometheus.monitoring.svc:9090
              query: avg(vllm_gpu_kv_cache_usage_percent{role="decode",model="llm-vllm-disagg"})
    ratioConstraints:
      - numeratorRole: prefill
        denominatorRole: decode
        minRatio: "0.25"                  # P:D >= 1:4
        maxRatio: "1"                     # P:D <= 1:1
```

##### Homogeneous scaling (single resource, before vs. after)

Before (two resources):

```yaml
apiVersion: workload.serving.volcano.sh/v1alpha1
kind: AutoscalingPolicy
metadata:
  name: my-policy
spec:
  tolerancePercent: 10
  metrics:
    - name: pending_requests
      targetValue: "5"
  behavior: { ... }
---
apiVersion: workload.serving.volcano.sh/v1alpha1
kind: AutoscalingPolicyBinding
metadata:
  name: my-binding
spec:
  policyRef:
    name: my-policy
  homogeneousTarget:
    target:
      targetRef:
        kind: ModelServing
        name: my-model
    minReplicas: 1
    maxReplicas: 10
```

After (single resource):

```yaml
apiVersion: workload.serving.volcano.sh/v1alpha1
kind: AutoscalingPolicy
metadata:
  name: my-policy
spec:
  tolerancePercent: 10
  metrics:
    - name: pending_requests
      targetValue: "5"
  behavior: { ... }
  homogeneousTarget:
    target:
      targetRef:
        kind: ModelServing
        name: my-model
    minReplicas: 1
    maxReplicas: 10
```

#### Validation Rules (Webhook)

| Rule | Scope |
|------|-------|
| Exactly one of `homogeneousTarget`, `heterogeneousTarget`, `disaggregatedTarget` must be set. | `AutoscalingPolicySpec` (CEL) |
| `spec.metrics` must have at least one entry. | `AutoscalingPolicySpec` |
| `metricSources` keys must be a subset of the effective metric names for that scope. | `Target` / `RoleScalingParam` |
| For each `MetricSource`, `type`/backend pairing must be valid (`Pod` -> `pod`, `Prometheus` -> `prometheus`). | `MetricSource` (CEL, preserved) |
| `targetRef.kind` must be `ModelServing`. | `DisaggregatedTarget` |
| `roles` map keys must reference existing roles in the referenced ModelServing and contain at least two entries. | `DisaggregatedTarget` |
| `minReplicas <= maxReplicas` for each role. | `RoleScalingParam` |
| For each `ratioConstraints` item: `numeratorRole != denominatorRole`, both roles exist in `roles`, and `minRatio <= maxRatio`. | `RoleRatioConstraint` (CEL) |
| No two `ratioConstraints` items may share the same `(numeratorRole, denominatorRole)` pair. | `DisaggregatedTarget` (CEL) |
| For each inverse pair `(A→B, B→A)`, the ranges must overlap: `[minRatio, maxRatio]` of `B→A` must intersect `[1/maxRatio, 1/minRatio]` of `A→B`. | `DisaggregatedTarget` (webhook) |
| For every cycle in the constraint graph, the product of `minRatio` values ≤ 1 and the product of `maxRatio` values ≥ 1. | `DisaggregatedTarget` (webhook) |
| For each transitive path `A→…→C`, if an explicit `A→C` constraint exists, the implied range (product of edge ranges) must overlap with the explicit range. | `DisaggregatedTarget` (webhook) |
| For each `ratioConstraints` item, bounds must be jointly achievable given role min/max replicas (when denominator min > 0). | `DisaggregatedTarget` |
| For every constrained role pair `(A,B)`, scalable-to-zero must match: `roles[A].minReplicas == 0` **iff** `roles[B].minReplicas == 0`. | `DisaggregatedTarget` (CEL) |

#### Scaling Semantics (Controller Contract)

> **Note**: Controller implementation is out of scope for this proposal. These semantics define the contract the controller must honor.

1. **Independent metric evaluation**: The controller evaluates metrics independently for each role in `disaggregatedTarget.roles`, producing a desired replica count per role. If a role defines its own `metrics`, those are used; otherwise the controller falls back to the top-level `spec.metrics`.
2. **Metric source resolution**: For each effective metric name, the controller resolves `MetricSource` in this order: role-level `metricSources`, then target-level/default semantics. Resolved sources can be pod scraping or Prometheus query.
3. **Per-role clamping**: Each desired count is clamped to `[minReplicas, maxReplicas]` of the corresponding role.
4. **Coupled scale-to-zero (per constrained pair)**: For each pair appearing in `ratioConstraints`, both roles in that pair must reach zero together. The controller does not evaluate that pair's ratio while either side is `0`.
5. **Ratio enforcement**: For each configured role pair, when both roles are non-zero, after clamping the controller adjusts replica counts to satisfy `minRatio <= replicas[numeratorRole]/replicas[denominatorRole] <= maxRatio`.
6. **Atomic patch**: The controller patches all affected `spec.template.roles[*].replicas` in a single ModelServing update to avoid transient states that violate ratio constraints.

#### Multi-Constraint Conflict Analysis

When `ratioConstraints` contains multiple entries, the constraints may be mutually unsatisfiable. This section enumerates all known conflict categories and specifies how the system must detect or handle each one.

##### Conflict Taxonomy

###### Type 1: Cyclic Inconsistency

When constraints form a directed cycle — for example three constraints `A→B`, `B→C`, `C→A` — the product of ratios around the cycle is a mathematical identity:

$$\frac{r_A}{r_B} \times \frac{r_B}{r_C} \times \frac{r_C}{r_A} = 1$$

For the cycle to be satisfiable, the product of the constraint ranges must contain `1`:

- product of all `minRatio` values along the cycle ≤ 1
- product of all `maxRatio` values along the cycle ≥ 1

**Example (infeasible)**:

```yaml
ratioConstraints:
  - numeratorRole: A
    denominatorRole: B
    minRatio: "0.5"          # A/B = 0.5
    maxRatio: "0.5"
  - numeratorRole: B
    denominatorRole: C
    minRatio: "0.5"          # B/C = 0.5
    maxRatio: "0.5"
  - numeratorRole: C
    denominatorRole: A
    minRatio: "0.5"          # C/A = 0.5
    maxRatio: "0.5"
```

Product of `minRatio`: `0.5 × 0.5 × 0.5 = 0.125 < 1` ✓, but product of `maxRatio`: `0.5 × 0.5 × 0.5 = 0.125 < 1` ✗. The range `[0.125, 0.125]` does not contain `1`, so no solution exists.

A consistent version would be:

```yaml
ratioConstraints:
  - numeratorRole: A
    denominatorRole: B
    minRatio: "0.5"
    maxRatio: "1"
  - numeratorRole: B
    denominatorRole: C
    minRatio: "0.5"
    maxRatio: "1"
  - numeratorRole: C
    denominatorRole: A
    minRatio: "1"
    maxRatio: "4"
# min product = 0.5 × 0.5 × 1 = 0.25 ≤ 1  ✓
# max product = 1 × 1 × 4 = 4 ≥ 1  ✓
```

This generalizes to any cycle length: for a cycle of `k` edges, the product condition must hold.

###### Type 2: Transitive Inconsistency

Two constraints `A→B` and `B→C` imply a derived range for `A→C`:

$$m_{AB} \times m_{BC} \leq \frac{r_A}{r_C} \leq M_{AB} \times M_{BC}$$

If the user also provides an explicit `A→C` constraint, the explicit range must overlap with the implied range. Otherwise the constraints are unsatisfiable.

**Example (infeasible)**:

```yaml
ratioConstraints:
  - numeratorRole: A
    denominatorRole: B
    minRatio: "2"
    maxRatio: "3"    # A/B ∈ [2,3]
  - numeratorRole: B
    denominatorRole: C
    minRatio: "2"
    maxRatio: "3"    # B/C ∈ [2,3]
  # Implied: A/C ∈ [4, 9]
  - numeratorRole: A
    denominatorRole: C
    minRatio: "1"
    maxRatio: "2"   # A/C ∈ [1,2] — no overlap with [4,9]
```

###### Type 3: Inverse Pair Inconsistency

If both `A→B` and `B→A` constraints exist, they must be inverses. Constraint `A→B ∈ [m₁, M₁]` implies `B→A ∈ [1/M₁, 1/m₁]`. The explicit `B→A ∈ [m₂, M₂]` must overlap:

$$\max(m_2,\ 1/M_1) \leq \min(M_2,\ 1/m_1)$$

**Example (infeasible)**:

```yaml
ratioConstraints:
  - numeratorRole: A
    denominatorRole: B
    minRatio: "2"
    maxRatio: "3"    # A/B ∈ [2,3] → B/A ∈ [0.33,0.5]
  - numeratorRole: B
    denominatorRole: A
    minRatio: "1"
    maxRatio: "2"    # B/A ∈ [1,2] — no overlap with [0.33,0.5]
```

###### Type 4: Duplicate Pair Conflict

Two constraints that reference the same ordered pair `(A, B)` with non-overlapping ranges.

**Example (infeasible)**:

```yaml
ratioConstraints:
  - numeratorRole: A
    denominatorRole: B
    minRatio: "1"
    maxRatio: "2"
  - numeratorRole: A
    denominatorRole: B
    minRatio: "4"
    maxRatio: "5"
```

###### Type 5: Replica Bound–Ratio Conflict

Even if ratio constraints are mathematically consistent with each other, they may be unsatisfiable given the integer `minReplicas`/`maxReplicas` bounds of each role.

**Example (infeasible)**:

```yaml
roles:
  A:
    minReplicas: 1
    maxReplicas: 2
  B:
    minReplicas: 1
    maxReplicas: 2
ratioConstraints:
  - numeratorRole: A
    denominatorRole: B
    minRatio: "3"
    maxRatio: "4"
# A/B ≥ 3 requires A ≥ 3·B ≥ 3, but maxReplicas(A) = 2.
```

###### Type 6: Integer Feasibility Gap

A constraint range that is satisfiable in continuous values but has no integer solution within the replica bounds.

**Example (infeasible in integers)**:

```yaml
roles:
  A:
    minReplicas: 1
    maxReplicas: 1
  B:
    minReplicas: 3
    maxReplicas: 3
ratioConstraints:
  - { numeratorRole: A, denominatorRole: B, minRatio: "0.4", maxRatio: "0.45" }
# A/B = 1/3 ≈ 0.333, outside [0.4, 0.45]. No other integer combination is possible.
```

##### Detection Strategy

Conflicts are detected at **two stages**:

| Stage | Checks | Mechanism |
|-------|--------|-----------|
| **Admission (webhook)** | Type 1 (cycle product), Type 2 (transitive overlap), Type 3 (inverse pair), Type 4 (duplicate pair), Type 5 (bound–ratio) | Webhook validates on create/update. Build a directed constraint graph, enumerate all simple cycles (bounded by role count), verify product conditions, compute transitive closures, cross-check with replica bounds. |
| **Runtime (controller)** | Type 6 (integer feasibility), edge cases missed by continuous analysis | Controller reports `RatioConstraintViolated` condition when no integer assignment satisfies all constraints simultaneously. |

Admission-time validation algorithm sketch:

1. **Build constraint graph**: Nodes = role names, directed edge for each `RoleRatioConstraint` labeled with `[minRatio, maxRatio]`.
2. **Reject duplicate pairs** (Type 4): Error if two edges share the same `(numeratorRole, denominatorRole)`.
3. **Merge inverse pairs** (Type 3): For each edge `A→B [m, M]`, if edge `B→A [m', M']` also exists, verify `[m', M'] ∩ [1/M, 1/m] ≠ ∅`.
4. **Cycle detection** (Type 1): Find all simple cycles using DFS. For each cycle, multiply the `minRatio` values and `maxRatio` values along the cycle. Reject if the product range does not contain `1`.
5. **Transitive closure** (Type 2): For each pair `(A, C)` reachable via a path `A→…→C`, compute the implied range by multiplying edge ranges along the path. If an explicit `A→C` edge exists, verify overlap.
6. **Bound feasibility** (Type 5): For each edge `A→B [m, M]`, verify: `m ≤ maxReplicas(A)/minReplicas(B)` and `M ≥ minReplicas(A)/maxReplicas(B)` (when denominators > 0).

> **Complexity**: With `N` roles, the number of simple cycles is at most exponential in `N`, but in practice `N` is small (typically 2–4 roles). For `N ≤ 10`, exhaustive cycle enumeration is computationally trivial.

##### Resolution Strategies

When the controller encounters conflicts at runtime (Type 6 or transient inconsistencies during scaling), it must choose a resolution strategy. Three candidates are considered:

**Priority-based relaxation**

Each `RoleRatioConstraint` carries an optional `priority` field (higher value = higher priority, default = 0). When constraints cannot all be satisfied simultaneously, the controller drops the lowest-priority constraints first until a feasible solution exists.

```go
type RoleRatioConstraint struct {
    NumeratorRole   string            `json:"numeratorRole"`
    DenominatorRole string            `json:"denominatorRole"`
    MinRatio        resource.Quantity `json:"minRatio"`
    MaxRatio        resource.Quantity `json:"maxRatio"`
    // Priority determines enforcement order when constraints conflict at
    // runtime. Higher values are enforced first. Default is 0.
    // +optional
    // +kubebuilder:default=0
    Priority int32 `json:"priority,omitempty"`
}
```

**Recommendation**: Use priority-based for runtime resolution, combined with fail-closed for admission-time detected conflicts. This ensures:

- Statically detectable conflicts (Types 1–5) are rejected at admission — the user must fix the spec.
- Runtime-only conflicts (Type 6, transient states) are handled gracefully via priority relaxation.
- The controller always reports which constraints were relaxed in `status.disaggregatedStatus.ratioStatuses`.

#### Migration

##### From `AutoscalingPolicy` + `AutoscalingPolicyBinding`

| Before | After |
|--------|-------|
| `AutoscalingPolicy` with metrics + behavior | Same fields stay in `AutoscalingPolicy.spec` |
| `AutoscalingPolicyBinding` with `policyRef` + target | Target fields (including `metricSources` with `Pod`/`Prometheus`) move into `AutoscalingPolicy.spec`; `policyRef` is deleted |
| Two resources per scaling config | One resource |

##### From `SubTarget` P/D bindings

| Before (policy + two bindings with SubTarget) | After (single policy) |
|---|---|
| Policy: metrics + behavior | `spec.metrics` + `spec.behavior` (same policy) |
| Binding A: `homogeneousTarget.target.subTargets: {kind: Role, name: prefill}` | `spec.disaggregatedTarget.roles.prefill` |
| Binding B: `homogeneousTarget.target.subTargets: {kind: Role, name: decode}` | `spec.disaggregatedTarget.roles.decode` |
| 3 resources, no ratio coordination | 1 resource, `ratioConstraints` provides coordination |

### Alternatives

#### Alternative 1: Keep `AutoscalingPolicyBinding` as a separate CRD

Keep the current two-resource model and only add `DisaggregatedTarget` to the binding.

**Rejected because**: The policy/binding split provides no practical benefit — policies are not shared across bindings. It keeps metric targets and metric retrieval sources in different resources, increases the number of objects to manage, and makes the complete autoscaling configuration harder to read. Merging into one resource is simpler for both users and the controller.

#### Alternative 2: Keep `SubTarget` and add ratio annotation

Add a `volcano.sh/pd-ratio-range` annotation to coordinate two separate bindings.

**Rejected because**: Annotations are untyped, unvalidated, and invisible to schema tooling. Coordination between two separate resources via annotations is fragile and hard to reason about.

#### Alternative 3: Generic `roles[]` list instead of `roles` map

```go
type DisaggregatedTarget struct {
    TargetRef  corev1.ObjectReference `json:"targetRef"`
    Roles      []RoleScalingParam     `json:"roles"`
    RatioConstraints []RoleRatioConstraint `json:"ratioConstraints,omitempty"`
}
```

**Rejected because**: a list weakens key-based validation and makes patch/update operations harder (rename and merge semantics are less stable than map keys). `roles` map uses roleName as the canonical key and works better with ratio constraints that reference roles by name.

#### Alternative 4: Extend `HomogeneousTarget` with optional P/D fields

Add `prefill` and `decode` fields inside `HomogeneousTarget`.

**Rejected because**: `HomogeneousTarget` is inherently single-target. Embedding P/D semantics overloads its purpose and creates confusing validation rules (e.g., `minReplicas`/`maxReplicas` at top level vs. per-role). A separate target type is cleaner.

#### Alternative 5: Integer-pair ratio instead of `resource.Quantity`

Express each ratio bound as an explicit numerator/denominator integer pair rather than a single decimal `resource.Quantity`:

```go
// RoleRatio expresses a role-to-role ratio as an integer pair N:D.
// For example, {Numerator: 1, Denominator: 4} means ratio = 1:4 (0.25).
type RoleRatio struct {
    // Numerator is the numerator side of the ratio.
    // +kubebuilder:validation:Minimum=0
    Numerator int32 `json:"numerator"`
    // Denominator is the denominator side of the ratio.
    // +kubebuilder:validation:Minimum=1
    Denominator int32 `json:"denominator"`
}

// RoleRatioConstraintIntPair defines one role-pair ratio constraint.
type RoleRatioConstraintIntPair struct {
    NumeratorRole   string    `json:"numeratorRole"`
    DenominatorRole string    `json:"denominatorRole"`
    MinRatio        RoleRatio `json:"minRatio"`
    MaxRatio        RoleRatio `json:"maxRatio"`
}
```

Example YAML:

```yaml
    ratioConstraints:
      - numeratorRole: prefill
        denominatorRole: decode
        minRatio:                # P:D >= 1:4
          numerator: 1
          denominator: 4
        maxRatio:                # P:D <= 1:1
          numerator: 1
          denominator: 1
```

**Pros**:

- **No unit ambiguity** — integers cannot be misread the way `resource.Quantity` interprets suffixes (`"250m"` → `0.25`), removing a class of user error.
- **Directly mirrors how operators reason** — people think and communicate in terms of "1:4", not "0.25".
- **Exact comparison** — ratio checks become cross-multiplication of integers (`p1*d2 <= p2*d1`), avoiding any decimal parsing or rounding entirely.

**Cons**:

- **Two fields per bound** instead of one — slightly more verbose YAML.
- **Diverges from existing convention** — `AutoscalingPolicyMetric.TargetValue` and other Kthena fields already use `resource.Quantity` for decimal values, so the integer pair would be the odd one out.
- **CEL validation is marginally more complex** — comparisons require cross-multiplication rather than a direct `<=`.

**Decision**: The proposal uses `resource.Quantity` for consistency with the rest of the Kthena API, mitigating the unit-ambiguity concern through documentation and webhook validation (rejecting values with non-empty suffixes). The integer-pair form is recorded here as a viable alternative should the unit ambiguity prove to be a frequent source of user error in practice.
