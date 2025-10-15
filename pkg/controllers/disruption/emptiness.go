/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package disruption

import (
	"context"
	"errors"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
)

// Emptiness is a subreconciler that deletes empty candidates.
type Emptiness struct {
	consolidation
	Validator
}

func NewEmptiness(c consolidation) *Emptiness {
	return &Emptiness{consolidation: c, Validator: NewEmptinessValidator(c)}
}

// ShouldDisrupt is a predicate used to filter candidates
func (e *Emptiness) ShouldDisrupt(ctx context.Context, c *Candidate) bool {
	candidateSummary := summarizeCandidatesForLog([]*Candidate{c})
	var candidateFields map[string]any
	if len(candidateSummary) > 0 {
		candidateFields = candidateSummary[0]
	}
	logger := log.FromContext(ctx).WithValues("candidate", candidateFields)

	// If consolidation is disabled, don't do anything. This emptiness should run for both WhenEmpty and WhenEmptyOrUnderutilized
	if c.NodePool.Spec.Disruption.ConsolidateAfter.Duration == nil {
		e.recorder.Publish(disruptionevents.Unconsolidatable(c.Node, c.NodeClaim, fmt.Sprintf("NodePool %q has consolidation disabled", c.NodePool.Name))...)
		logger.V(1).Info("skipping emptiness candidate, consolidation disabled")
		return false
	}
	// return true if there are no pods and the nodeclaim is consolidatable
	consolidatable := len(c.reschedulablePods) == 0 && c.NodeClaim.StatusConditions().Get(v1.ConditionTypeConsolidatable).IsTrue()
	if consolidatable {
		logger.V(1).Info("emptiness candidate eligible", "reschedulable-pod-count", len(c.reschedulablePods))
	} else {
		logger.V(1).Info("skipping emptiness candidate, pods present or not consolidatable", "reschedulable-pod-count", len(c.reschedulablePods))
	}
	return consolidatable
}

// ComputeCommand generates a disruption command given candidates
//
//nolint:gocyclo
func (e *Emptiness) ComputeCommand(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) (Command, error) {
	if e.IsConsolidated() {
		log.FromContext(ctx).V(1).Info("empty consolidation already satisfied, skipping")
		return Command{}, nil
	}
	candidates = e.sortCandidates(candidates)
	logger := log.FromContext(ctx).WithValues("candidate-count", len(candidates), "candidates", summarizeCandidatesForLog(candidates))
	logger.V(1).Info("evaluating empty consolidation candidates")

	empty := make([]*Candidate, 0, len(candidates))
	constrainedByBudgets := false
	for _, candidate := range candidates {
		candidateSummary := summarizeCandidatesForLog([]*Candidate{candidate})
		var candidateFields map[string]any
		if len(candidateSummary) > 0 {
			candidateFields = candidateSummary[0]
		}
		candidateLog := logger.WithValues("candidate", candidateFields)
		if len(candidate.reschedulablePods) > 0 {
			candidateLog.V(1).Info("skipping non-empty candidate", "reschedulable-pod-count", len(candidate.reschedulablePods))
			continue
		}
		if disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			// set constrainedByBudgets to true if any node was a candidate but was constrained by a budget
			constrainedByBudgets = true
			candidateLog.V(1).Info("skipping candidate due to disruption budget", "remaining-budget", disruptionBudgetMapping[candidate.NodePool.Name])
			continue
		}
		// If there's disruptions allowed for the candidate's nodepool,
		// add it to the list of candidates, and decrement the budget.
		empty = append(empty, candidate)
		candidateLog.V(1).Info("selected empty candidate for disruption")
		disruptionBudgetMapping[candidate.NodePool.Name]--
	}
	// none empty, so do nothing
	if len(empty) == 0 {
		// if there are no candidates, but a nodepool had a fully blocking budget,
		// don't mark the cluster as consolidated, as it's possible this nodepool
		// should be consolidated the next time we try to disrupt.
		if !constrainedByBudgets {
			e.markConsolidated()
			logger.V(1).Info("no empty candidates, marking consolidated")
		} else {
			logger.V(1).Info("no empty candidates, constrained by budgets")
		}
		return Command{}, nil
	}

	cmd := Command{
		Candidates: empty,
	}
	logger.WithValues("selected-candidates", summarizeCandidatesForLog(empty)).V(1).Info("prepared empty consolidation command, awaiting validation")

	// Empty Node Consolidation doesn't use Validation as we get to take advantage of cluster.IsNodeNominated.  This
	// lets us avoid a scheduling simulation (which is performed periodically while pending pods exist and drives
	// cluster.IsNodeNominated already).
	select {
	case <-ctx.Done():
		return Command{}, errors.New("interrupted")
	case <-e.clock.After(consolidationTTL):
	}

	validCmd, err := e.Validate(ctx, cmd, consolidationTTL)
	if err != nil {
		if IsValidationError(err) {
			log.FromContext(ctx).V(1).WithValues(cmd.LogValues()...).Info("abandoning empty node consolidation attempt due to pod churn, command is no longer valid")
			return Command{}, nil
		}
		return Command{}, err
	}

	log.FromContext(ctx).WithValues(validCmd.LogValues()...).Info("validated empty consolidation command")
	return validCmd, nil
}

func (e *Emptiness) Reason() v1.DisruptionReason {
	return v1.DisruptionReasonEmpty
}

func (e *Emptiness) Class() string {
	return GracefulDisruptionClass
}

func (e *Emptiness) ConsolidationType() string {
	return "empty"
}
