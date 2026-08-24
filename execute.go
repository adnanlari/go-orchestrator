package saga

import (
	"context"
	"time"

	"github.com/adnanlari/go-orchestrator/internal/idgen"
)

// Execute runs the saga sequentially: each step's Action is invoked in
// definition order, receiving the data returned by the previous step (or
// input, for the first step). It returns the completed Execution record
// together with an error describing why the run did not reach
// StatusCompleted, if any.
//
// Execute freezes the Definition on first call (see Definition.Freeze),
// so it is safe to call Execute concurrently with other Execute calls,
// but not concurrently with AddStep.
//
// This phase does not yet compensate steps that already succeeded when a
// later step fails or the context is cancelled — a step failure simply
// ends the execution in StatusFailed. Reverse-order compensation is
// added in a later phase.
func (d *Definition) Execute(ctx context.Context, input any) (*Execution, error) {
	d.Freeze()

	now := time.Now()
	exec := &Execution{
		ID:        idgen.New(),
		SagaName:  d.name,
		Status:    StatusPending,
		Input:     input,
		Steps:     make([]StepExecution, len(d.steps)),
		CreatedAt: now,
		UpdatedAt: now,
	}
	for i, step := range d.steps {
		exec.Steps[i] = StepExecution{Name: step.Name, Status: StepStatusPending}
	}

	mustTransition(exec, StatusRunning)

	data := input
	for i, step := range d.steps {
		select {
		case <-ctx.Done():
			exec.CurrentStep = ""
			return exec, fail(exec, ctx.Err())
		default:
		}

		exec.CurrentStep = step.Name
		stepExec := &exec.Steps[i]
		mustTransitionStep(stepExec, StepStatusRunning)
		stepExec.Attempts++

		out, err := step.Action(ctx, data)
		if err != nil {
			stepExec.Error = err.Error()
			mustTransitionStep(stepExec, StepStatusFailed)
			exec.CurrentStep = ""
			return exec, fail(exec, &StepError{Step: step.Name, Err: err})
		}
		mustTransitionStep(stepExec, StepStatusSucceeded)
		data = out
	}

	exec.CurrentStep = ""
	exec.Output = data
	mustTransition(exec, StatusCompleted)
	return exec, nil
}

// fail records err on exec and transitions it to StatusFailed. It
// returns err unchanged so callers can `return exec, fail(exec, err)`.
func fail(exec *Execution, err error) error {
	exec.Error = err.Error()
	mustTransition(exec, StatusFailed)
	return err
}

// mustTransition applies a Status transition that the engine itself
// controls and knows to be legal. A failure here means the engine drove
// an execution's status incorrectly, which is a bug in this package, not
// a condition callers can hit — so it panics rather than threading an
// error return through every call site.
func mustTransition(exec *Execution, to Status) {
	if err := exec.transition(to, time.Now()); err != nil {
		panic(err)
	}
}

// mustTransitionStep is mustTransition's StepExecution counterpart.
func mustTransitionStep(step *StepExecution, to StepStatus) {
	if err := step.transition(to, time.Now()); err != nil {
		panic(err)
	}
}
