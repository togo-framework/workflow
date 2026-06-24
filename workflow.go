// Package workflow runs dynamic, multi-step workflows over the togo queue.
//
// A workflow is an ordered list of steps; each step has a TYPE that maps to a
// registered handler (StepFunc). Any plugin can register step types via
// RegisterStep, so a workflow uses other plugins as providers (send a mail,
// charge a payment, call an AI model, …). Steps run asynchronously through the
// kernel Queue, so long pipelines never block a request; shared State is passed
// between steps in a run.
package workflow

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/togo-framework/togo"
)

// Step is one node in a workflow.
type Step struct {
	Type string         `json:"type"`           // a registered step type
	Name string         `json:"name,omitempty"` // optional label
	With map[string]any `json:"with,omitempty"` // step parameters
}

// Workflow is an ordered pipeline of steps. Branching is expressed by step
// types that mutate State / set the next index via StepContext.
type Workflow struct {
	Name  string `json:"name"`
	Steps []Step `json:"steps"`
}

// StepContext is passed to a step handler.
type StepContext struct {
	Kernel *togo.Kernel
	Run    *Run
	// Next overrides the next step index; -1 ends the run early. Zero value
	// (unset) advances to the following step.
	Next *int
}

// StepFunc executes a step. Returning an error fails the run.
type StepFunc func(ctx context.Context, sc *StepContext, with map[string]any) error

var (
	regMu     sync.RWMutex
	stepTypes = map[string]StepFunc{}
)

// RegisterStep registers a step type → handler (call from init() or a plugin's
// Boot). Other plugins register step types so workflows can use them.
func RegisterStep(typ string, fn StepFunc) {
	regMu.Lock()
	stepTypes[typ] = fn
	regMu.Unlock()
}

// Status values for a Run.
const (
	StatusPending = "pending"
	StatusRunning = "running"
	StatusDone    = "done"
	StatusFailed  = "failed"
)

// Run is a single execution of a workflow.
type Run struct {
	ID       string         `json:"id"`
	Workflow string         `json:"workflow"`
	Step     int            `json:"step"`
	Status   string         `json:"status"`
	State    map[string]any `json:"state"`
	Error    string         `json:"error,omitempty"`
	Started  time.Time      `json:"started"`
}

const queueName = "workflow.step"

type stepJob struct {
	RunID string
	Step  int
}

// Service is the workflow runtime stored on the kernel (k.Get("workflow")).
type Service struct {
	k    *togo.Kernel
	mu   sync.Mutex
	defs map[string]Workflow
	runs map[string]*Run
	seq  int
}

func init() {
	togo.RegisterProviderFunc("workflow", togo.PriorityLate+10, func(k *togo.Kernel) error {
		s := &Service{k: k, defs: map[string]Workflow{}, runs: map[string]*Run{}}
		if k.Queue != nil {
			k.Queue.Handle(queueName, func(ctx context.Context, payload any) error {
				job, ok := payload.(stepJob)
				if !ok {
					return fmt.Errorf("workflow: bad job payload %T", payload)
				}
				return s.runStep(ctx, job)
			})
		}
		k.Set("workflow", s)
		return nil
	})
}

// Define registers (or replaces) a workflow definition.
func (s *Service) Define(wf Workflow) {
	s.mu.Lock()
	s.defs[wf.Name] = wf
	s.mu.Unlock()
}

// Definitions lists the registered workflow names.
func (s *Service) Definitions() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.defs))
	for n := range s.defs {
		out = append(out, n)
	}
	return out
}

// Run returns a run by id.
func (s *Service) GetRun(id string) (*Run, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[id]
	return r, ok
}

// Trigger starts a run of a workflow with the given initial State.
func (s *Service) Trigger(ctx context.Context, name string, input map[string]any) (*Run, error) {
	s.mu.Lock()
	if _, ok := s.defs[name]; !ok {
		s.mu.Unlock()
		return nil, fmt.Errorf("workflow: unknown workflow %q", name)
	}
	s.seq++
	run := &Run{ID: fmt.Sprintf("run_%d", s.seq), Workflow: name, Status: StatusPending, State: input, Started: time.Now()}
	if run.State == nil {
		run.State = map[string]any{}
	}
	s.runs[run.ID] = run
	s.mu.Unlock()
	return run, s.dispatch(ctx, stepJob{RunID: run.ID, Step: 0})
}

func (s *Service) dispatch(ctx context.Context, job stepJob) error {
	if s.k.Queue == nil {
		// No queue installed — run inline (synchronous) so the plugin still works.
		return s.runStep(ctx, job)
	}
	return s.k.Queue.Dispatch(ctx, queueName, job)
}

func (s *Service) runStep(ctx context.Context, job stepJob) error {
	s.mu.Lock()
	run, ok := s.runs[job.RunID]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("workflow: unknown run %q", job.RunID)
	}
	wf, ok := s.defs[run.Workflow]
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("workflow: run %s references missing workflow %q", run.ID, run.Workflow)
	}

	if job.Step >= len(wf.Steps) {
		s.setStatus(run, StatusDone, nil)
		return nil
	}

	step := wf.Steps[job.Step]
	regMu.RLock()
	fn, ok := stepTypes[step.Type]
	regMu.RUnlock()
	if !ok {
		err := fmt.Errorf("workflow: unknown step type %q (a plugin must RegisterStep it)", step.Type)
		s.setStatus(run, StatusFailed, err)
		return err
	}

	run.Status = StatusRunning
	run.Step = job.Step
	sc := &StepContext{Kernel: s.k, Run: run}
	if err := fn(ctx, sc, step.With); err != nil {
		s.setStatus(run, StatusFailed, err)
		return err
	}

	next := job.Step + 1
	if sc.Next != nil {
		next = *sc.Next
	}
	if next < 0 || next >= len(wf.Steps) {
		s.setStatus(run, StatusDone, nil)
		return nil
	}
	return s.dispatch(ctx, stepJob{RunID: run.ID, Step: next})
}

func (s *Service) setStatus(run *Run, status string, err error) {
	s.mu.Lock()
	run.Status = status
	if err != nil {
		run.Error = err.Error()
	}
	s.mu.Unlock()
}

// FromKernel fetches the workflow service from the kernel container.
func FromKernel(k *togo.Kernel) (*Service, bool) {
	v, ok := k.Get("workflow")
	if !ok {
		return nil, false
	}
	s, ok := v.(*Service)
	return s, ok
}
