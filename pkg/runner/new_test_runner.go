package runner

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/facebookincubator/contest/pkg/cerrors"
	"github.com/facebookincubator/contest/pkg/event/testevent"
	"github.com/facebookincubator/contest/pkg/logging"
	"github.com/facebookincubator/contest/pkg/storage"
	"github.com/facebookincubator/contest/pkg/target"
	"github.com/facebookincubator/contest/pkg/test"
	"github.com/facebookincubator/contest/pkg/types"
)

// ErrPaused will be returned when runner was able to pause successfully.
// When this error is returned from Run(), it is accompanied by valid serialized state
// that can be passed later to Run() to continue from where we left off.
var ErrPaused = errors.New("paused")

type TestRunnerIf interface {
	Run(cancel, pause <-chan struct{}, test *test.Test, targets []*target.Target, jobID types.JobID, runID types.RunID, resumeState []byte) ([]byte, error)
}

// testRunner2 is the state associated with a test run.
type testRunner2 struct {
	cond      *sync.Cond              // Used to notify overseer routine of changes
	steps     []*stepState            // The ipeline, in order of execution
	targets   map[string]*targetState // Target state lookup map
	stepsWg   sync.WaitGroup          // Tracks all the step runners
	targetsWg sync.WaitGroup          // Tracks all the target runners
	log       *logrus.Entry           // Logger

	// One mutex to rule them all, used to serialize access to all the state above.
	// Could probably be split for finer granularity if necessary.
	mu sync.Mutex

	// Pause and cancel channels that were passed externally
	pause  <-chan struct{}
	cancel <-chan struct{}
}

// stepState contains state associated with one state of the pipeline:
type stepState struct {
	sb      test.TestStepBundle
	inCh    chan *target.Target
	outCh   chan *target.Target
	errCh   chan cerrors.TargetError
	ev      testevent.Emitter
	running bool          // Is the goroutine running?
	runErr  error         // Error returned by Run(), if any
	log     *logrus.Entry // Logger
}

// targetStepPhase denotes progression of a target through a step
type targetStepPhase int

const (
	targetStepPhaseInit  targetStepPhase = 0
	targetStepPhaseBegin                 = 1 // Picked up for execution
	targetStepPhaseRun                   = 2 // Injected into step
	targetStepPhaseEnd                   = 3 // Finished running a step
)

// targetState contains state associated with one target
// progressing through the pipeline.
type targetState struct {
	tgt *target.Target

	// This part of state gets serialized into JSON for resumption.
	CurStep  int             `json:"cur_step"`  // Current step number
	CurPhase targetStepPhase `json:"cur_phase"` // Current phase of step execution
	Res      error           `json:"res"`       // Final result, if reached the end state

	finished bool       // Runner goroutine exited
	resCh    chan error // Channel used to communicate result by the step runner.
}

func (ss *stepState) String() string {
	return fmt.Sprintf("[%s]", ss.sb.TestStepLabel)
}

func (ts *targetState) String() string {
	resText := ""
	if ts.Res != nil {
		resText = fmt.Sprintf(" %s", ts.Res)
	}
	return fmt.Sprintf("[%s: %d %s %t%s]",
		ts.tgt, ts.CurStep, ts.CurPhase, ts.finished, resText)
}

// Run is the main enty point of the runner.
func (tr *testRunner2) Run(
	cancel, pause <-chan struct{},
	t *test.Test, targets []*target.Target,
	jobID types.JobID, runID types.RunID,
	resumeState []byte) ([]byte, error) {

	tr.pause = pause
	tr.cancel = cancel

	// Set up logger
	rootLog := logging.GetLogger("pkg/runner")
	fields := make(map[string]interface{})
	fields["jobid"] = jobID
	fields["runid"] = runID
	rootLog = logging.AddFields(rootLog, fields)
	tr.log = logging.AddField(rootLog, "phase", "run")

	// Set up the pipeline
	for _, sb := range t.TestStepsBundles {
		tr.steps = append(tr.steps, &stepState{
			sb:    sb,
			inCh:  make(chan *target.Target),
			outCh: make(chan *target.Target),
			errCh: make(chan cerrors.TargetError),
			ev: storage.NewTestEventEmitterFetcher(testevent.Header{
				JobID:    jobID,
				RunID:    runID,
				TestName: t.Name,
			}),
			log: logging.AddField(tr.log, "step", sb.TestStepLabel),
		})
	}

	// Set up the targets
	tr.targets = make(map[string]*targetState)

	// If we have target state to resume, do it now.
	if len(resumeState) > 0 {
		tr.log.Debugf("Attempting to resume from state: %s", string(resumeState))
		if err := json.Unmarshal(resumeState, &tr.targets); err != nil {
			tr.log.Errorf("Invalid resume state: %s", err)
			// TODO: Is this ok?
			return nil, err
		}
	}
	// Initialize remaining fields of the target structures,
	// build the map and kick off target processing.
	for _, tgt := range targets {
		tstr := tgt.String()
		ts := tr.targets[tstr]
		if ts == nil {
			ts = &targetState{}
		}
		ts.tgt = tgt
		ts.resCh = make(chan error)
		tr.mu.Lock()
		tr.targets[tgt.String()] = ts
		tr.mu.Unlock()
		tr.targetsWg.Add(1)
		go tr.runTarget(ts)
	}

	// Run until no more progress can be made.
	tr.runOverseer()

	// Wait for the entire thing to drain.
	tr.log.Debugf("waiting for target runners to finish")
	tr.targetsWg.Wait()
	tr.log.Debugf("waiting for step runners to finish")
	tr.stepsWg.Wait()

	// Examine the resulting state.
	tr.log.Debugf("done, target states:")
	resumeOk := true
	for i, tgt := range targets {
		ts := tr.targets[tgt.String()]
		tr.log.Debugf("  %d %s", i, ts)
		if ts.CurPhase != targetStepPhaseInit && ts.CurPhase != targetStepPhaseEnd {
			resumeOk = false
		}
	}
	tr.log.Debugf("-- ok to resume? %t", resumeOk)

	var runErr error

	// Have we been asked to pause? If yes, is it safe to do so?
	select {
	case <-pause:
		if !resumeOk {
			tr.log.Warningf("paused but unable to resume")
			break
		}
		resumeState, runErr = json.Marshal(tr.targets)
		if runErr != nil {
			tr.log.Errorf("unable to serialize the state: %s", runErr)
		} else {
			runErr = ErrPaused
		}
	default:
	}

	return resumeState, runErr
}

// runTarget runs one target through all the steps of the pipeline.
func (tr *testRunner2) runTarget(ts *targetState) {
	log := logging.AddField(tr.log, "target", ts.tgt.Name)
	log.Debugf("target runner active")
	// NB: CurStep may be non-zero on entry if resumed
loop:
	for i := ts.CurStep; i < len(tr.steps); {
		// Early check for pause of cancelation.
		select {
		case <-tr.pause:
			log.Debugf("paused 0")
			break loop
		case <-tr.cancel:
			log.Debugf("canceled 0")
			break loop
		default:
		}
		ss := tr.steps[i]
		tr.mu.Lock()
		if ts.CurPhase == targetStepPhaseEnd {
			// This target already terminated.
			// Can happen if resumed from terminal state.
			break loop
		}
		ts.CurStep = i
		ts.CurPhase = targetStepPhaseBegin
		tr.mu.Unlock()
		// Make sure we have a step runner active.
		// These are started on-demand.
		tr.runStepIfNeeded(ss)
		// Inject the target.
		log.Debugf("injecting %q into %q", ts, ss)
		select {
		case ss.inCh <- ts.tgt:
			// Injected successfully.
			tr.mu.Lock()
			ts.CurPhase = targetStepPhaseRun
			tr.mu.Unlock()
			tr.cond.Signal()
			// Check for pause and cancellation.
		case <-tr.pause:
			log.Debugf("paused 1")
			break loop
		case <-tr.cancel:
			log.Debugf("canceled 1")
			break loop
		}
		// Await result. it will be communicaed to us by the step runner.
		select {
		case res, ok := <-ts.resCh:
			if !ok {
				log.Debugf("channel closed")
				break loop
			}
			log.Debugf("target result for %s: %s", ss, res)
			tr.mu.Lock()
			ts.CurPhase = targetStepPhaseEnd
			ts.Res = res
			tr.cond.Signal()
			if res != nil {
				tr.mu.Unlock()
				break loop
			}
			i++
			ts.CurStep = i
			ts.CurPhase = targetStepPhaseInit
			tr.mu.Unlock()
			// Check for cancellation.
			// Notably we are not checking for pause condition here.
			// When paused, we want to let all the injected targets to finish.
		case <-tr.cancel:
			log.Debugf("canceled 2")
			break loop
		}
	}
	log.Debugf("target runner finished")
	tr.mu.Lock()
	ts.finished = true
	tr.mu.Unlock()
	tr.cond.Signal()
	tr.targetsWg.Done()
}

// runStepIfNeeded starts the step runner goroutine if not already running.
func (tr *testRunner2) runStepIfNeeded(ss *stepState) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if ss.running {
		return
	}
	if ss.runErr != nil {
		ss.log.Errorf("Run() already failed (%w)", ss.runErr)
		return
	}
	tr.stepsWg.Add(1)
	ss.running = true
	go tr.runStep(ss)
}

// emitStepEvent emits an error event if step resulted in an error.
func (ss *stepState) emitStepEvent(tgt *target.Target, err error) {
	if err == nil {
		return
	}
	payload, jmErr := json.Marshal(err.Error())
	if jmErr != nil {
		// TODO
	}
	rm := json.RawMessage(payload)
	errEv := testevent.Data{
		EventName: EventTestError,
		Target:    tgt,
		Payload:   &rm,
	}
	ss.ev.Emit(errEv)
}

// reportTargetResult reports result of executing a step to the appropriate target runner.
func (tr *testRunner2) reportTargetResult(tgt *target.Target, res error) {
	tr.mu.Lock()
	ts := tr.targets[tgt.String()]
	tr.mu.Unlock()
	if ts == nil {
		tr.log.Errorf("result for nonexistent target %+v %s", tgt, res)
		return
	}
	select {
	case ts.resCh <- res:
		break
	case <-tr.cancel:
		break
	}
}

// runStep runs a test pipeline's step log (the Run() method) and processes the results.
func (tr *testRunner2) runStep(ss *stepState) {
	ss.log.Debugf("step runner active")
	// Launch the Run() method.
	go func() {
		err := ss.sb.TestStep.Run(tr.cancel, tr.pause, test.TestStepChannels{In: ss.inCh, Out: ss.outCh, Err: ss.errCh}, ss.sb.Parameters, ss.ev)
		ss.emitStepEvent(nil, err)
		tr.mu.Lock()
		defer tr.mu.Unlock()
		ss.runErr = err
		ss.running = false
		// Signal to the result processor that no more will be coming.
		close(ss.outCh)
		close(ss.errCh)
	}()
	// Process the results.
loop:
	for {
		select {
		case tgt, ok := <-ss.outCh:
			if !ok {
				ss.log.Debugf("chan closed")
				break loop
			}
			ss.log.Debugf("step done for %s", tgt.ID)
			tr.reportTargetResult(tgt, nil)
		case res, ok := <-ss.errCh:
			if !ok {
				ss.log.Debugf("chan closed")
				break
			}
			ss.log.Debugf("step failed for %s: %s", res.Target.ID, res.Err)
			ss.emitStepEvent(res.Target, res.Err)
			tr.reportTargetResult(res.Target, res.Err)
		case <-tr.cancel:
			ss.log.Debugf("canceled 3")
			break loop
		}
	}
	ss.log.Debugf("step runner finished")
	tr.stepsWg.Done()
}

// runOverseer monitors progress of targets through the pipeline
// and closes input channels of the steps to indicate that no more are expected.
func (tr *testRunner2) runOverseer() {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	// First, compute the starting step of the pipeline (it may be non-zero
	// if the pipleine was resumed).
	minStep := len(tr.steps)
	for _, ts := range tr.targets {
		if ts.CurStep < minStep {
			minStep = ts.CurStep
		}
	}
	if minStep < len(tr.steps) {
		tr.log.Debugf("overseer: starting at step %s", tr.steps[minStep])
	}

	// Run the main loop.
loop:
	for i := minStep; i < len(tr.steps); {
		// Wait for notification: as targets make progress,
		//target runners notify us via this cond.
		tr.cond.Wait()
		ss := tr.steps[i]
		tr.log.Debugf("overseer: current step %s", ss)
		// Check if all the targets have either made it past the injection phase or terminated.
		for _, ts := range tr.targets {
			if ts.finished {
				continue
			}
			tr.log.Tracef("overseer: %s: %s", ss, ts)
			if ts.CurStep < i || (ts.CurStep == i && ts.CurPhase < targetStepPhaseRun) {
				tr.log.Debugf("overseer: %s: not all targets injected yet", ss)
				continue loop
			}
		}
		// All targets ok, close the step's input channel.
		tr.log.Debugf("overseer: %s: no more targets, closing input channel", ss)
		close(ss.inCh)
		i++
	}
	tr.log.Debugf("overseer: all done")
}

func NewTestRunner2() TestRunnerIf {
	tr := &testRunner2{}
	tr.cond = sync.NewCond(&tr.mu)
	return tr
}

func (tph targetStepPhase) String() string {
	switch tph {
	case targetStepPhaseInit:
		return "init"
	case targetStepPhaseBegin:
		return "begin"
	case targetStepPhaseRun:
		return "run"
	case targetStepPhaseEnd:
		return "end"
	}
	return fmt.Sprintf("???(%d)", tph)
}
