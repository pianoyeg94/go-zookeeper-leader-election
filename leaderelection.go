package leaderelection

import (
	"context"
	"errors"
	"fmt"
	"path"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/google/uuid"

	"github.com/pianoyeg94/go-zookeeper-leader-election/ctxt"
	"github.com/pianoyeg94/go-zookeeper-leader-election/goruntime"
	"github.com/pianoyeg94/go-zookeeper-leader-election/timer"
)

const (
	flagEphemeralSequential = zk.FlagEphemeral | zk.FlagSequence

	electionResource = "election"
	serversResource  = "servers"
	protectedPrefix  = "_c_"
)

var (
	ErrTmpltServerAlreadyRegistered = "leaderelection: server with id %d already registered"

	ErrResign = errors.New("leaderelection: resigned")

	ErrZKNoSession   = errors.New("zk: no session")
	ErrZKLostSession = errors.New("zk: lost session")
	ErrZKNoChildren  = errors.New("zk: node has no children")
)

type (
	Leader interface {
		Lead(ctx context.Context, zk *zk.Conn) error
	}
	Follower interface {
		Follow(ctx context.Context, zk *zk.Conn) error
	}

	LeaderRoutine   func(ctx context.Context, zk *zk.Conn) error
	FollowerRoutine func(ctx context.Context, zk *zk.Conn) error
)

func (lr LeaderRoutine) Lead(ctx context.Context, zk *zk.Conn) error     { return lr(ctx, zk) }
func (fr FollowerRoutine) Follow(ctx context.Context, zk *zk.Conn) error { return fr(ctx, zk) }

var (
	_ Leader   = LeaderRoutine(nil)
	_ Follower = FollowerRoutine(nil)
)

func NewLeaderElection(id int64, namespace string, servers []string, sessionTimeout time.Duration) *LeaderElection {
	if !strings.HasPrefix(namespace, "/") {
		namespace = "/" + namespace
	}
	return &LeaderElection{
		id:             id,
		namespace:      namespace,
		servers:        servers,
		sessionTimeout: sessionTimeout,
	}
}

type LeaderElection struct {
	id             int64
	namespace      string
	servers        []string
	sessionTimeout time.Duration

	conn          *zk.Conn
	sessionEvents chan sessionEvent

	candidatePath string
	candidateId   string

	resignCtx context.Context
	resignFn  context.CancelFunc
	resignWg  sync.WaitGroup
}

func (e *LeaderElection) Join(leader Leader, follower Follower) error {
	e.resignWg.Add(1)
	defer e.resignWg.Done()

	e.resignCtx, e.resignFn = context.WithCancel(context.Background())
	sessionId, err := e.acquireSession()
	if err != nil {
		return err
	}

joinElection:
	for !ctxt.ContextDone(e.resignCtx) {
		if sessionId, err = e.maybeInitializeElection(sessionId); err != nil {
			if err == zk.ErrNoNode {
				continue joinElection
			}
			return err
		}

		if sessionId, err = e.registerServer(sessionId); err != nil {
			return err
		}

		if sessionId, err = e.makeCandidateOffer(sessionId); err != nil {
			return err
		}

		if sessionId, err = e.maybeLostSession(sessionId); err != nil {
			if err == ErrZKLostSession {
				continue joinElection
			}
			return err
		}

		var candidates []string
		switch candidates, sessionId, err = e.getCandidates(sessionId); err {
		case ErrZKLostSession, zk.ErrNoNode, ErrZKNoChildren:
			continue joinElection
		default:
			if err != nil {
				return err
			}
		}

		for !ctxt.ContextDone(e.resignCtx) {
			if sessionId, err = e.maybeLostSession(sessionId); err != nil {
				if err == ErrZKLostSession {
					continue joinElection
				}
				return err
			}

			if e.hasAcquiredLeadership(candidates) {
				if sessionId, err = e.executeLeaderScenario(sessionId, leader); err == nil || err == ErrZKLostSession {
					continue joinElection
				}
				return err
			}

			switch sessionId, err = e.executeFollowerScenario(sessionId, candidates, follower); err {
			case nil:
				candidates = []string{e.candidateId}
			case ErrZKLostSession:
				continue joinElection
			default:
				return err
			}
		}
	}

	return ErrResign
}

func (e *LeaderElection) Resign() {
	e.resignFn()
	e.resignWg.Wait()
	e.conn.Close()
	e.waitResigned()
	if e.sessionEvents != nil {
		close(e.sessionEvents)
	}
}

// waitResigned spins and periodically yields
// to the runtime scheduler until the underlying
// zookeeper connection is closed.
func (e *LeaderElection) waitResigned() {
	// active spinning is never a good idea in user-space,
	// so we cap the spinning time to 5 microseconds
	const yieldDelay = 5 * 1000
	var nextYield int64
	for i := 0; e.conn.State() != zk.StateDisconnected; i++ {
		if i == 0 {
			nextYield = goruntime.Nanotime() + yieldDelay
		}

		if goruntime.Nanotime() < nextYield {
			for x := 0; x < 10 && e.conn.State() != zk.StateDisconnected; x++ {
				goruntime.Procyield(1) // calls the assembly PAUSE instruction once (x86 ISR)
			}
		} else {
			// switches to g0's OS scheduling stack,
			// puts the current goroutine on to the global run queue
			// and eventually schedules another goroutine to run
			// on the current P's M (OS thread).
			runtime.Gosched()
			nextYield = goruntime.Nanotime() + yieldDelay/2 // spin for another 2.5 microseconds
		}
	}
}

func (e *LeaderElection) acquireSession() (sessionId int64, err error) {
	const (
		delay      = 0
		backoff    = 200 * time.Millisecond
		maxBackoff = 1 * time.Second
	)
	if err = timer.RetryOverTime(e.resignCtx, delay, backoff, maxBackoff,
		func(ctx context.Context) (err error) {
			var sessionEvents <-chan zk.Event
			if e.conn, sessionEvents, err = zk.Connect(e.servers, e.sessionTimeout, zk.WithLogInfo(false)); err != nil {
				return err
			}

			for {
				select {
				case event, ok := <-sessionEvents:
					if !ok {
						return ErrZKNoSession
					}

					if ctxt.ContextDone(ctx) {
						return ErrResign
					}

					if event.State == zk.StateHasSession {
						sessionId = e.conn.SessionID()
						e.sessionEvents = make(chan sessionEvent)
						src, dst := sessionEvents, make(chan *zk.Event)
						go e.bufferSessionEvents(dst, src)
						go e.watchSession(sessionId, dst)
						return nil
					}
				case <-ctx.Done():
					return ErrResign
				}
			}
		},
		func(ctx context.Context, err error) bool { return err != ErrResign },
	); err != nil {
		return invalidSessionId, err
	}

	return sessionId, nil
}

func (e *LeaderElection) maybeInitializeElection(sessionId int64) (int64, error) {
	paths := [...]string{e.namespace, path.Join(e.namespace, electionResource)}
	for i := 0; i < len(paths); {
		switch _, err := e.conn.Create(paths[i], nil, 0, zk.WorldACL(zk.PermAll)); err {
		case nil, zk.ErrNodeExists:
			i++
			continue
		case zk.ErrSessionExpired, zk.ErrAuthFailed, zk.ErrConnectionClosed, zk.ErrNoServer:
			if sessionId, err = e.waitSessionReacquire(sessionId); err == ErrResign {
				return sessionId, err
			}
		default:
			return invalidSessionId, err
		}
	}

	return sessionId, nil
}

func (e *LeaderElection) registerServer(sessionId int64) (int64, error) {
	path := path.Join(e.namespace, serversResource, strconv.Itoa(int(e.id)))
createServerIdNode:
	for !ctxt.ContextDone(e.resignCtx) {
		switch _, err := e.conn.Create(path, nil, zk.FlagEphemeral, zk.WorldACL(zk.PermAll)); err {
		case nil:
			return sessionId, nil
		case zk.ErrNodeExists:
			for !ctxt.ContextDone(e.resignCtx) {
				switch _, stats, err := e.conn.Get(path); err {
				case zk.ErrNoNode:
					continue createServerIdNode
				case nil:
					if stats.EphemeralOwner != sessionId {
						return invalidSessionId, fmt.Errorf(ErrTmpltServerAlreadyRegistered, e.id)
					}
					return sessionId, nil
				case zk.ErrSessionExpired, zk.ErrAuthFailed, zk.ErrConnectionClosed, zk.ErrNoServer:
					if sessionId, err = e.waitSessionReacquire(sessionId); err == ErrResign {
						return invalidSessionId, err
					}
				default:
					return invalidSessionId, err
				}
			}
		case zk.ErrSessionExpired, zk.ErrAuthFailed, zk.ErrConnectionClosed, zk.ErrNoServer:
			if sessionId, err = e.waitSessionReacquire(sessionId); err == ErrResign {
				return invalidSessionId, err
			}
		default:
			return invalidSessionId, err
		}
	}

	return invalidSessionId, ErrResign
}

func (e *LeaderElection) makeCandidateOffer(sessionId int64) (int64, error) {
	for !ctxt.ContextDone(e.resignCtx) {
		guid, err := uuid.NewRandom()
		if err != nil {
			return sessionId, err
		}

		switch e.candidatePath, err = e.conn.Create(e.buildCandidateBasePath(&guid), nil, flagEphemeralSequential, zk.WorldACL(zk.PermAll)); err {
		case nil:
			parts := strings.Split(e.candidatePath, "/")
			e.candidateId = parts[len(parts)-1]
			return sessionId, nil
		case zk.ErrSessionExpired, zk.ErrAuthFailed, zk.ErrConnectionClosed, zk.ErrNoServer:
			if sessionId, err = e.waitSessionReacquire(sessionId); err != nil {
				if err != ErrZKLostSession {
					return sessionId, err
				}
				continue
			}

			switch e.candidatePath, sessionId, err = e.recheckCandidateOffer(sessionId, &guid); err {
			case nil:
				if e.candidatePath == "" {
					continue
				}
			case ErrZKLostSession, ErrZKNoChildren:
				continue
			default:
				return sessionId, err
			}
		default:
			return sessionId, err
		}
	}

	return sessionId, ErrResign
}

func (e *LeaderElection) buildCandidateBasePath(guid *uuid.UUID) string {
	return path.Join(e.namespace, electionResource, fmt.Sprintf("%s%s-%d-", protectedPrefix, guid, e.id))
}

func (e *LeaderElection) recheckCandidateOffer(sessionId int64, guid *uuid.UUID) (string, int64, error) {
	for {
		candidates, _, err := e.conn.Children(path.Join(e.namespace, electionResource))
		switch err {
		case nil:
			if len(candidates) == 0 {
				return "", sessionId, ErrZKNoChildren
			}
		case zk.ErrNoNode:
			return "", sessionId, err
		case zk.ErrSessionExpired, zk.ErrAuthFailed, zk.ErrConnectionClosed, zk.ErrNoServer:
			if sessionId, err = e.waitSessionReacquire(sessionId); err != nil {
				return "", sessionId, err
			}
			continue
		default:
			return "", sessionId, err
		}

		return e.restoreCandidatePath(candidates, guid), sessionId, nil
	}
}

func (e *LeaderElection) restoreCandidatePath(candidates []string, guid *uuid.UUID) string {
	for _, candidate := range candidates {
		parts := strings.Split(candidate, "/")
		if candidateId := parts[len(parts)-1]; strings.HasPrefix(candidateId, protectedPrefix) {
			if candidateId[len(protectedPrefix):len(protectedPrefix)+len(guid)] == guid.String() {
				return path.Join(e.namespace, electionResource, candidate)
			}
		}
	}

	return ""
}

func (e *LeaderElection) getCandidates(sessionId int64) ([]string, int64, error) {
	for !ctxt.ContextDone(e.resignCtx) {
		candidates, _, err := e.conn.Children(path.Join(e.namespace, electionResource))
		switch err {
		case nil:
			if len(candidates) == 0 {
				err = ErrZKNoChildren
			}
			sortCandidatesInPlace(candidates)
			return candidates, sessionId, err
		case zk.ErrSessionExpired, zk.ErrAuthFailed, zk.ErrConnectionClosed, zk.ErrNoServer:
			if sessionId, err = e.waitSessionReacquire(sessionId); err != nil {
				return nil, sessionId, err
			}
			continue
		default:
			return nil, sessionId, err
		}
	}

	return nil, sessionId, ErrResign
}

func sortCandidatesInPlace(candidates []string) {
	sort.SliceStable(candidates, func(i, j int) bool {
		iparts := strings.Split(candidates[i], "-")
		jparts := strings.Split(candidates[j], "-")
		inum, _ := strconv.Atoi(iparts[len(iparts)-1])
		jnum, _ := strconv.Atoi(jparts[len(jparts)-1])
		return inum < jnum
	})
}

func (e *LeaderElection) hasAcquiredLeadership(candidates []string) bool {
	if len(candidates) == 0 {
		return false
	}
	return strings.EqualFold(e.candidateId, candidates[0])
}

func (e *LeaderElection) executeLeaderScenario(sessionId int64, leader Leader) (int64, error) {
	for !ctxt.ContextDone(e.resignCtx) {
		ctx, cancel := context.WithCancel(e.resignCtx)
		leaderErrors := make(chan error, 1)
		go func() {
			defer close(leaderErrors)
			if err := leader.Lead(ctx, e.conn); err != nil {
				leaderErrors <- err
			}
		}()

		sessionErrors := make(chan error, 1)
		go func() {
			defer close(sessionErrors)
			select {
			case event := <-e.sessionEvents:
				sessionErrors <- event.err
			case <-ctx.Done():
			}
		}()

		var leaderErr, sessionErr error
		select {
		case leaderErr = <-leaderErrors:
			cancel()
			sessionErr = <-sessionErrors
		case sessionErr = <-sessionErrors:
			cancel()
			leaderErr = <-leaderErrors
		}

		if ctxt.ContextDone(e.resignCtx) {
			return invalidSessionId, ErrResign
		}

		if leaderErr != nil {
			return sessionId, leaderErr
		}

		if sessionErr != nil {
			if sessionId, err := e.waitSessionReacquire(sessionId); err != nil {
				return sessionId, err
			}
		}
	}

	return invalidSessionId, ErrResign
}

func (e *LeaderElection) executeFollowerScenario(sessionId int64, candidates []string, follower Follower) (int64, error) {
	for i, id := range candidates {
		if e.candidateId == id {
			candidates = candidates[:i]
			break
		}

		if i == len(candidates)-1 {
			return sessionId, zk.ErrNoNode
		}
	}

	for !ctxt.ContextDone(e.resignCtx) {
		ctx, cancel := context.WithCancel(e.resignCtx)
		followerErrors := make(chan error, 1)
		go func() {
			defer close(followerErrors)
			if err := follower.Follow(ctx, e.conn); err != nil {
				followerErrors <- err
			}
		}()

		watchErrors := make(chan error, 1)
		go func() {
			defer close(watchErrors)
			var err error
			if sessionId, candidates, err = e.watchNextCandidates(ctx, sessionId, candidates); err != nil {
				watchErrors <- err
			}
		}()

		var followerErr, watchErr error
		select {
		case followerErr = <-followerErrors:
			cancel()
			watchErr = <-watchErrors
		case watchErr = <-watchErrors:
			cancel()
			followerErr = <-followerErrors
		}

		if ctxt.ContextDone(e.resignCtx) {
			return sessionId, ErrResign
		}

		if followerErr != nil {
			return sessionId, followerErr
		}

		switch err := watchErr; err {
		case nil:
			if len(candidates) == 0 {
				return sessionId, nil
			}
		case zk.ErrSessionExpired, zk.ErrAuthFailed, zk.ErrConnectionClosed, zk.ErrNoServer:
			if sessionId, err := e.waitSessionReacquire(sessionId); err != nil {
				return sessionId, err
			}
		default:
			return sessionId, err
		}
	}

	return invalidSessionId, ErrResign
}

func (e *LeaderElection) watchNextCandidates(ctx context.Context, sessionId int64, candidates []string) (int64, []string, error) {
setWatch:
	for len(candidates) > 0 {
		exists, _, events, err := e.conn.ExistsW(path.Join(e.namespace, electionResource, candidates[len(candidates)-1]))
		if err != nil {
			return sessionId, candidates, err
		}

		if !exists {
			candidates = candidates[:len(candidates)-1]
			continue setWatch
		}

		for {
			select {
			case event, ok := <-events:
				if ctxt.ContextDone(ctx) {
					return sessionId, candidates, ErrResign
				}

				if !ok || event.Type == zk.EventNotWatching {
					continue setWatch
				}

				if event.Type == zk.EventNodeDeleted {
					candidates = candidates[:len(candidates)-1]
					continue setWatch
				}
			case event := <-e.sessionEvents:
				return sessionId, candidates, event.err
			case <-ctx.Done():
				return sessionId, candidates, nil
			}
		}
	}

	return sessionId, candidates, nil
}

func (e *LeaderElection) maybeLostSession(sessionId int64) (int64, error) {
	for {
		select {
		case event, ok := <-e.sessionEvents:
			if !ok || ctxt.ContextDone(e.resignCtx) {
				return invalidSessionId, ErrResign
			}

			switch event.sessionStatus {
			case sessionStatusDisconnected, sessionStatusLost:
				return e.waitSessionReacquire(sessionId)
			case sessionStatusAcquired, sessionStatusReacquired:
				if event.sessionId == sessionId {
					return event.sessionId, ErrZKLostSession
				}
				return sessionId, nil
			}
		case <-e.resignCtx.Done():
			return invalidSessionId, ErrResign
		default:
			return sessionId, nil
		}
	}
}

func (e *LeaderElection) waitSessionReacquire(sessionId int64) (int64, error) {
	for {
		select {
		case event, ok := <-e.sessionEvents:
			if !ok || ctxt.ContextDone(e.resignCtx) {
				return invalidSessionId, ErrResign
			}

			switch event.sessionStatus {
			case sessionStatusAcquired:
				return event.sessionId, ErrZKLostSession
			case sessionStatusReacquired:
				return event.sessionId, nil
			}
		case <-e.resignCtx.Done():
			return invalidSessionId, ErrResign
		}
	}
}

func (e *LeaderElection) bufferSessionEvents(dst chan<- *zk.Event, src <-chan zk.Event) {
	e.resignWg.Add(1)
	defer e.resignWg.Done()

	var queue sessionEventsQueue
	for !ctxt.ContextDone(e.resignCtx) {
		select {
		case event := <-src:
			if event.Type == zk.EventSession {
				queue.enqueue(&event)
			}
		case <-e.resignCtx.Done():
			return
		}

		for !ctxt.ContextDone(e.resignCtx) && queue.len() > 0 {
			select {
			case dst <- queue.peek():
				_ = queue.dequeue()
			case event := <-src:
				if event.Type == zk.EventSession {
					queue.enqueue(&event)
				}
			case <-e.resignCtx.Done():
				return
			}
		}
	}
}

func (e *LeaderElection) watchSession(sessionId int64, events <-chan *zk.Event) {
	e.resignWg.Add(1)
	defer e.resignWg.Done()

	prevSessionId := sessionId
	for {
		select {
		case event, ok := <-events:
			if !ok || ctxt.ContextDone(e.resignCtx) {
				return
			}

			switch event.State {
			case zk.StateExpired:
				if notified := e.notifySessionStatusChanged(sessionId, sessionStatusLost, zk.ErrSessionExpired); !notified {
					return
				}
			case zk.StateAuthFailed:
				if notified := e.notifySessionStatusChanged(sessionId, sessionStatusLost, zk.ErrAuthFailed); !notified {
					return
				}
			case zk.StateDisconnected:
				if notified := e.notifySessionStatusChanged(sessionId, sessionStatusDisconnected, zk.ErrConnectionClosed); !notified {
					return
				}
			case zk.StateHasSession:
				if sessionId = e.conn.SessionID(); sessionId == prevSessionId {
					if notified := e.notifySessionStatusChanged(sessionId, sessionStatusReacquired, nil); !notified {
						return
					}
				} else {
					if notified := e.notifySessionStatusChanged(sessionId, sessionStatusAcquired, nil); !notified {
						return
					}
					prevSessionId = sessionId
				}
			}
		case <-e.resignCtx.Done():
			return
		}
	}
}

func (e *LeaderElection) notifySessionStatusChanged(sessionId int64, status sessionStatus, err error) (notified bool) {
	select {
	case e.sessionEvents <- sessionEvent{sessionId: sessionId, sessionStatus: status, err: err}:
		return true
	case <-e.resignCtx.Done():
		return false
	}
}
