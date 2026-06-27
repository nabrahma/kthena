/*
Copyright The Volcano Authors.

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

package datastore

import (
	"container/heap"
	"container/list"
	"context"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// This file extends RequestPriorityQueue with session-boost behavior. When a
// queue is constructed with FairnessQueueConfig.SessionBoostEnabled, the shared
// priority-queue framework in fairness_queue.go reuses the same heap, push/pop,
// cancellation and shutdown logic, while the ordering and dequeue strategy below
// replace per-user fairness with session-aware boosting for prefix-cache reuse.

// BackendWaitingChecker is a function that checks whether the backend pods
// have capacity to accept new requests. It returns true when at least one pod
// has an empty waiting queue (i.e. RequestWaitingNum == 0), meaning the backend
// can accept a new request without queuing.
type BackendWaitingChecker func() bool

// SessionTracker tracks recently completed sessions for priority boosting using
// a bounded LRU cache. It remembers the N most-recently-completed sessions (N is
// the configured capacity); follow-up requests belonging to one of those sessions
// are boosted so they can reuse the still-warm prefix cache on the backend.
//
// An LRU bound is used instead of a time-based TTL because it directly mirrors how
// inference engines (e.g. vLLM) evict their KV/prefix cache: the least-recently-used
// sessions fall out first. This means operators only need to size the cache by the
// number of concurrent conversations they want to keep warm, rather than guessing a
// duration. Under high load, stale sessions are evicted quickly; under low load,
// keeping a few extra entries is harmless because boosting only matters when the
// queue is contended.
type SessionTracker struct {
	mu       sync.Mutex
	capacity int
	// ll holds session IDs ordered by recency: front = most recently completed,
	// back = least recently used (next to be evicted).
	ll    *list.List
	items map[string]*list.Element // sessionID -> element in ll
}

// NewSessionTracker creates a new session tracker that remembers up to capacity
// most-recently-completed sessions. A non-positive capacity falls back to the
// default.
func NewSessionTracker(capacity int) *SessionTracker {
	if capacity <= 0 {
		capacity = defaultSessionBoostMaxSessions
	}
	return &SessionTracker{
		capacity: capacity,
		ll:       list.New(),
		items:    make(map[string]*list.Element),
	}
}

// MarkCompleted records that a request from the given session has completed,
// promoting it to the most-recently-used position. When the cache exceeds its
// capacity, the least-recently-used session is evicted.
func (st *SessionTracker) MarkCompleted(sessionID string) {
	if sessionID == "" {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if el, ok := st.items[sessionID]; ok {
		st.ll.MoveToFront(el)
		return
	}
	el := st.ll.PushFront(sessionID)
	st.items[sessionID] = el
	if st.ll.Len() > st.capacity {
		oldest := st.ll.Back()
		if oldest != nil {
			st.ll.Remove(oldest)
			delete(st.items, oldest.Value.(string))
			klog.V(4).Infof("[SessionTracker] evicted LRU session %q, tracked=%d/%d",
				oldest.Value.(string), st.ll.Len(), st.capacity)
		}
	}
}

// HasRecentCompletion reports whether the given session ID is currently tracked
// (i.e. it is among the N most-recently-completed sessions). It is a pure read and
// does not change recency ordering.
func (st *SessionTracker) HasRecentCompletion(sessionID string) bool {
	if sessionID == "" {
		return false
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	_, exists := st.items[sessionID]
	return exists
}

// ActiveSessions returns the number of sessions currently tracked.
func (st *SessionTracker) ActiveSessions() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.ll.Len()
}

// MarkSessionCompleted records that a request from the given session has completed,
// enabling priority boosting for follow-up requests in the same session. No-op when
// the queue is not in session-boost mode.
func (pq *RequestPriorityQueue) MarkSessionCompleted(sessionID string) {
	if pq.sessionTracker != nil {
		pq.sessionTracker.MarkCompleted(sessionID)
	}
}

// GetSessionTracker returns the session tracker, or nil if session boost is disabled.
func (pq *RequestPriorityQueue) GetSessionTracker() *SessionTracker {
	return pq.sessionTracker
}

// GetInflightCount returns the current number of inflight requests in session-boost mode.
func (pq *RequestPriorityQueue) GetInflightCount() int64 {
	return pq.inflightCount.Load()
}

// runSessionBoostMode is the session-boost dequeue loop. It uses backend backpressure
// when a checker is configured, otherwise dequeues directly (no rate limiting).
func (pq *RequestPriorityQueue) runSessionBoostMode(ctx context.Context) {
	if pq.backendChecker != nil {
		pq.runBackpressureMode(ctx)
		return
	}
	pq.runDirectMode(ctx)
}

// admitSessionBoost marks a request as inflight, installs its release callback and
// unblocks the waiting caller by closing its NotifyChan.
func (pq *RequestPriorityQueue) admitSessionBoost(req *Request) {
	pq.inflightCount.Add(1)
	releaseOnce := sync.Once{}
	req.Release = func() {
		releaseOnce.Do(func() {
			pq.inflightCount.Add(-1)
			select {
			case pq.releaseCh <- struct{}{}:
			default:
			}
			pq.metricDecInflight(req.ModelName)
		})
	}
	pq.metricIncInflight(req.ModelName)
	if req.NotifyChan != nil {
		close(req.NotifyChan)
	}
}

// runDirectMode dequeues requests as fast as they arrive with no rate limiting.
func (pq *RequestPriorityQueue) runDirectMode(ctx context.Context) {
	for {
		req, err := pq.popWhenAvailable(ctx)
		if err != nil {
			return
		}
		if req == nil || req.NotifyChan == nil {
			continue
		}
		if req.isCancelled() {
			continue
		}
		pq.admitSessionBoost(req)
	}
}

// runBackpressureMode dequeues requests only when backend pods have capacity.
// Uses two-level admission control:
//  1. Inflight limit: at most MaxConcurrent requests in flight across all backends.
//  2. Backend metrics check: at least one pod reports capacity available.
//
// Session Grace Period: When SessionBoostGracePeriod > 0, a release event triggers
// a short wait before dequeuing to give the same session time to submit a follow-up
// request that can leverage prefix cache.
func (pq *RequestPriorityQueue) runBackpressureMode(ctx context.Context) {
	pollInterval := pq.config.BackpressurePollInterval
	if pollInterval <= 0 {
		pollInterval = 100 * time.Millisecond
	}
	klog.V(4).Infof("[SessionBoost] starting backpressure dequeue loop, poll_interval=%v, gracePeriod=%v",
		pollInterval, pq.config.SessionBoostGracePeriod)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	if pq.config.SessionBoostGracePeriod > 0 {
		pq.runBackpressureWithGrace(ctx, ticker)
	} else {
		pq.runBackpressureNoGrace(ctx, ticker)
	}
}

// runBackpressureNoGrace is the fast path when grace period is disabled (default).
// Listens on notifyCh for immediate dequeue of freshly enqueued requests.
func (pq *RequestPriorityQueue) runBackpressureNoGrace(ctx context.Context, ticker *time.Ticker) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-pq.stopCh:
			return
		case <-pq.releaseCh:
			pq.tryBackpressureDequeue(ctx)
		case <-pq.notifyCh:
			pq.tryBackpressureDequeue(ctx)
		case <-ticker.C:
			pq.tryBackpressureDequeue(ctx)
		}
	}
}

// runBackpressureWithGrace handles the case where grace period is configured.
// The grace wait applies to release events (releaseCh): after a request completes,
// we briefly hold the freed capacity to give the same session a chance to submit a
// follow-up that can reuse the warm prefix cache. New arrivals (notifyCh) are
// admitted immediately when capacity exists, so enabling grace does not add
// admission latency to first turns on an idle queue.
//
// When both a fresh arrival and a release are pending, the release must win so the
// just-freed slot is held for the grace period; Go's select picks randomly between
// ready cases, so the notifyCh branch drains any pending release and routes it
// through the grace path to keep that ordering deterministic. The ticker remains a
// backstop.
func (pq *RequestPriorityQueue) runBackpressureWithGrace(ctx context.Context, ticker *time.Ticker) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-pq.stopCh:
			return
		case <-pq.releaseCh:
			pq.waitGraceAndDequeue(ctx)
		case <-pq.notifyCh:
			// A fresh request arrived. If a release is also pending, prefer the
			// grace path so the freed slot is held briefly for a same-session
			// follow-up; otherwise admit immediately so an idle queue need not wait
			// for the next ticker tick.
			select {
			case <-pq.releaseCh:
				pq.waitGraceAndDequeue(ctx)
			default:
				pq.tryBackpressureDequeue(ctx)
			}
		case <-ticker.C:
			pq.tryBackpressureDequeue(ctx)
		}
	}
}

// isHeadSessionBoosted checks if the highest-priority request in the queue has a session boost.
func (pq *RequestPriorityQueue) isHeadSessionBoosted() bool {
	pq.mu.RLock()
	defer pq.mu.RUnlock()
	if len(pq.heap) == 0 {
		return false
	}
	return pq.heap[0].SessionBoost
}

// waitGraceAndDequeue waits up to SessionBoostGracePeriod for a session-boosted
// request to arrive at the head of the queue.
func (pq *RequestPriorityQueue) waitGraceAndDequeue(ctx context.Context) {
	// Fast path: head is already session-boosted.
	if pq.isHeadSessionBoosted() {
		klog.V(4).Info("[SessionBoost] grace: head already boosted, skipping wait")
		pq.tryBackpressureDequeue(ctx)
		return
	}

	klog.V(4).Infof("[SessionBoost] grace: starting grace period %v", pq.config.SessionBoostGracePeriod)
	timer := time.NewTimer(pq.config.SessionBoostGracePeriod)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-pq.stopCh:
			return
		case <-pq.notifyCh:
			if pq.isHeadSessionBoosted() {
				klog.V(4).Info("[SessionBoost] grace period: session-boosted request arrived, dequeuing immediately")
				pq.tryBackpressureDequeue(ctx)
				return
			}
		case <-timer.C:
			pq.tryBackpressureDequeue(ctx)
			return
		}
	}
}

// drainCancelledLocked removes cancelled/timed-out requests from anywhere in the
// heap, decrements queue-size metrics for each, and rebuilds the heap. The caller
// must hold pq.mu. It returns the number of requests drained.
//
// This is needed because while all backends report busy (or the inflight limit is
// reached), popWhenAvailable is never called, so requests whose CancelCh has fired
// would otherwise linger in the heap and keep the reported queue size inflated
// until capacity returns. Draining them here keeps the queue size accurate and
// avoids wasting a future dequeue slot on an already-dead request. The waiting
// caller detects cancellation via its own request-scoped signal, so we deliberately
// do not close NotifyChan here.
func (pq *RequestPriorityQueue) drainCancelledLocked() int {
	origLen := len(pq.heap)
	if origLen == 0 {
		return 0
	}
	kept := pq.heap[:0]
	for _, req := range pq.heap {
		if req.isCancelled() {
			pq.metricDecSize(req.ModelName, req.UserID)
			pq.metricRecordDuration(req.ModelName, req.UserID, time.Since(req.RequestTime))
			pq.metricIncCancelled(req.ModelName, req.UserID)
			continue
		}
		kept = append(kept, req)
	}
	drained := origLen - len(kept)
	if drained == 0 {
		return 0
	}
	// Release references to the drained tail before shrinking the heap.
	for i := len(kept); i < origLen; i++ {
		pq.heap[i] = nil
	}
	pq.heap = kept
	heap.Init(pq)
	return drained
}

// tryBackpressureDequeue admits as many queued requests as possible in one pass,
// stopping when the inflight limit is reached, backends report no capacity, or
// the queue is empty. This avoids the one-request-per-tick bottleneck during
// initial ramp-up and whenever spare capacity exists.
func (pq *RequestPriorityQueue) tryBackpressureDequeue(ctx context.Context) {
	// In session-boost mode, MaxConcurrent is the global (total) inflight limit.
	// Operators size it from the estimated per-pod concurrency and pod count.
	maxInflight := int64(pq.config.MaxConcurrent)
	if maxInflight <= 0 {
		maxInflight = int64(defaultSessionBoostMaxConcurrent)
	}

	for {
		currentInflight := pq.inflightCount.Load()

		if currentInflight >= maxInflight {
			pq.mu.Lock()
			drained := pq.drainCancelledLocked()
			pq.mu.Unlock()
			klog.V(4).Infof("[SessionBoost] backpressure: inflight limit reached, inflight=%d maxInflight=%d drainedCancelled=%d",
				currentInflight, maxInflight, drained)
			return
		}

		if !pq.backendChecker() {
			pq.mu.Lock()
			drained := pq.drainCancelledLocked()
			queueLen := len(pq.heap)
			pq.mu.Unlock()
			klog.V(4).Infof("[SessionBoost] backpressure: backend pods busy, holding dequeue. queueLen=%d inflight=%d drainedCancelled=%d",
				queueLen, currentInflight, drained)
			return
		}

		pq.mu.RLock()
		queueLen := len(pq.heap)
		pq.mu.RUnlock()
		if queueLen == 0 {
			return
		}

		req, err := pq.popWhenAvailable(ctx)
		if err != nil || req == nil {
			return
		}

		pq.admitSessionBoost(req)

		klog.V(4).Infof("[SessionBoost] backpressure dequeue: user=%s model=%s sessionBoost=%v inflight=%d/%d",
			req.UserID, req.ModelName, req.SessionBoost, pq.inflightCount.Load(), maxInflight)
	}
}
