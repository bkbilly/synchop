package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"synchop/internal/engine"
)

type Hub struct {
	mu      sync.RWMutex
	clients map[chan []byte]bool
}

func NewHub() *Hub {
	return &Hub{
		clients: make(map[chan []byte]bool),
	}
}

func (h *Hub) BroadcastState(state engine.SyncState) {
	payload, err := json.Marshal(state)
	if err != nil {
		return
	}

	msg := []byte(fmt.Sprintf("event: state\ndata: %s\n\n", payload))

	h.mu.RLock()
	defer h.mu.RUnlock()

	for ch := range h.clients {
		select {
		case ch <- msg:
		default:
		}
	}
}

func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	clientChan := make(chan []byte, 50)

	h.mu.Lock()
	h.clients[clientChan] = true
	h.mu.Unlock()

	defer func() {
		h.mu.Lock()
		delete(h.clients, clientChan)
		close(clientChan)
		h.mu.Unlock()
	}()

	fmt.Fprintf(w, "event: ping\ndata: {}\n\n")
	flusher.Flush()

	notify := r.Context().Done()
	for {
		select {
		case <-notify:
			return
		case msg, ok := <-clientChan:
			if !ok {
				return
			}
			_, _ = w.Write(msg)
			flusher.Flush()
		}
	}
}
