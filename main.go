package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Peer struct {
	conn        *websocket.Conn
	id          string
	connectedAt time.Time
}

type Room struct {
	mu     sync.Mutex
	peers  []*Peer
	buffer [][]byte
}

type Server struct {
	mu    sync.Mutex
	rooms map[string]*Room
}

func newServer() *Server {
	return &Server{rooms: make(map[string]*Room)}
}

func (s *Server) getOrCreateRoom(id string) *Room {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.rooms[id]; ok {
		return r
	}
	r := &Room{}
	s.rooms[id] = r
	return r
}

func (s *Server) deleteRoom(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.rooms, id)
}

var peerCounter int
var peerMu sync.Mutex

func nextPeerID() string {
	peerMu.Lock()
	defer peerMu.Unlock()
	peerCounter++
	return fmt.Sprintf("peer-%d", peerCounter)
}

func (r *Room) handle(p *Peer, roomID string, s *Server) {
	r.mu.Lock()

	if len(r.peers) >= 2 {
		r.mu.Unlock()
		log.Printf("[%s] room piena, rifiuto %s", roomID, p.id)
		p.conn.Close()
		return
	}

	r.peers = append(r.peers, p)
	count := len(r.peers)

	// Secondo peer: scarica il buffer
	var toFlush [][]byte
	if count == 2 {
		toFlush = r.buffer
		r.buffer = nil
	}
	r.mu.Unlock()

	log.Printf("[%s] %s connesso (%d/2)", roomID, p.id, count)

	for _, msg := range toFlush {
		p.conn.WriteMessage(websocket.TextMessage, msg)
	}

	defer func() {
		r.mu.Lock()
		for i, peer := range r.peers {
			if peer == p {
				r.peers = append(r.peers[:i], r.peers[i+1:]...)
				break
			}
		}
		remaining := len(r.peers)
		r.mu.Unlock()

		duration := time.Since(p.connectedAt).Round(time.Second)
		log.Printf("[%s] %s disconnesso dopo %v (%d/2 rimasti)", roomID, p.id, duration, remaining)

		if remaining == 0 {
			s.deleteRoom(roomID)
			log.Printf("[%s] room eliminata", roomID)
		}

		p.conn.Close()
	}()

	for {
		_, msg, err := p.conn.ReadMessage()
		if err != nil {
			break
		}

		var env map[string]any
		if json.Unmarshal(msg, &env) == nil {
			log.Printf("[%s] %s → %v", roomID, p.id, env["type"])
		}

		r.mu.Lock()
		sent := false
		for _, peer := range r.peers {
			if peer != p {
				peer.conn.WriteMessage(websocket.TextMessage, msg)
				sent = true
			}
		}
		if !sent {
			r.buffer = append(r.buffer, msg)
			log.Printf("[%s] messaggio bufferizzato (%d in coda)", roomID, len(r.buffer))
		}
		r.mu.Unlock()
	}
}

func main() {
	s := newServer()

	// /signal?room=default  — room opzionale, default se non specificata
	http.HandleFunc("/signal", func(w http.ResponseWriter, r *http.Request) {
		roomID := r.URL.Query().Get("room")
		if roomID == "" {
			roomID = "default"
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("upgrade error:", err)
			return
		}

		peer := &Peer{
			conn:        conn,
			id:          nextPeerID(),
			connectedAt: time.Now(),
		}

		room := s.getOrCreateRoom(roomID)
		room.handle(peer, roomID, s)
	})

	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		type roomInfo struct {
			Peers    int `json:"peers"`
			Buffered int `json:"buffered"`
		}
		result := make(map[string]roomInfo)
		for id, room := range s.rooms {
			room.mu.Lock()
			result[id] = roomInfo{len(room.peers), len(room.buffer)}
			room.mu.Unlock()
		}
		json.NewEncoder(w).Encode(result)
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("Online"))
		if err != nil {
			return
		}
	})

	log.Println("signaling server :8080")
	log.Println("  ws://localhost:8080/signal         (room default)")
	log.Println("  ws://localhost:8080/signal?room=X  (room nominata)")
	log.Println("  http://localhost:8080/status        (stato rooms)")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
