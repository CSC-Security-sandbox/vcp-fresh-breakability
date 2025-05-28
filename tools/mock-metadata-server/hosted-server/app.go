package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"sync"
	"time"
)

type token struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
	TokenType   string `json:"token_type"`
}

var (
	accessToken token
	tokenLock   sync.Mutex
	expiryTime  time.Time
)

// Handler to serve token for clients
func getTokenHandler(w http.ResponseWriter, r *http.Request) {
	tokenLock.Lock()

	// Refresh if token is about to expire (within 10 seconds)
	if time.Now().After(expiryTime.Add(-10 * time.Second)) {
		log.Println("Token is expiring soon, refreshing...")
		tokenLock.Unlock()
		refreshToken()
		tokenLock.Lock()
	}

	tokenLock.Unlock()

	log.Printf("[%s] %s %s", time.Now().Format(time.RFC3339), r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(accessToken); err != nil {
		log.Printf("Failed to encode token: %v", err)
	}
}

// Simple healthcheck/status endpoint
func getStatusHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("[%s] %s %s", time.Now().Format(time.RFC3339), r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	if accessToken.AccessToken != "" {
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	} else {
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "error"})
	}
}

// Fetch token directly from GCP metadata server (since this runs inside the VM)
func getTokenFromMetadataServer() token {
	req, err := http.NewRequest("GET", "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token", nil)
	if err != nil {
		log.Fatalf("Failed to create metadata request: %v", err)
	}
	req.Header.Add("Metadata-Flavor", "Google")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("Failed to contact metadata server: %v", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Failed to close response body: %v", err)
		}
	}()

	var newToken token
	if err := json.NewDecoder(resp.Body).Decode(&newToken); err != nil {
		log.Fatalf("Failed to parse metadata token: %v", err)
	}
	return newToken
}

// Refresh token and schedule the next refresh
func refreshToken() {
	log.Println("Refreshing token...")

	newToken := getTokenFromMetadataServer()

	tokenLock.Lock()
	accessToken = newToken
	expiryTime = time.Now().Add(time.Duration(accessToken.ExpiresIn) * time.Second)
	tokenLock.Unlock()

	log.Printf("[%s] Token refreshed, expires at %v", time.Now().Format(time.RFC3339), expiryTime)

	// Schedule next refresh 10 minutes before expiry
	refreshAfter := time.Duration(accessToken.ExpiresIn-600) * time.Second
	if refreshAfter <= 0 {
		refreshAfter = time.Duration(accessToken.ExpiresIn/2) * time.Second
	}
	time.AfterFunc(refreshAfter, refreshToken)
}

func main() {
	port := flag.String("port", "9090", "Port to listen on")
	flag.Parse()

	log.Println("Starting metadata proxy server on sde-atom-vm1...")

	// Initial token fetch
	refreshToken()

	// Handlers
	http.HandleFunc("/computeMetadata/v1/instance/service-accounts/default/token", getTokenHandler)
	http.HandleFunc("/status", getStatusHandler)

	log.Printf("Listening on :%s", *port)
	if err := http.ListenAndServe(":"+*port, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}