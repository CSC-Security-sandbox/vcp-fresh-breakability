package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os/exec"
	"strings"
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

func getTokenHandler(w http.ResponseWriter, r *http.Request) {
	tokenLock.Lock()

	// Check if the token is expiring soon (10-second buffer)
	if time.Now().After(expiryTime.Add(-10 * time.Second)) {
		log.Println("Token is expiring soon, refreshing...")
		refreshToken()
	}

	tokenLock.Unlock()

	log.Printf(" %s - Request - Method: %s, Path: %s \n", time.Now().String(), r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(accessToken)
	if err != nil {
		return
	}
}

func getStatusHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("Request - Method: %s, Path: %s \n", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	log.Printf("accessToken: %v\n", accessToken.AccessToken)
	if accessToken.AccessToken != "" {
		err := json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		if err != nil {
			return
		}
	} else {
		err := json.NewEncoder(w).Encode(map[string]string{"status": "error"})
		if err != nil {
			return
		}
	}
}

func setupSSHKeys(zone, vmName, project string) error {
	cmd := exec.Command("gcloud", "compute", "ssh", "--zone", zone, vmName, "--project", project, "--dry-run")
	return cmd.Run()
}

func getTokenFromVM(zone, project, vmName string) token {
	if err := setupSSHKeys(zone, vmName, project); err != nil {
		log.Fatalf("SSH key setup failed: %v", err)
	}

	cmd := exec.Command("bash", "-c", fmt.Sprintf("gcloud compute ssh --zone %q %q --project %q  --command 'curl \"http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token\" -H \"Metadata-Flavor: Google\" '", zone, vmName, project))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}

	if err = cmd.Start(); err != nil {
		log.Fatal(err)
	}

	defer func(cmd *exec.Cmd) {
		err := cmd.Wait()
		if err != nil {
			log.Printf("Error waiting for command: %v", err)
		}
	}(cmd)

	buf := new(strings.Builder)
	_, _ = io.Copy(buf, stdout)

	var newToken token

	err = json.Unmarshal([]byte(buf.String()), &newToken)
	if err != nil {
		log.Printf("Error: %v \n\n", err)
	}

	return newToken
}

func refreshToken() {
	log.Println("Refreshing token...")

	// Fetch new token
	newToken := getTokenFromVM(zone, project, vmName)

	// Update token and expiry time in a thread-safe manner
	tokenLock.Lock()
	accessToken = newToken
	expiryTime = time.Now().Add(time.Duration(accessToken.ExpiresIn) * time.Second)
	tokenLock.Unlock()

	log.Printf(" %s - expires in : %v", time.Now().String(), expiryTime)
	// Schedule next refresh slightly before token expiry
	time.AfterFunc(time.Duration(accessToken.ExpiresIn-600)*time.Second, refreshToken)
}

var (
	zone, project, vmName string
)

func main() {
	flag.StringVar(&zone, "zone", "europe-north1-c", "Specify the zone for the VM")
	flag.StringVar(&project, "project", "ntap-eu-n1-auto-atom-sde-tst", "Specify the project ID")
	flag.StringVar(&vmName, "vmname", "sde-atom-vm1", "VM name to get the token")
	port := flag.String("port", "9090", "Port to listen on")
	flag.Parse()

	log.Printf("Generating token for zone %q from VM %q and project %q\n", zone, vmName, project)

	// Initialize the first token and schedule auto-refresh
	refreshToken()

	http.HandleFunc("/computeMetadata/v1/instance/service-accounts/default/token", getTokenHandler)
	// utility endpoint to check if the server is running properly
	http.HandleFunc("/status", getStatusHandler)

	log.Printf(" - Server is running on localhost:%v \n", *port)
	if err := http.ListenAndServe(fmt.Sprintf(":%v", *port), nil); err != nil {
		log.Fatal(err)
	}
}
