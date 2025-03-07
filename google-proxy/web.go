package main

import (
	"io"
	"net/http"

	"github.com/labstack/gommon/log"
)

func handler(w http.ResponseWriter, r *http.Request) {
	resp, err := http.Get("http://core-api:50051/v1/pools")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if _, err := io.Copy(w, resp.Body); err != nil {
		panic(err)
	}
}

func main() {

	log.Infof("leeroy web server ready")

	http.HandleFunc("/", handler)
	http.ListenAndServe(":8080", nil)
}
