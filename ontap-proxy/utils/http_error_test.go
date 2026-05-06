package utils

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHTTPErrorError(t *testing.T) {
	t.Run("WhenMessageIsPresent_ShouldReturnMessage", func(t *testing.T) {
		err := &HTTPError{Status: http.StatusBadGateway, Message: "custom message"}
		assert.Equal(t, "custom message", err.Error())
	})

	t.Run("WhenMessageIsEmpty_ShouldReturnStatusText", func(t *testing.T) {
		err := &HTTPError{Status: http.StatusServiceUnavailable}
		assert.Equal(t, http.StatusText(http.StatusServiceUnavailable), err.Error())
	})
}
