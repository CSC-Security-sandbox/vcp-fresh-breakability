package google

// RetryStrategy defines methods for retrying http requests
type RetryStrategy interface {
	Sleep(error) error
	Reset()
	GetRetryCount() uint
}
