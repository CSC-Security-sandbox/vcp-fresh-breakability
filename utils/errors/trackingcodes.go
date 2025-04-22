package errors

// GetTrackingID calls GetTrackingID for the specific error type of the error parameter
func GetTrackingID(err error) int {
	u, ok := err.(interface {
		GetTrackingID() int
	})
	if !ok {
		return 0
	}
	return u.GetTrackingID()
}
