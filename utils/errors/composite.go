package errors

// CompositeErr defines an error that is a composite of multiple errors
type CompositeErr struct {
	errs []error
}

func (ce *CompositeErr) Error() string {
	var e string
	if len(ce.errs) > 0 {
		e = ce.errs[0].Error()
	}
	for _, err := range ce.errs[1:] {
		e += " - " + err.Error()
	}
	return e
}

// Errors returns all errors from which CompositeErr is composed
func (ce *CompositeErr) Errors() []error {
	return ce.errs
}

// NewCompositeErr returns a CompositeErr if more than one non-nil error is passed in
// If only one of the errors passed in is non-nil, will return that error
// If all errors passed in are nil, will return nil
func NewCompositeErr(err1, err2 error, errs ...error) error {
	var flattened []error
	for _, e := range append([]error{err1, err2}, errs...) {
		if e != nil {
			if IsCompositeErr(e) {
				comp := e.(*CompositeErr)
				flattened = append(flattened, comp.errs...)
			} else {
				flattened = append(flattened, e)
			}
		}
	}
	if len(flattened) < 1 {
		return nil
	}
	if len(flattened) < 2 {
		return flattened[0]
	}
	return &CompositeErr{errs: flattened}
}

// IsCompositeErr checks whether the specified error is a CompositeErr
func IsCompositeErr(err error) bool {
	_, is := err.(*CompositeErr)
	return is
}
