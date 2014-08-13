package gocassos

func (o *Object) Status() string {
	if o == nil {
		return "UNKNOWN"
	}
	if o.failure {
		return "ERR"
	}
	return "OK"
}
