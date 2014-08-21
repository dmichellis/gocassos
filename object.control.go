package gocassos

import "time"

func (o *Object) Status() string {
	if o == nil {
		return "UNKNOWN"
	}
	if o.failure {
		return "ERR"
	}
	return "OK"
}

func (o *Object) FullName() string {
	if o == nil {
		return ""
	}
	return o.id
}

func (o *Object) Ttl() int64 {
	if o == nil {
		return 0
	}
	if o.Expiration.IsZero() {
		return 0
	}
	return o.Expiration.Unix() - time.Now().Unix()
}
