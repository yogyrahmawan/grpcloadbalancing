package grpcloadbalancing

import (
	"time"

	"google.golang.org/grpc"
)

// Endpoint represent endpoint struct
type Endpoint struct {
	*grpc.ClientConn
	url       string
	weight    int
	maxIddle  int
	lastUsed  time.Time
	generator func() (*grpc.ClientConn, error)
}

// NewEndpoint create endpoint together with client connection
func NewEndpoint(url string, weight, maxIddle int, generator func() (*grpc.ClientConn, error)) (*Endpoint, error) {
	var err error
	e := new(Endpoint)
	e.url = url
	e.weight = weight
	e.maxIddle = maxIddle
	e.lastUsed = time.Now()

	e.ClientConn, err = generator()
	if err != nil {
		return nil, err
	}
	e.generator = generator
	return e, nil
}

// GetClientConn get client connection of this endpoint
func (e *Endpoint) GetClientConn() *grpc.ClientConn {
	return e.ClientConn
}

func (e *Endpoint) checkOrInitiateNewConnection() error {
	if e.lastUsed.Add(time.Duration(e.maxIddle) * time.Second).Before(time.Now()) {
		e.ClientConn.Close()
		e.ClientConn = nil
	}

	var err error
	if e.ClientConn == nil {
		e.ClientConn, err = e.generator()
		if err != nil {
			return err
		}
	}

	e.lastUsed = time.Now()

	return nil
}

func (e *Endpoint) destroy() {
	e.ClientConn.Close()
}
