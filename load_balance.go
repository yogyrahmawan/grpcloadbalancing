package grpcloadbalancing

import (
	"errors"
	"sync"
)

/*
* based on http://kb.linuxvirtualserver.org/wiki/Weighted_Round-Robin_Scheduling#
 */

// LoadBalance represent loadbalance . prefer to use mutex since shared state
type LoadBalance struct {
	sync.Mutex
	lastSelected int // lastSelected server
	cw           int // current weight
	gcd          int // gcd of all weights
	maxWeight    int
	endpoints    []*Endpoint
}

// NewLoadBalance create load balance
func NewLoadBalance(endpoints []*Endpoint) *LoadBalance {
	// calculate gcd
	return &LoadBalance{
		lastSelected: -1,
		cw:           0,
		endpoints:    endpoints,
		gcd:          <-calculateEndpointsGCD(endpoints),
		maxWeight:    <-getMaxWeight(endpoints),
	}
}

// Get endpoint load balance
func (l *LoadBalance) Get() (*Endpoint, error) {
	l.Lock()
	defer l.Unlock()
	endPoint, err := l.getEndpoint()
	if err != nil {
		return nil, err
	}

	if err := endPoint.checkOrInitiateNewConnection(); err != nil {
		return nil, err
	}

	return endPoint, nil
}

// AddEndpoint add new endpoint and reset stats
func (l *LoadBalance) AddEndpoint(endpoint *Endpoint) {
	l.Lock()
	defer l.Unlock()
	l.endpoints = append(l.endpoints, endpoint)
	l.reset()
}

// reset and recalculated lastSelected, cw, gcd, maxWeight
func (l *LoadBalance) reset() {
	l.Lock()
	defer l.Unlock()

	l.lastSelected = -1
	l.cw = 0
	l.gcd = <-calculateEndpointsGCD(l.endpoints)
	l.maxWeight = <-getMaxWeight(l.endpoints)
}

// Destroy connection and commit suicide
func (l *LoadBalance) Destroy() error {
	l.Lock()
	defer l.Unlock()
	for _, v := range l.endpoints {
		v.destroy()
	}

	l.endpoints = nil

	return nil
}

func (l *LoadBalance) getEndpoint() (*Endpoint, error) {
	for {
		l.lastSelected = (l.lastSelected + 1) % len(l.endpoints)
		if l.lastSelected == 0 {
			l.cw = l.cw - l.gcd
			if l.cw <= 0 {
				l.cw = l.maxWeight
				if l.cw == 0 {
					return nil, errors.New("null error")
				}
			}
		}

		if l.endpoints[l.lastSelected].getWeight() >= l.cw {
			return l.endpoints[l.lastSelected], nil
		}
		continue

	}
}

func getMaxWeight(endpoints []*Endpoint) chan int {
	result := make(chan int)
	go func() {
		max := -1
		for _, v := range endpoints {
			if v.getWeight() > max {
				max = v.getWeight()
			}
		}

		result <- max
		close(result)
	}()

	return result
}

func calculateEndpointsGCD(endpoints []*Endpoint) chan int {
	gcdResult := make(chan int)
	go func() {
		divider := -1
		for _, v := range endpoints {
			if divider == -1 {
				divider = v.getWeight()
			} else {
				divider = gcd(divider, v.getWeight())
			}
		}
		gcdResult <- divider
		close(gcdResult)
	}()

	return gcdResult

}

func gcd(a, b int) int {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}
