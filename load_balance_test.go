package grpcloadbalancing

import (
	"fmt"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"google.golang.org/grpc"
)

type tempEndpoint struct {
	endpoint  *Endpoint
	iteration int
}

var generator = func() (*grpc.ClientConn, error) {
	return &grpc.ClientConn{}, nil
}

func TestGcd(t *testing.T) {
	Convey("test gcd", t, func() {

		Convey("b = 0", func() {
			res := gcd(2, 0)
			So(res, ShouldEqual, 2)
		})

		Convey("running well", func() {
			res := gcd(9, 6)
			So(res, ShouldEqual, 3)
		})

		Convey("calculate all gcd", func() {
			end1, _ := NewEndpoint("localhost:8080", 2, 20, generator)
			end2, _ := NewEndpoint("localhost:8888", 2, 20, generator)
			end3, _ := NewEndpoint("localhost:9999", 7, 20, generator)
			gcd := <-calculateEndpointsGCD([]*Endpoint{end1, end2, end3})
			So(gcd, ShouldEqual, 1)
		})

		Convey("calculate all gcd 2", func() {
			end1, _ := NewEndpoint("localhost:8080", 2, 20, generator)
			end2, _ := NewEndpoint("localhost:8888", 2, 20, generator)
			end3, _ := NewEndpoint("localhost:9999", 6, 20, generator)
			gcd := <-calculateEndpointsGCD([]*Endpoint{end1, end2, end3})
			So(gcd, ShouldEqual, 2)
		})
	})
}

func TestMaxWeight(t *testing.T) {
	Convey("test max weight", t, func() {
		Convey("test max weight ordered", func() {
			end1, _ := NewEndpoint("localhost:8080", 2, 20, generator)
			end2, _ := NewEndpoint("localhost:8888", 2, 20, generator)
			end3, _ := NewEndpoint("localhost:9999", 6, 20, generator)
			max := <-getMaxWeight([]*Endpoint{end1, end2, end3})
			So(max, ShouldEqual, 6)

		})

		Convey("test max weight unordered", func() {
			end1, _ := NewEndpoint("localhost:8080", 2, 20, generator)
			end2, _ := NewEndpoint("localhost:8888", 9, 20, generator)
			end3, _ := NewEndpoint("localhost:9999", 6, 20, generator)
			max := <-getMaxWeight([]*Endpoint{end1, end2, end3})
			So(max, ShouldEqual, 9)

		})

	})
}

func TestAllLoadBalance(t *testing.T) {
	Convey("Test All load balance", t, func() {
		// creating struct
		end1, _ := NewEndpoint("localhost:8080", 1, 20, generator)
		end2, _ := NewEndpoint("localhost:8888", 1, 20, generator)
		end3, _ := NewEndpoint("localhost:9999", 3, 20, generator)
		res := NewLoadBalance([]*Endpoint{end1, end2, end3})
		So(res, ShouldNotBeNil)

		Convey("test creating load balance", func() {
			So(res.cw, ShouldEqual, 0)
			So(len(res.endpoints), ShouldEqual, 3)
			So(res.lastSelected, ShouldEqual, -1)
			So(res.gcd, ShouldEqual, 1)
		})

		Convey("test get then reset", func() {
			// 9999, 9999, 8080, 8888, 9999
			// first get
			e, err := res.Get()
			So(err, ShouldBeNil)
			So(e.getURL(), ShouldEqual, "localhost:9999")

			// second get
			e, err = res.Get()
			So(err, ShouldBeNil)
			So(e.getURL(), ShouldEqual, "localhost:9999")

			// third get
			e, err = res.Get()
			So(err, ShouldBeNil)
			So(e.getURL(), ShouldEqual, "localhost:8080")

			// fourth get
			e, err = res.Get()
			So(err, ShouldBeNil)
			So(e.getURL(), ShouldEqual, "localhost:8888")

			// fifth get
			e, err = res.Get()
			So(err, ShouldBeNil)
			So(e.getURL(), ShouldEqual, "localhost:9999")

			// validate element parameter
			So(res.gcd, ShouldEqual, 1)
			So(res.maxWeight, ShouldEqual, 3)
			So(res.cw, ShouldEqual, 1)
			So(res.lastSelected, ShouldEqual, 2)

			// reset it
			res.reset()
			So(res.gcd, ShouldEqual, 1)
			So(res.cw, ShouldEqual, 0)
			So(res.lastSelected, ShouldEqual, -1)
			So(res.maxWeight, ShouldEqual, 3)
		})

		Convey("test get with multiple goroutine", func() {
			chanEndpoints := make(chan *tempEndpoint, 3)
			chanError := make(chan error)

			defer func() {
				close(chanEndpoints)
				close(chanError)
			}()
			for i := 0; i < 3; i++ {
				go func(iteration int) {
					e, err := res.Get()
					if err != nil {
						chanError <- err
						return
					}

					tmp := &tempEndpoint{
						endpoint:  e,
						iteration: iteration,
					}
					chanEndpoints <- tmp
				}(i)
				time.Sleep(1 * time.Millisecond)
			}

			// store temp result
			tmpResults := []string{"localhost:9999", "localhost:9999", "localhost:8080"}
			var succeed, fail int
			for i := 0; i < 3; i++ {
				fmt.Println("Waiting ")
				select {
				case e := <-chanEndpoints:
					So(e.endpoint.getURL(), ShouldEqual, tmpResults[e.iteration])
					succeed++
				case <-chanError:
					fail++
				}
			}

			So(succeed, ShouldEqual, 3)
			So(fail, ShouldEqual, 0)

			// validate parameter
			So(res.gcd, ShouldEqual, 1)
			So(res.maxWeight, ShouldEqual, 3)
			So(res.cw, ShouldEqual, 1)
			So(res.lastSelected, ShouldEqual, 0)

			// reset it
			res.reset()
			So(res.gcd, ShouldEqual, 1)
			So(res.cw, ShouldEqual, 0)
			So(res.lastSelected, ShouldEqual, -1)
			So(res.maxWeight, ShouldEqual, 3)

		})

		Convey("test get then add", func() {
			e, err := res.Get()
			So(err, ShouldBeNil)
			So(e.getURL(), ShouldEqual, "localhost:9999")

			// validate parameter
			So(res.gcd, ShouldEqual, 1)
			So(res.maxWeight, ShouldEqual, 3)
			So(res.cw, ShouldEqual, 3)
			So(res.lastSelected, ShouldEqual, 2)
		})
	})
}
