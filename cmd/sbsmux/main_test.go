package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	_ "embed"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/nettest"
)

func TestDialler(t *testing.T) {

	var wg sync.WaitGroup

	// prep context
	ctx, cancel := context.WithCancel(context.Background())

	// prep test socket
	nl, err := nettest.NewLocalListener("tcp")
	require.NoError(t, err, "error preparing test listener")
	defer nl.Close()

	// handle test connections
	wg.Add(1)
	go func(t *testing.T) {
		defer wg.Done()
		b := make([]byte, 1024)

		// accept a connection
		c, err := nl.Accept()
		require.NoError(t, err, "error accepting test connection")
		defer c.Close()

		// read until connection closed
		for {
			_, err := c.Read(b)
			if err != nil {
				return
			}
		}
	}(t)

	// counter
	i := 1

	// define bogus handler func for testing
	handler := func(ctx context.Context, conn net.Conn, ioc *iochans) error {
		// close context allowing dialler to finish up
		t.Logf("handler called #%d, closing connection", i)
		err := conn.Close()
		require.NoError(t, err)

		i += 1
		// connect 5 times
		if i > 5 {
			t.Log("cancelling context")
			cancel()
		}
		return nil
	}

	// prep iochans
	ioc := iochans{}

	// set reconnectTimeout to something sensible for testing
	reconnectDelay = time.Millisecond

	// call dialler
	dialler(ctx, nl.Addr().(*net.TCPAddr), handler, &ioc)

	// wait for everything to finish
	wg.Wait()

}

func TestDialler_connection_error(t *testing.T) {

	var wg sync.WaitGroup

	// prep context
	ctx, cancel := context.WithCancel(context.Background())

	// prep test socket
	nl, err := nettest.NewLocalListener("tcp")
	require.NoError(t, err, "error preparing test listener")
	defer nl.Close()

	// handle test connections
	wg.Add(1)
	go func(t *testing.T) {
		defer wg.Done()
		b := make([]byte, 1024)

		// accept a connection
		c, err := nl.Accept()
		require.NoError(t, err, "error accepting test connection")
		defer c.Close()

		// read until connection closed
		for {
			_, err := c.Read(b)
			if err != nil {
				return
			}
		}
	}(t)

	// counter
	i := 1

	// define bogus handler func for testing
	handler := func(ctx context.Context, conn net.Conn, ioc *iochans) error {
		// close context allowing dialler to finish up
		t.Logf("handler called #%d, closing connection", i)
		err := conn.Close()
		require.NoError(t, err)

		i += 1
		// connect 5 times
		if i > 5 {
			t.Log("cancelling context")
			cancel()
		}
		return errors.New("test error")
	}

	// prep iochans
	ioc := iochans{}

	// set reconnectTimeout to something sensible for testing
	reconnectDelay = time.Millisecond

	// call dialler
	dialler(ctx, nl.Addr().(*net.TCPAddr), handler, &ioc)

	// wait for everything to finish
	wg.Wait()

}

func TestDialler_Connection_Failures(t *testing.T) {

	var wg sync.WaitGroup

	// prep context
	ctx, cancel := context.WithCancel(context.Background())

	// prep test socket
	tmpnl, err := nettest.NewLocalListener("tcp")
	require.NoError(t, err, "error preparing test listener")
	tmpnl.Close()

	// set connectTimeout to something sensible for testing
	connectTimeout = time.Millisecond * 250

	// set reconnectTimeout to something sensible for testing
	reconnectDelay = time.Millisecond * 250

	// handle test connections
	wg.Add(1)
	go func(t *testing.T) {
		defer wg.Done()
		b := make([]byte, 1024)

		// wait before accepting a connection to allow for timeouts
		t.Log("waiting before accepting connections to produce connection error")
		_ = <-time.After(time.Second * 1)
		t.Log("now accepting connections")

		// prep listener
		nl, err := net.Listen("tcp", tmpnl.Addr().String())
		require.NoError(t, err, "error preparing test listener")

		// accept a connection
		c, err := nl.Accept()
		require.NoError(t, err, "error accepting test connection")
		defer c.Close()

		// read until connection closed
		for {
			_, err := c.Read(b)
			if err != nil {
				return
			}
		}
	}(t)

	// counter
	i := 1

	// define bogus handler func for testing
	handler := func(ctx context.Context, conn net.Conn, ioc *iochans) error {
		// close context allowing dialler to finish up
		t.Logf("handler called #%d, closing connection", i)
		err := conn.Close()
		require.NoError(t, err)

		i += 1
		// connect 5 times
		if i > 5 {
			t.Log("cancelling context")
			cancel()
		}
		return nil
	}

	// prep iochans
	ioc := iochans{}

	// call dialler
	dialler(ctx, tmpnl.Addr().(*net.TCPAddr), handler, &ioc)

	// wait for everything to finish
	wg.Wait()

}

func TestDialler_context_cancel_before_connect(t *testing.T) {

	// prep context
	ctx, cancel := context.WithCancel(context.Background())

	// prep test socket
	nl, err := nettest.NewLocalListener("tcp")
	require.NoError(t, err, "error preparing test listener")
	defer nl.Close()

	// define bogus handler func for testing
	handler := func(ctx context.Context, conn net.Conn, ioc *iochans) error {
		// close context allowing dialler to finish up
		t.Log("handler called")
		return nil
	}

	ioc := iochans{}

	cancel()

	dialler(ctx, nl.Addr().(*net.TCPAddr), handler, &ioc)

}

func TestListener(t *testing.T) {

	var wg sync.WaitGroup

	// prep context
	ctx, cancel := context.WithCancel(context.Background())

	// get tcp address for testing
	tmpnl, err := nettest.NewLocalListener("tcp")
	require.NoError(t, err, "error preparing test listener")
	tmpnl.Close()
	testAddr := tmpnl.Addr().String()

	// define bogus handler func for testing
	i := 1
	handler := func(ctx context.Context, conn net.Conn, ioc *iochans) error {
		// close context allowing dialler to finish up
		t.Logf("handler called #%d, closing connection", i)
		err := conn.Close()
		require.NoError(t, err)

		i += 1
		// connect 5 times
		if i > 5 {
			t.Log("cancelling context")
			cancel()
		}
		return nil
	}

	ioc := iochans{}

	// start listener
	wg.Add(1)
	go func(t *testing.T) {
		defer wg.Done()
		err = listener(ctx, testAddr, handler, &ioc)
		require.NoError(t, err)
	}(t)

	// start diallers
	wg.Add(1)
	go func() {
		defer wg.Done()
		b := make([]byte, 1024)
		for {
			select {

			// quit dial loop of context closed
			case <-ctx.Done():
				return

			// dial the listener
			default:
				c, err := net.Dial("tcp", testAddr)
				if err != nil {
					continue
				}
				t.Log("dial ok")
				c.Read(b) // wait for handler to close the connection
			}
		}
	}()

	wg.Wait()

}

func TestListener_connection_error(t *testing.T) {

	var wg sync.WaitGroup

	// prep context
	ctx, cancel := context.WithCancel(context.Background())

	// get tcp address for testing
	tmpnl, err := nettest.NewLocalListener("tcp")
	require.NoError(t, err, "error preparing test listener")
	tmpnl.Close()
	testAddr := tmpnl.Addr().String()

	// define bogus handler func for testing
	i := 1
	handler := func(ctx context.Context, conn net.Conn, ioc *iochans) error {
		// close context allowing dialler to finish up
		t.Logf("handler called #%d, closing connection", i)
		err := conn.Close()
		require.NoError(t, err)

		i += 1
		// connect 5 times
		if i > 5 {
			t.Log("cancelling context")
			cancel()
		}
		return errors.New("test error")
	}

	ioc := iochans{}

	// start listener
	wg.Add(1)
	go func(t *testing.T) {
		defer wg.Done()
		err = listener(ctx, testAddr, handler, &ioc)
		require.NoError(t, err)
	}(t)

	// start diallers
	wg.Add(1)
	go func() {
		defer wg.Done()
		b := make([]byte, 1024)
		for {
			select {

			// quit dial loop of context closed
			case <-ctx.Done():
				return

			// dial the listener
			default:
				c, err := net.Dial("tcp", testAddr)
				if err != nil {
					continue
				}
				t.Log("dial ok")
				c.Read(b) // wait for handler to close the connection
			}
		}
	}()

	wg.Wait()

}

func TestListener_invalid_listen_address(t *testing.T) {

	var wg sync.WaitGroup

	// prep context
	ctx, cancel := context.WithCancel(context.Background())

	// define bogus handler func for testing
	handler := func(ctx context.Context, conn net.Conn, ioc *iochans) error {
		// close context allowing dialler to finish up
		t.Logf("handler called, closing connection")
		err := conn.Close()
		require.NoError(t, err)
		t.Log("cancelling context")
		cancel()
		return nil
	}

	ioc := iochans{}

	// start listener
	wg.Add(1)
	go func(t *testing.T) {
		defer wg.Done()
		err := listener(ctx, "127.0.0.1", handler, &ioc)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing port")
	}(t)

	wg.Wait()

}

func TestListener_address_in_use(t *testing.T) {

	var wg sync.WaitGroup

	// prep context
	ctx, cancel := context.WithCancel(context.Background())

	// prep test listener
	nl, err := nettest.NewLocalListener("tcp")
	require.NoError(t, err)
	defer nl.Close()

	// define bogus handler func for testing
	handler := func(ctx context.Context, conn net.Conn, ioc *iochans) error {
		// close context allowing dialler to finish up
		t.Logf("handler called, closing connection")
		err := conn.Close()
		require.NoError(t, err)
		t.Log("cancelling context")
		cancel()
		return nil
	}

	ioc := iochans{}

	// start listener
	wg.Add(1)
	go func(t *testing.T) {
		defer wg.Done()
		err := listener(ctx, nl.Addr().String(), handler, &ioc)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "address already in use")
	}(t)

	wg.Wait()

}

func TestOutputHandler(t *testing.T) {

	var wg sync.WaitGroup

	// prep context
	ctx, cancel := context.WithCancel(context.Background())

	testData := []byte("hello world")

	// prep network interfaces
	connA, connB := net.Pipe()
	defer connA.Close()
	defer connB.Close()

	// set up output chan
	ioc := iochans{}
	ioc.chans = make(map[string]*iochan)

	// start handler
	wg.Add(1)
	go func(t *testing.T) {
		defer wg.Done()
		t.Log("starting outputHandler")
		defer t.Log("outputHandler stopped")
		err := outputHandler(ctx, connB, &ioc)
		require.NoError(t, err)
	}(t)

	// send data to chan
	wg.Add(1)
	go func() {
		defer wg.Done()
		t.Log("sending data to chan")
		defer t.Log("data sent to chan")
		for {
			ioc.mu.RLock()
			c, ok := ioc.chans[connB.RemoteAddr().String()]
			ioc.mu.RUnlock()
			if !ok {
				t.Log("waiting for chan to exist")
				time.Sleep(time.Second)
				continue
			}
			c.c <- testData
			return
		}
	}()

	// receive data
	b := make([]byte, 1024)
	n, err := connA.Read(b)
	require.NoError(t, err)

	// newline added so received len will be +1
	assert.Equal(t, len(testData)+1, n)
	assert.Equal(t, testData, b[:n-1])

	// add newline to input and ensure a match
	testData = append(testData, []byte("\n")...)
	assert.Equal(t, testData, b[:n])

	cancel()

	wg.Wait()

}

func TestOutputHandler_closed_connection(t *testing.T) {

	var wg sync.WaitGroup

	// prep context
	ctx, cancel := context.WithCancel(context.Background())

	testData := []byte("hello world")

	// prep network interfaces
	connA, connB := net.Pipe()
	defer connA.Close()
	defer connB.Close()

	// set up output chan
	ioc := iochans{}
	ioc.chans = make(map[string]*iochan)

	// start handler
	wg.Add(1)
	go func(t *testing.T) {
		defer wg.Done()
		t.Log("starting outputHandler")
		defer t.Log("outputHandler stopped")
		err := outputHandler(ctx, connB, &ioc)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "closed")
	}(t)

	// send data to chan
	wg.Add(1)
	go func() {
		defer wg.Done()
		t.Log("sending data to chan")
		defer t.Log("data sent to chan")
		for {
			ioc.mu.RLock()
			c, ok := ioc.chans[connB.RemoteAddr().String()]
			ioc.mu.RUnlock()
			if !ok {
				t.Log("waiting for chan to exist")
				time.Sleep(time.Second)
				continue
			}
			c.c <- testData
			return
		}
	}()

	// close connections
	connA.Close()
	connB.Close()

	wg.Wait()

	cancel()

}

func TestInputHandler(t *testing.T) {

	var (
		wgHandler       sync.WaitGroup
		wgSocketWrite   sync.WaitGroup
		wgChanReadReady sync.WaitGroup
		wgChanRead      sync.WaitGroup
	)

	// prep context
	ctx, cancel := context.WithCancel(context.Background())

	testData := []byte("hello world\n")

	// prep network interfaces
	connA, connB := net.Pipe()
	defer connA.Close()
	defer connB.Close()

	// set up output chan
	ioc := iochans{}
	ioc.chans = make(map[string]*iochan)

	// receive from chan
	wgChanReadReady.Add(1)
	wgChanRead.Add(1)
	go func(t *testing.T) {
		defer wgChanRead.Done()
		for {
			t.Log("checking if chan exists")
			c, ok := ioc.chans[connB.RemoteAddr().String()]
			if !ok {
				t.Log("waiting for chan to exist")
				time.Sleep(time.Second)
				continue
			}
			wgChanReadReady.Done()
			t.Log("reading from chan")
			msg, ok := <-c.c
			t.Log("read from chan finished")
			require.True(t, ok)
			msg = append(msg, []byte("\n")...)
			require.Equal(t, testData, msg)
			break
		}
	}(t)

	// start handler
	wgHandler.Add(1)
	go func(t *testing.T) {
		defer wgHandler.Done()
		t.Log("starting inputHandler")
		defer t.Log("inputHandler stopped")
		err := inputHandler(ctx, connB, &ioc)
		require.NoError(t, err)
	}(t)

	// send data to socket twice.
	// first write will be read via channel below
	// second write will be dropped
	wgSocketWrite.Add(1)
	go func(t *testing.T) {
		defer wgSocketWrite.Done()
		defer t.Log("writes to connA finished")
		wgChanReadReady.Wait()
		for i := 1; i <= 2; i++ {
			t.Log("performing write to connA")
			n, err := connA.Write(testData)
			require.NoError(t, err)
			assert.Equal(t, len(testData), n)
		}
	}(t)

	wgSocketWrite.Wait()
	wgChanRead.Wait()
	cancel()
	wgHandler.Wait()

}

func TestInputHandler_closed_connection(t *testing.T) {

	var (
		wgHandler sync.WaitGroup
	)

	// prep context
	ctx, cancel := context.WithCancel(context.Background())

	// prep network interfaces
	connA, connB := net.Pipe()
	defer connA.Close()

	// set up output chan
	ioc := iochans{}
	ioc.chans = make(map[string]*iochan)

	// start handler
	wgHandler.Add(1)
	go func(t *testing.T) {
		defer wgHandler.Done()
		t.Log("starting inputHandler")
		defer t.Log("inputHandler stopped")
		err := inputHandler(ctx, connB, &ioc)
		require.Error(t, err)
	}(t)

	time.Sleep(time.Second)

	connB.Close()
	wgHandler.Wait()
	cancel()

}

func TestWorker(t *testing.T) {

	// prep context
	ctx, cancel := context.WithCancel(context.Background())

	// set statsInterval to something appropriate for testing
	statsInterval = time.Microsecond * 15

	// prep sbsIn
	sbsIn := iochans{
		chans: make(map[string]*iochan)}
	sbsIn.chans["inputA"] = &iochan{
		c: make(chan []byte, 10)}
	sbsIn.chans["inputB"] = &iochan{
		c: make(chan []byte, 10)}
	sbsIn.chans["inputC"] = &iochan{
		c: make(chan []byte, 10)}
	sbsIn.chans["inputD"] = &iochan{
		c: make(chan []byte, 10)}

	// populate sbsIn, 10 messages queued
	for i := 1; i <= 10; i++ {
		sbsIn.chans["inputA"].c <- []byte("a")
	}
	for i := 1; i <= 10; i++ {
		sbsIn.chans["inputB"].c <- []byte("b")
	}
	for i := 1; i <= 10; i++ {
		sbsIn.chans["inputC"].c <- []byte("c")
	}
	for i := 1; i <= 10; i++ {
		sbsIn.chans["inputD"].c <- []byte("d")
	}

	sbsOut := iochans{
		chans: make(map[string]*iochan),
	}
	sbsOut.chans["outputA"] = &iochan{
		c: make(chan []byte, 40)}
	sbsOut.chans["outputB"] = &iochan{
		c: make(chan []byte, 40)}
	sbsOut.chans["outputC"] = &iochan{
		c: make(chan []byte, 40)}
	sbsOut.chans["outputD"] = &iochan{
		c: make(chan []byte, 40)}

	go func() {
		worker(ctx, &sbsIn, &sbsOut, 1)
	}()

	// wait for worker to process queues
	time.Sleep(time.Second * 1)

	assert.Equal(t, 40, len(sbsOut.chans["outputA"].c))
	assert.Equal(t, 40, len(sbsOut.chans["outputB"].c))
	assert.Equal(t, 40, len(sbsOut.chans["outputC"].c))
	assert.Equal(t, 40, len(sbsOut.chans["outputD"].c))

	aCount := 0
	bCount := 0
	cCount := 0
	dCount := 0

	for i := 1; i <= 40; i++ {
		x := <-sbsOut.chans["outputA"].c
		switch {
		case bytes.Equal(x, []byte("a")):
			aCount += 1
		case bytes.Equal(x, []byte("b")):
			bCount += 1
		case bytes.Equal(x, []byte("c")):
			cCount += 1
		case bytes.Equal(x, []byte("d")):
			dCount += 1
		}
	}

	assert.Equal(t, 10, aCount)
	assert.Equal(t, 10, bCount)
	assert.Equal(t, 10, cCount)
	assert.Equal(t, 10, dCount)

	aCount = 0
	bCount = 0
	cCount = 0
	dCount = 0

	for i := 1; i <= 40; i++ {
		x := <-sbsOut.chans["outputB"].c
		switch {
		case bytes.Equal(x, []byte("a")):
			aCount += 1
		case bytes.Equal(x, []byte("b")):
			bCount += 1
		case bytes.Equal(x, []byte("c")):
			cCount += 1
		case bytes.Equal(x, []byte("d")):
			dCount += 1
		}
	}

	assert.Equal(t, 10, aCount)
	assert.Equal(t, 10, bCount)
	assert.Equal(t, 10, cCount)
	assert.Equal(t, 10, dCount)

	aCount = 0
	bCount = 0
	cCount = 0
	dCount = 0

	for i := 1; i <= 40; i++ {
		x := <-sbsOut.chans["outputC"].c
		switch {
		case bytes.Equal(x, []byte("a")):
			aCount += 1
		case bytes.Equal(x, []byte("b")):
			bCount += 1
		case bytes.Equal(x, []byte("c")):
			cCount += 1
		case bytes.Equal(x, []byte("d")):
			dCount += 1
		}
	}

	assert.Equal(t, 10, aCount)
	assert.Equal(t, 10, bCount)
	assert.Equal(t, 10, cCount)
	assert.Equal(t, 10, dCount)

	aCount = 0
	bCount = 0
	cCount = 0
	dCount = 0

	for i := 1; i <= 40; i++ {
		x := <-sbsOut.chans["outputD"].c
		switch {
		case bytes.Equal(x, []byte("a")):
			aCount += 1
		case bytes.Equal(x, []byte("b")):
			bCount += 1
		case bytes.Equal(x, []byte("c")):
			cCount += 1
		case bytes.Equal(x, []byte("d")):
			dCount += 1
		}
	}

	assert.Equal(t, 10, aCount)
	assert.Equal(t, 10, bCount)
	assert.Equal(t, 10, cCount)
	assert.Equal(t, 10, dCount)

	cancel()

}

func TestWorker_drop_message(t *testing.T) {

	// prep context
	ctx, cancel := context.WithCancel(context.Background())

	// set statsInterval to something appropriate for testing
	statsInterval = time.Microsecond * 15

	// prep sbsIn
	sbsIn := iochans{
		chans: make(map[string]*iochan)}
	sbsIn.chans["inputA"] = &iochan{
		c: make(chan []byte, 10)}
	sbsIn.chans["inputB"] = &iochan{
		c: make(chan []byte, 10)}
	sbsIn.chans["inputC"] = &iochan{
		c: make(chan []byte, 10)}
	sbsIn.chans["inputD"] = &iochan{
		c: make(chan []byte, 10)}

	// populate sbsIn, 10 messages queued
	for i := 1; i <= 10; i++ {
		sbsIn.chans["inputA"].c <- []byte("a")
	}
	for i := 1; i <= 10; i++ {
		sbsIn.chans["inputB"].c <- []byte("b")
	}
	for i := 1; i <= 10; i++ {
		sbsIn.chans["inputC"].c <- []byte("c")
	}
	for i := 1; i <= 10; i++ {
		sbsIn.chans["inputD"].c <- []byte("d")
	}

	sbsOut := iochans{
		chans: make(map[string]*iochan),
	}
	sbsOut.chans["outputA"] = &iochan{
		c: make(chan []byte, 1)}

	go func() {
		worker(ctx, &sbsIn, &sbsOut, 1)
	}()

	// wait for worker to process queues
	time.Sleep(time.Second * 1)

	assert.Equal(t, uint64(39), sbsOut.chans["outputA"].msgsDropped)

	cancel()

}

func TestHumanReadableUint64(t *testing.T) {
	assert.Equal(t, "1", humanReadableUint64(1))
	assert.Equal(t, "1.95K", humanReadableUint64(2000))
	assert.Equal(t, "1.91M", humanReadableUint64(2000000))
	assert.Equal(t, "1.86G", humanReadableUint64(2000000000))
	assert.Equal(t, "1.82T", humanReadableUint64(2000000000000))
}

func TestApp(t *testing.T) {

	var wg sync.WaitGroup

	// get listen addresses
	inputlisten, err := nettest.NewLocalListener("tcp")
	require.NoError(t, err)
	inputlisten.Close()

	os.Setenv("SBSMUX_INPUTLISTEN", inputlisten.Addr().String())

	outputlisten, err := nettest.NewLocalListener("tcp")
	require.NoError(t, err)
	outputlisten.Close()

	os.Setenv("SBSMUX_OUTPUTLISTEN", outputlisten.Addr().String())

	// start app
	wg.Add(1)
	go func(t *testing.T) {
		defer wg.Done()
		err = app.Run([]string{})
		assert.NoError(t, err)
	}(t)

	time.Sleep(time.Second * 2)

	// prep network connections
	connIn, err := net.Dial("tcp", inputlisten.Addr().String())
	require.NoError(t, err)
	defer connIn.Close()

	connOut, err := net.Dial("tcp", outputlisten.Addr().String())
	require.NoError(t, err)
	defer connOut.Close()

	buf := make([]byte, 1024)

	// WHY IS THIS LOOP SO SLOW???!!!!
	f, err := os.Open("./test_data/readsb.sbs.output")
	require.NoError(t, err)
	s := bufio.NewScanner(f)
	i := 0
	for s.Scan() {

		i++
		if i >= 10 {
			break
		}

		line := s.Bytes()

		n, err := connIn.Write(line)
		require.NoError(t, err)
		assert.Len(t, line, n)

		n, err = connOut.Read(buf)
		require.NoError(t, err)
		assert.Equal(t, len(line), n-1)
		assert.Equal(t, line, buf[:n-1])

		fmt.Print(".")
	}

	fmt.Println("done")

	// shutdown
	sigChan <- syscall.SIGTERM

	wg.Wait()

}
