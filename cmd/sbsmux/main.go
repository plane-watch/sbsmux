package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v2"
)

// iochans for all remote hosts of a given type (input / output)
type iochans struct {
	mu    sync.RWMutex
	chans map[string]*iochan // string key is remote address as string
}

// iochan for each remote host
type iochan struct {
	mu          sync.RWMutex
	c           chan []byte // msg buffer
	msgsDropped uint64
}

var (

	// command line arguments
	app = cli.App{
		Version:     "1.0.0",
		Name:        "plane.watch sbsmux",
		Usage:       "Multiplexer for SBS data",
		Description: `Receives SBS data via connect out or connect in. Sends SBS data via connect out or connect in.`,
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:  "numworkers",
				Usage: "Number of workers routing messages (defaults to number of CPUs)",
				Value: runtime.NumCPU(),
			},
			&cli.StringSliceFlag{
				Category: "SBS In",
				Name:     "inputconnect",
				Usage:    "Connect to SBS data source to retrieve data",
			},
			&cli.StringFlag{
				Category: "SBS In",
				Name:     "inputlisten",
				Usage:    "Listen on this TCP address for connections to receive data",
				Value:    ":30103",
				EnvVars:  []string{"SBSMUX_INPUTLISTEN"},
			},
			&cli.IntFlag{
				Category: "SBS In",
				Name:     "bufsizein",
				Usage:    "Buffer size per-host for incoming messages (number of messages)",
				Value:    100,
			},
			&cli.StringSliceFlag{
				Category: "SBS Out",
				Name:     "outputconnect",
				Usage:    "Connect to SBS data source to send data",
			},
			&cli.StringFlag{
				Category: "SBS Out",
				Name:     "outputlisten",
				Usage:    "Listen on this TCP address for connections to send data",
				Value:    ":30003",
				EnvVars:  []string{"SBSMUX_OUTPUTLISTEN"},
			},
			&cli.IntFlag{
				Category: "SBS Out",
				Name:     "bufsizeout",
				Usage:    "Buffer size per-host for outgoing messages (number of messages)",
				Value:    100,
			},
			&cli.UintFlag{
				Category: "Timeouts/Durations",
				Name:     "reconnectdelay",
				Usage:    "Delay between connection attempts (seconds)",
				Value:    uint(10),
			},
			&cli.UintFlag{
				Category: "Timeouts/Durations",
				Name:     "statsinterval",
				Usage:    "Delay between printing per-connection statistics (seconds)",
				Value:    uint(600),
			},
			&cli.UintFlag{
				Category: "Timeouts/Durations",
				Name:     "connecttimeout",
				Usage:    "Timeout when dialling a connection (seconds)",
				Value:    uint(10),
			},
		},
		Action: runApp,
	}

	// channel for signals
	sigChan chan os.Signal

	// durations
	connectTimeout time.Duration
	reconnectDelay time.Duration
	statsInterval  time.Duration

	// buffer sizes
	bufSizeIncomingMsgsPerHost int
	bufSizeOutgoingMsgsPerHost int
)

func init() {
	// set up logging
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.UnixDate})
}

func main() {

	// run & final exit
	err := app.Run(os.Args)
	if err != nil {
		log.Err(err).Msg("finished with error")
		os.Exit(1)
	}
}

func runApp(cliCtx *cli.Context) error {

	var (
		wg sync.WaitGroup
	)

	log.Info().Str("version", cliCtx.App.Version).Msg("sbsmux started")
	defer log.Info().Str("version", cliCtx.App.Version).Msg("sbsmux stopped")

	// context
	ctx, cancel := context.WithCancel(context.Background())

	// signals
	sigChan = make(chan os.Signal)
	defer close(sigChan)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		log := log.With().Str("component", "sighandler").Logger()
		log.Info().Msg("signal handler started")
		defer log.Info().Msg("signal handler stopped")
		for {
			select {
			case <-ctx.Done():
				return
			case s := <-sigChan:
				log.Info().Str("signal", s.String()).Msg("caught signal, performing clean shutdown")
				cancel()
				return
			}
		}
	}()

	// timeouts / durations
	reconnectDelay = time.Second * time.Duration(cliCtx.Uint64("reconnectdelay"))
	statsInterval = time.Second * time.Duration(cliCtx.Uint64("statsinterval"))
	connectTimeout = time.Second * time.Duration(cliCtx.Uint64("connecttimeout"))

	// buffer sizes
	bufSizeIncomingMsgsPerHost = cliCtx.Int("bufsizein")
	bufSizeOutgoingMsgsPerHost = cliCtx.Int("bufsizeout")

	// prep input iochans
	sbsIn := iochans{
		chans: make(map[string]*iochan),
	}

	// prep output iochans
	sbsOut := iochans{
		chans: make(map[string]*iochan),
	}

	// start workers
	for i := 1; i <= cliCtx.Int("numworkers"); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(ctx, &sbsIn, &sbsOut, i)
		}()
	}

	// start output listener
	lOutAddr := cliCtx.String("outputlisten")
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := listener(ctx, lOutAddr, outputHandler, &sbsOut)
		if err != nil {
			cancel()
		}
	}()

	// get slice of addr:ports from --outputconnect
	connOutStrs := cliCtx.StringSlice("outputconnect")

	// for each outbound connection to send data to
	for _, connOutStr := range connOutStrs {

		// check addresss
		addr, err := net.ResolveTCPAddr("tcp", connOutStr)
		if err != nil {
			log.Err(err).Msg("invalid address")
		} else {

			// if address valid then dial remote host
			wg.Add(1)
			go func() {
				defer wg.Done()
				dialler(ctx, addr, outputHandler, &sbsOut)
			}()
		}
	}

	// start input listener
	lInAddr := cliCtx.String("inputlisten")
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := listener(ctx, lInAddr, inputHandler, &sbsIn)
		if err != nil {
			cancel()
		}
	}()

	// get slice of addr:ports from --inputconnect
	connInStrs := cliCtx.StringSlice("inputconnect")

	// for each outbound connection to receive data from
	for _, connInStr := range connInStrs {

		// check addresss
		addr, err := net.ResolveTCPAddr("tcp", connInStr)
		if err != nil {
			log.Err(err).Msg("invalid address")
		} else {

			// if address valid then dial remote host
			wg.Add(1)
			go func() {
				defer wg.Done()
				dialler(ctx, addr, inputHandler, &sbsIn)
			}()
		}
	}

	// wait for goroutines
	wg.Wait()

	return nil
}

func dialler(ctx context.Context, addr *net.TCPAddr, handler func(ctx context.Context, conn net.Conn, ioc *iochans) error, handlerIOchan *iochans) {

	// update log context
	log := log.With().
		Str("raddr", addr.String()).
		Str("component", "dialler").
		Logger()

	log.Info().Msg("started dialler")
	defer log.Info().Msg("shutdown dialler")

	// dial output connection & hand off to outputHandler
	for {

		// check for context cancellation
		select {
		case <-ctx.Done():
			return
		default:
		}

		// attempt connect
		conn, err := net.DialTimeout("tcp", addr.String(), connectTimeout)
		if err != nil {
			log.Err(err).Msg("could not connect")
		} else {
			err := handler(ctx, conn.(*net.TCPConn), handlerIOchan)
			if err != nil {
				log.Err(err).Msg("connection closed with error")
			}
		}

		// wait for reconnect with check for context cancellation
		select {
		case <-ctx.Done():
			return
		case <-time.After(reconnectDelay):
			break
		}
	}
}

func listener(ctx context.Context, addr string, handler func(ctx context.Context, conn net.Conn, ioc *iochans) error, handlerIOchan *iochans) error {

	// update log context
	log := log.With().
		Str("laddr", addr).
		Str("component", "listener").
		Logger()

	log.Info().Msg("started listener")
	defer log.Info().Msg("shutdown listener")

	// resolve listen address
	laddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		log.Err(err).Msg("invalid TCP listen address")
		return err
	}
	log = log.With().Str("laddr", laddr.String()).Logger()

	// listen for connections
	l, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		log.Err(err).Msg("error listening")
		return err
	}
	log.Info().Msg("listening for connections")

	// accept connections and hand off to outputHandler
	for {

		// check for context closure
		select {
		case <-ctx.Done():
			l.Close()
			return nil
		default:
		}

		// set deadline
		err := l.SetDeadline(time.Now().Add(time.Second))
		if err != nil {
			log.Err(err).Msg("error setting deadline on listener")
			return err
		}

		// accept connection
		conn, err := l.AcceptTCP()
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				continue
			} else {
				log.Err(err).Msg("error accepting connection")
			}
		} else {
			go func() {
				err := handler(ctx, conn, handlerIOchan)
				if err != nil {
					log.Err(err).Msg("connection closed with error")
				}
			}()
		}
	}
}

func outputHandler(ctx context.Context, conn net.Conn, ioc *iochans) error {
	// connect to `addr`, receive lines from iochans, send to conn
	defer conn.Close()

	var (
		wg sync.WaitGroup

		// stats counters
		bytesWritten uint64
		msgsOut      uint64
	)

	clientId := conn.RemoteAddr().String()

	log := log.With().
		Str("direction", "out").
		Str("laddr", conn.LocalAddr().String()).
		Str("raddr", clientId).
		Logger()

	log.Info().Msg("connection established")
	defer log.Info().Msg("closing connection")

	// create channel
	wg.Add(1)
	go func() {
		defer wg.Done()
		ioc.mu.Lock()
		defer ioc.mu.Unlock()
		ioc.chans[clientId] = &iochan{
			c: make(chan []byte, bufSizeOutgoingMsgsPerHost),
		}
	}()
	wg.Wait()
	defer func() {
		ioc.mu.Lock()
		delete(ioc.chans, clientId)
		ioc.mu.Unlock()
	}()

	// init update time variable
	lastUpdateTime := time.Now()

	c := ioc.chans[clientId]

	// loop forever
	for {

		// update log if its been `statsInterval` since last update
		if time.Since(lastUpdateTime) > statsInterval {
			c.mu.RLock()
			log.Info().
				Str("bytesTx", humanReadableUint64(bytesWritten)).
				Str("msgsOut", humanReadableUint64(msgsOut)).
				Str("msgsDropped", humanReadableUint64(c.msgsDropped)).
				Int("msgsPending", len(c.c)).
				Msg("output statistics")
			ioc.chans[clientId].mu.RUnlock()

			lastUpdateTime = time.Now()
		}

		select {

		// handle context closure
		case <-ctx.Done():
			return nil

		case <-time.After(time.Second):

		// read from channel & write to client
		case msg := <-c.c:

			// sbs is line-delimited protocol
			if msg[len(msg)-1] != 0x0a {
				msg = append(msg, []byte("\n")...)
			}

			// write to client
			n, err := conn.Write(msg)
			if err != nil {
				return err
			}
			bytesWritten += uint64(n)
			msgsOut += 1
		}
	}
}

func inputHandler(ctx context.Context, conn net.Conn, ioc *iochans) error {
	// connect to `addr`, read lines from conn, send lines to iochans
	defer conn.Close()

	var (
		wg sync.WaitGroup

		// stats counters
		bytesRead           uint64
		msgsIn, msgsDropped uint64
	)

	clientId := conn.RemoteAddr().String()

	// update log context
	log := log.With().
		Str("laddr", conn.LocalAddr().String()).
		Str("raddr", clientId).
		Str("direction", "in").
		Logger()

	log.Info().Msg("connection established")
	defer log.Info().Msg("closing connection")

	// prepare reader
	rdr := bufio.NewReader(conn)

	// create channel
	wg.Add(1)
	go func() {
		defer wg.Done()
		ioc.mu.Lock()
		defer ioc.mu.Unlock()
		ioc.chans[clientId] = &iochan{
			c: make(chan []byte, bufSizeOutgoingMsgsPerHost),
		}
	}()
	wg.Wait()
	defer func() {
		ioc.mu.Lock()
		delete(ioc.chans, clientId)
		ioc.mu.Unlock()
	}()

	// init update time variable
	lastUpdateTime := time.Now()

	c := ioc.chans[clientId]

	// read each line (SBS is a line-by-line protocol)
	var buf []byte
	var isPrefix bool
	var err error
	for {
		var tmp []byte

		// handle context closure
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// update log if its been `statsInterval` since last update
		if time.Since(lastUpdateTime) > statsInterval {
			log.Info().
				Str("bytesRx", humanReadableUint64(bytesRead)).
				Str("msgsIn", humanReadableUint64(msgsIn)).
				Str("msgsDropped", humanReadableUint64(msgsDropped)).
				Int("msgsPending", len(c.c)).
				Msg("input statistics")
			lastUpdateTime = time.Now()
		}

		// set read deadline
		err = conn.SetReadDeadline(time.Now().Add(time.Second))
		if err != nil {
			log.Err(err).Msg("error setting read deadline on connection")
			return err
		}

		// read from client
		tmp, isPrefix, err = rdr.ReadLine()
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				continue
			} else {
				return err
			}
		}
		bytesRead += uint64(len(tmp))

		// append data to buffer
		buf = append(buf[:], tmp[:]...)

		// if end of data, then add data to incoming chan
		if !isPrefix {

			out := bytes.Clone(buf)

			// send data non blocking
			select {
			case c.c <- out:
				msgsIn += 1
			default:
				msgsDropped += 1
			}

			// flush buffer
			buf = []byte{}
		}
	}
}

func worker(ctx context.Context, sbsIn *iochans, sbsOut *iochans, workerNum int) {

	var msgsRouted uint64

	log := log.With().
		Int("#", workerNum).
		Str("component", "worker").
		Logger()

	log.Info().Msg("worker started")
	defer log.Info().Msg("shutdown worker")

	// init update time variable
	// lastUpdateTime := time.Now()

	for {

		// read lock while selecting a channel to read from
		sbsIn.mu.RLock()

		// reflect dynamic select magic
		// see: https://stackoverflow.com/questions/19992334/how-to-listen-to-n-channels-dynamic-select-statement

		cases := make([]reflect.SelectCase, len(sbsIn.chans)+2)

		// add context cancellation
		cases[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())}

		// add timeout to prevent deadlocks
		cases[1] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(time.After(time.Millisecond * 500))}

		i := 2
		for _, c := range sbsIn.chans {
			cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(c.c)}
			i += 1
		}
		chosen, value, ok := reflect.Select(cases)

		// finished reading from input channels
		sbsIn.mu.RUnlock()

		switch chosen {

		// if chosen channel was context cancellation, quit
		case 0:
			return

		// if chosen channel was deadlock timeout, continue
		case 1:
			continue

		// for all other scenarios:
		default:

			// if our channel read wasn't ok, then bail out
			if !ok {
				continue
			}

			// read lock on output channels while we write
			sbsOut.mu.RLock()

			// write to each output channel
			for _, cli := range sbsOut.chans {
				// send to chan non blocking
				select {
				case cli.c <- value.Bytes():
					// msg was delivered
					msgsRouted += 1
					// log.Debug().Str("cli", cn).Bytes("msg", msg).Msg("wrote msg")
				default:
					// drop msg
					cli.mu.Lock()
					cli.msgsDropped += 1
					cli.mu.Unlock()
				}
			}

			// finished writing
			sbsOut.mu.RUnlock()

			// // update log if its been `statsInterval` since last update
			// if time.Since(lastUpdateTime) > statsInterval {
			// 	log.Info().
			// 		Str("msgsRouted", humanReadableUint64(msgsRouted)).
			// 		Msg("worker statistics")
			// 	lastUpdateTime = time.Now()
			// }
		}
	}
}

func humanReadableUint64(u uint64) string {
	switch {
	case u >= 1099511627776:
		return fmt.Sprintf("%.2fT", float64(u)/1099511627776.0)
	case u >= 1073741824:
		return fmt.Sprintf("%.2fG", float64(u)/1073741824.0)
	case u >= 1048576:
		return fmt.Sprintf("%.2fM", float64(u)/1048576.0)
	case u >= 1024:
		return fmt.Sprintf("%.2fK", float64(u)/1024.0)
	default:
		return fmt.Sprintf("%d", u)
	}
}
