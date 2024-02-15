package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"reflect"
	"runtime"
	"sync"
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
		Version:     "0.0.1",
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
			},
			&cli.IntFlag{
				Category: "SBS In",
				Name:     "bufsizeout",
				Usage:    "Buffer size per-host for outgoing messages (number of messages)",
				Value:    100,
			},
			&cli.UintFlag{
				Category: "Timeouts/Durations",
				Name:     "reconnecttimeout",
				Usage:    "Delay between connection attempts (seconds)",
				Value:    uint(10),
			},
			&cli.UintFlag{
				Category: "Timeouts/Durations",
				Name:     "statsinterval",
				Usage:    "Delay between printing per-connection statistics (minutes)",
				Value:    uint(10),
			},
		},
	}

	// durations
	reconnectTimeout time.Duration
	statsInterval    time.Duration

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
	app.Action = runApp
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

	log.Info().Str("version", cliCtx.App.Version).Msg("sbsmux starting")

	// timeouts / durations
	reconnectTimeout = time.Second * time.Duration(cliCtx.Uint64("reconnecttimeout"))
	statsInterval = time.Minute * time.Duration(cliCtx.Uint64("statsinterval"))

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

	// start msgRouter workers
	for i := 1; i <= cliCtx.Int("numworkers"); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msgRouter(&sbsIn, &sbsOut, i)
		}()
	}

	// start output listener
	lOutAddr := cliCtx.String("outputlisten")
	wg.Add(1)
	go func() {
		defer wg.Done()
		outputListener(lOutAddr, &sbsOut)
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
				outputDialler(addr, &sbsOut)
			}()
		}
	}

	// start input listener
	lInAddr := cliCtx.String("inputlisten")
	wg.Add(1)
	go func() {
		defer wg.Done()
		inputListener(lInAddr, &sbsIn)
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
				inputDialler(addr, &sbsIn)
			}()
		}
	}

	// wait for goroutines
	wg.Wait()

	return nil
}

func outputDialler(addr *net.TCPAddr, sbsOut *iochans) {

	// update log context
	log := log.With().
		Str("raddr", addr.String()).
		Str("direction", "out").
		Logger()

	// dial output connection & hand off to outputHandler
	for {
		conn, err := net.DialTCP("tcp", nil, addr)
		if err != nil {
			log.Err(err).Msg("could not connect")
			time.Sleep(reconnectTimeout)
			continue
		}
		outputHandler(conn, sbsOut)
		time.Sleep(reconnectTimeout)
	}
}

func outputListener(addr string, sbsOut *iochans) {

	// update log context
	log := log.With().
		Str("laddr", addr).
		Str("direction", "out").
		Logger()

	// resolve listen address
	laddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		log.Fatal().AnErr("err", err).Msg("invalid TCP listen address")
		return
	}
	log = log.With().Str("laddr", laddr.String()).Logger()

	// listen for connections
	l, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		log.Fatal().AnErr("err", err).Msg("error listening")
		return
	}
	log.Info().Msg("listening for connections")

	// accept connections and hand off to outputHandler
	for {
		conn, err := l.AcceptTCP()
		if err != nil {
			log.Err(err).Msg("error accepting connection")
		} else {
			go outputHandler(conn, sbsOut)
		}
	}
}

func outputHandler(conn *net.TCPConn, sbsOut *iochans) {
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

	// create channel
	wg.Add(1)
	go func() {
		defer wg.Done()
		sbsOut.mu.Lock()
		defer sbsOut.mu.Unlock()
		sbsOut.chans[clientId] = &iochan{
			c: make(chan []byte, bufSizeOutgoingMsgsPerHost),
		}
	}()
	wg.Wait()

	// init update time variable
	lastUpdateTime := time.Now()

	c := sbsOut.chans[clientId]

	// loop forever
	for {

		// read from channel & write to client
		msg := <-c.c
		msg = append(msg, []byte("\n")...)
		n, err := conn.Write(msg)
		if err != nil {
			log.Err(err).Msg("write error")
			break
		}
		bytesWritten += uint64(n)
		msgsOut += 1

		// update log if its been `statsInterval` since last update
		if time.Since(lastUpdateTime) > statsInterval {
			c.mu.RLock()
			log.Info().
				Str("bytesTx", humanReadableUint64(bytesWritten)).
				Str("msgsOut", humanReadableUint64(msgsOut)).
				Str("msgsDropped", humanReadableUint64(c.msgsDropped)).
				Int("msgsPending", len(c.c)).
				Msg("output statistics")
			sbsOut.chans[clientId].mu.RUnlock()

			lastUpdateTime = time.Now()
		}
	}
	log.Info().Msg("closing connection")
	sbsOut.mu.Lock()
	delete(sbsOut.chans, clientId)
	sbsOut.mu.Unlock()
}

func msgRouter(sbsIn *iochans, sbsOut *iochans, workerNum int) {

	var msgsRouted uint64

	log := log.With().Int("worker#", workerNum).Logger()

	// init update time variable
	lastUpdateTime := time.Now()

	for {

		// read lock while selecting a channel to read from
		sbsIn.mu.RLock()

		// bail out if no chans to read from
		if len(sbsIn.chans) <= 0 {
			sbsIn.mu.RUnlock()
			time.Sleep(time.Millisecond * 25) // backoff to prevent killing cpu
			continue
		}

		// reflect dynamic select magic
		// see: https://stackoverflow.com/questions/19992334/how-to-listen-to-n-channels-dynamic-select-statement
		cases := make([]reflect.SelectCase, len(sbsIn.chans))
		i := 0
		for _, c := range sbsIn.chans {
			cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(c.c)}
			i += 1
		}
		_, value, ok := reflect.Select(cases)

		// finished reading from input channels
		sbsIn.mu.RUnlock()

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

		// update log if its been `statsInterval` since last update
		if time.Since(lastUpdateTime) > statsInterval {
			log.Info().
				Str("msgsRouted", humanReadableUint64(msgsRouted)).
				Msg("router statistics")
			lastUpdateTime = time.Now()
		}
	}
}

func inputDialler(addr *net.TCPAddr, sbsIn *iochans) {

	// update log context
	log := log.With().
		Str("raddr", addr.String()).
		Str("direction", "in").
		Logger()

	// dial input connection & hand off to inputHandler
	for {
		conn, err := net.DialTCP("tcp", nil, addr)
		if err != nil {
			log.Err(err).Msg("could not connect")
			time.Sleep(reconnectTimeout)
			continue
		}
		inputHandler(conn, sbsIn)
		time.Sleep(reconnectTimeout)
	}
}

func inputListener(addr string, sbsIn *iochans) {

	// update log context
	log := log.With().
		Str("laddr", addr).
		Str("direction", "in").
		Logger()

	// resolve listen address
	laddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		log.Fatal().AnErr("err", err).Msg("invalid TCP listen address")
		return
	}
	log = log.With().Str("laddr", laddr.String()).Logger()

	// listen for connections
	l, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		log.Fatal().AnErr("err", err).Msg("error listening")
		return
	}
	log.Info().Msg("listening for connections")

	// accept connections and hand off to outputHandler
	for {
		conn, err := l.AcceptTCP()
		if err != nil {
			log.Err(err).Msg("error accepting connection")
		} else {
			go inputHandler(conn, sbsIn)
		}
	}
}

func inputHandler(conn *net.TCPConn, sbsIn *iochans) {
	// handleInputConnect: connect to `addr`, read lines, send lines to chan `out`.

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

	// prepare reader
	rdr := bufio.NewReader(conn)

	// create channel
	wg.Add(1)
	go func() {
		defer wg.Done()
		sbsIn.mu.Lock()
		defer sbsIn.mu.Unlock()
		sbsIn.chans[clientId] = &iochan{
			c: make(chan []byte, bufSizeOutgoingMsgsPerHost),
		}
	}()
	wg.Wait()

	// init update time variable
	lastUpdateTime := time.Now()

	c := sbsIn.chans[clientId]

	// read each line (SBS is a line-by-line protocol)
	var buf []byte
	var isPrefix bool
	var err error
	for {
		var tmp []byte

		// read from client
		tmp, isPrefix, err = rdr.ReadLine()
		if err != nil {
			log.Err(err).Msg("read error")
			break
		}
		bytesRead += uint64(len(tmp))

		// append data to buffer
		buf = append(buf[:], tmp[:]...)

		// if end of data, then add data to incoming chan
		if !isPrefix {

			// send data non blocking
			select {
			case c.c <- buf:
				msgsIn += 1
			default:
				msgsDropped += 1
			}

			// flush buffer
			buf = []byte{}
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
	}
	log.Info().Msg("closing connection")
	sbsIn.mu.Lock()
	delete(sbsIn.chans, clientId)
	sbsIn.mu.Unlock()
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
