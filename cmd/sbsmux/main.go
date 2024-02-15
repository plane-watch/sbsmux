package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v2"
)

type outputs struct {
	mu          sync.RWMutex
	outputChans map[string]*outputPerClient
}

func (c *outputs) Lock() {
	c.mu.Lock()
}

func (c *outputs) Unlock() {
	c.mu.Unlock()
}

func (c *outputs) RLock() {
	c.mu.RLock()
}

func (c *outputs) RUnlock() {
	c.mu.RUnlock()
}

type outputPerClient struct {
	mu          sync.RWMutex
	outChan     chan []byte
	msgsDropped uint64
}

func (c *outputPerClient) Lock() {
	c.mu.Lock()
}

func (c *outputPerClient) Unlock() {
	c.mu.Unlock()
}

func (c *outputPerClient) RLock() {
	c.mu.RLock()
}

func (c *outputPerClient) RUnlock() {
	c.mu.RUnlock()
}

var (

	// command line arguments
	app = cli.App{
		Version:     "0.0.1",
		Name:        "plane.watch sbsmux",
		Usage:       "Multiplexer for SBS data",
		Description: `Receives SBS data via connect out or connect in. Sends SBS data via connect out or connect in.`,
		Flags: []cli.Flag{
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
				Usage:    "Buffer size for all incoming messages (number of messages)",
				Value:    1000,
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
				Usage:    "Buffer size per-client for outgoing messages (number of messages)",
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
	bufSizeIncomingMsgs          int
	bufSizeOutgoingMsgsPerClient int
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
	bufSizeIncomingMsgs = cliCtx.Int("bufsizein")
	bufSizeOutgoingMsgsPerClient = cliCtx.Int("bufsizeout")

	// prep input channel
	sbsIn := make(chan []byte, bufSizeIncomingMsgs)

	// prep output struct
	sbsOut := outputs{
		outputChans: make(map[string]*outputPerClient),
	}

	// start incoming data handler
	wg.Add(1)
	go func() {
		defer wg.Done()
		msgRouter(sbsIn, &sbsOut)
	}()

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
		inputListener(lInAddr, sbsIn)
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
				inputDialler(addr, sbsIn)
			}()
		}
	}

	// wait for goroutines
	wg.Wait()

	return nil
}

func outputDialler(addr *net.TCPAddr, sbsOut *outputs) {

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

func outputListener(addr string, sbsOut *outputs) {

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

func outputHandler(conn *net.TCPConn, sbsOut *outputs) {
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

	// prepare writer
	wtr := bufio.NewWriter(conn)

	// create channel
	wg.Add(1)
	go func() {
		defer wg.Done()
		sbsOut.Lock()
		defer sbsOut.Unlock()
		sbsOut.outputChans[clientId] = &outputPerClient{
			outChan: make(chan []byte, bufSizeOutgoingMsgsPerClient),
		}
	}()
	wg.Wait()

	// init update time variable
	lastUpdateTime := time.Now()

	c := sbsOut.outputChans[clientId]

	// loop forever
	for {

		// read from channel & write to client
		msg := <-c.outChan
		msg = append(msg, []byte("\n")...)
		n, err := wtr.Write(msg)
		if err != nil {
			log.Err(err).Msg("write error")
			break
		}
		bytesWritten += uint64(n)
		msgsOut += 1

		// update log if its been `statsInterval` since last update
		if time.Since(lastUpdateTime) > statsInterval {
			c.RLock()
			log.Info().
				Str("bytesTx", humanReadableUint64(bytesWritten)).
				Str("msgsOut", humanReadableUint64(msgsOut)).
				Str("msgsDropped", humanReadableUint64(c.msgsDropped)).
				Int("msgsPending", len(c.outChan)).
				Msg("output statistics")
			sbsOut.outputChans[clientId].RUnlock()

			lastUpdateTime = time.Now()
		}
	}
	log.Info().Msg("closing connection")
	sbsOut.Lock()
	delete(sbsOut.outputChans, clientId)
	sbsOut.Unlock()
}

func msgRouter(sbsIn chan []byte, sbsOut *outputs) {

	var msgsRouted uint64

	// init update time variable
	lastUpdateTime := time.Now()

	for {
		// receive msg from chan
		msg := <-sbsIn

		// for each output
		sbsOut.RLock()
		for _, cli := range sbsOut.outputChans {
			// send to chan non blocking
			select {
			case cli.outChan <- msg:
				// msg was delivered
				msgsRouted += 1
			default:
				// drop msg
				cli.Lock()
				cli.msgsDropped += 1
				cli.Unlock()
			}
		}
		sbsOut.RUnlock()

		// update log if its been `statsInterval` since last update
		if time.Since(lastUpdateTime) > statsInterval {
			log.Info().
				Str("msgsRouted", humanReadableUint64(msgsRouted)).
				Int("msgsPending", len(sbsIn)).
				Msg("router statistics")
			lastUpdateTime = time.Now()
		}
	}
}

func inputDialler(addr *net.TCPAddr, sbsIn chan []byte) {

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

func inputListener(addr string, sbsIn chan []byte) {

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

func inputHandler(conn *net.TCPConn, sbsIn chan []byte) {
	// handleInputConnect: connect to `addr`, read lines, send lines to chan `out`.

	defer conn.Close()

	// update log context
	log := log.With().
		Str("laddr", conn.LocalAddr().String()).
		Str("raddr", conn.RemoteAddr().String()).
		Str("direction", "in").
		Logger()

	log.Info().Msg("connection established")

	var (
		err error
		// stats counters
		bytesRead                    uint64
		msgsIn, msgsOut, msgsDropped uint64
	)

	// init update time variable
	lastUpdateTime := time.Now()

	// prepare reader
	rdr := bufio.NewReader(conn)

	// read each line (SBS is a line-by-line protocol)
	var buf []byte
	var isPrefix bool
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
			msgsIn += 1
			select {
			case sbsIn <- buf:
				msgsOut += 1
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
				Str("msgsOut", humanReadableUint64(msgsOut)).
				Str("msgsDropped", humanReadableUint64(msgsDropped)).
				Msg("input statistics")
			lastUpdateTime = time.Now()
		}
	}

	// let user know of issue
	log.Info().Msg("disconnected from remote host")

	// wait for reconnectTimeout before looping (retrying)
	time.Sleep(reconnectTimeout)
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
