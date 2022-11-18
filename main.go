package main

import (
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Global transactionID, infoHash and peerID counters.
var (
	infoHashCnt      atomic.Uint64
	peerIDCnt        atomic.Uint64
	transactionIDCnt atomic.Uint32
)

// Flags.
var (
	timeout        int
	requests       int
	clients        int
	target         string
	keepAlive      bool
	scrapeMode     bool
	errorReporting bool
	seeder         bool
)

const (
	readTimeout   = time.Second * 2
	udpPacketSize = 98
)

var udpConnectHeader = []byte{0x0, 0x0, 0x4, 0x17, 0x27, 0x10, 0x19, 0x80}

func init() {
	flag.IntVar(&timeout, "t", 0, "Period of testing (in seconds)")
	flag.IntVar(&requests, "r", -1, "Number of requests per client, -1=unlimited")
	flag.IntVar(&clients, "c", 100, "Number of concurrent clients")
	flag.StringVar(&target, "u", "", "Target URL (e.g. 127.0.0.1:12345 or tracker.example.org:1337)")
	flag.BoolVar(&keepAlive, "k", false, "Re-use connection IDs")
	flag.BoolVar(&scrapeMode, "s", false, "Scrape instead of announcing")
	flag.BoolVar(&errorReporting, "e", false, "Enable detailed error reporting")
	flag.BoolVar(&seeder, "f", false, "Send seeder announces (left=0)")

	rand.Seed(time.Now().UnixNano())

	infoHashCnt.Store(rand.Uint64())
	peerIDCnt.Store(rand.Uint64())
	transactionIDCnt.Store(rand.Uint32())
}

type configuration struct {
	url        string
	requests   int
	period     time.Duration
	keepAlive  bool
	scrapeMode bool
	seeder     bool
}

type result struct {
	connectAttempts int
	failedConnects  int
	requests        int
	success         int
	failed          int
	errors          map[error]int
}

type client struct {
	conn   net.Conn
	result *result
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	c := newConfig()
	t := make(chan struct{})
	if c.period > 0 {
		go func() {
			<-time.After(c.period)
			close(t)
		}()
	}

	wg := &sync.WaitGroup{}
	results := make([]*result, clients)

	fmt.Printf("Dispatching %d clients\n", clients)
	wg.Add(clients)
	startTime := time.Now()
	for i := 0; i < clients; i++ {
		res := &result{
			errors: make(map[error]int),
		}
		results[i] = res

		conn, err := net.Dial("udp", c.url)
		if err != nil {
			panic(err)
		}

		go client{
			conn:   conn,
			result: res,
		}.do(c, t, wg)
		go func() {
			if c.period > 0 {
				<-t
				_ = conn.SetReadDeadline(time.Now())
			}
		}()
	}

	fmt.Println("Waiting for results...")
	wg.Wait()
	printResults(results, time.Since(startTime), c)
}

func newConfig() *configuration {
	if target == "" {
		flag.Usage()
		os.Exit(1)
	}

	if requests <= 0 && timeout <= 0 {
		flag.Usage()
		os.Exit(1)
	}

	if requests > 0 && timeout > 0 {
		flag.Usage()
		os.Exit(1)
	}

	conf := &configuration{
		url:        target,
		requests:   math.MaxInt64,
		period:     0,
		keepAlive:  keepAlive,
		scrapeMode: scrapeMode,
		seeder:     seeder,
	}

	if timeout > 0 {
		conf.period = time.Duration(timeout) * time.Second
	}

	if requests != -1 {
		conf.requests = requests
	}

	return conf
}

func printResults(results []*result, runTime time.Duration, c *configuration) {
	var reqCount int
	var succeeded int
	var failed int
	var connectAttempts int
	var failedConnects int
	var timeouts int
	reqErrors := make(map[error]int)

	for _, res := range results {
		reqCount += res.requests
		succeeded += res.success
		failed += res.failed
		connectAttempts += res.connectAttempts
		failedConnects += res.failedConnects
		if errorReporting {
			for err, count := range res.errors {
				reqErrors[err] += count

				if errors.Is(err, os.ErrDeadlineExceeded) {
					timeouts += count
				}
			}
		}
	}

	elapsed := runTime.Seconds()

	if elapsed == 0 {
		elapsed = 1
	}

	// compute concurrency
	workTime := float64(clients) * elapsed
	realTime := workTime - (float64(timeouts) * readTimeout.Seconds())
	concurrency := float64(clients) * (realTime / workTime)

	fmt.Println()
	fmt.Printf("Requests:                       %10d\n", reqCount)
	fmt.Printf("Successful requests:            %10d\n", succeeded)
	fmt.Printf("Failed requests:                %10d\n", failed)
	fmt.Printf("Connect attempts:               %10d\n", connectAttempts)
	fmt.Printf("Failed connects:                %10d\n", failedConnects)
	fmt.Printf("Successful requests rate:       %10.0f hits/sec\n", float64(succeeded)/elapsed)
	if c.period > 0 {
		fmt.Printf("Approximate concurrency:        %10.2f clients running\n", concurrency)
	}
	fmt.Printf("Test time:                      %10.2f sec\n", elapsed)
	if errorReporting {
		fmt.Println("Errors encountered:")
		for err, count := range reqErrors {
			fmt.Printf("    %30v  %10d\n", err, count)
		}
	}
}

func checkDeadLine(err error, t chan struct{}) (inc int) {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		select {
		case <-t:
			// timeout because we're done
			return
		default:
			inc = 1
		}
	} else {
		inc = 1
	}
	return
}

func (r *result) incrementError(err error, t chan struct{}) (inc int) {
	if inc = checkDeadLine(err, t); inc > 0 && errorReporting {
		r.errors[err]++
	}
	return
}

func (c client) do(conf *configuration, t chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	var err error

	// Prepare a receipt buffer.
	buf := make([]byte, 1024)

	// Get a transaction ID.
	trID := transactionIDCnt.Add(1)

	rg := &xoroshiro128p{peerIDCnt.Add(1), infoHashCnt.Add(1)}

	// Prepare a packet.
	packet := prepareAnnounce(conf.seeder, rg.next())

	var connectionID uint64
	var connectionIDExpired <-chan time.Time

	// Perform requests.
loop:
	for c.result.requests < conf.requests {
		// Check if time has expired.
		select {
		case <-t:
			return
		default:
		}

		// Check if we have to (re)connect.
		if conf.keepAlive {
			if connectionIDExpired == nil {
				// Initial connect or fast reconnect.
				trID = transactionIDCnt.Add(1)
				connectionID, err = c.connect(trID, t)
				if err != nil {
					c.result.incrementError(err, t)
					continue loop
				}
				connectionIDExpired = time.After(time.Minute)
			} else {
				select {
				case <-connectionIDExpired:
					// Reconnect.
					trID = transactionIDCnt.Add(1)
					connectionID, err = c.connect(trID, t)
					if err != nil {
						c.result.incrementError(err, t)
						connectionIDExpired = nil // Do a fast reconnect.
						continue loop
					}
					connectionIDExpired = time.After(time.Minute)
				default:
				}
			}
		} else {
			// Reconnect on every request.
			trID = transactionIDCnt.Add(1)
			connectionID, err = c.connect(trID, t)
			if err != nil {
				c.result.incrementError(err, t)
				continue loop
			}
		}

		err = c.announce(buf, packet,
			transactionIDCnt.Add(1),
			connectionID,
			rg.nextIH(),
			peerIDCnt.Add(1))
		c.result.requests++
		if err != nil {
			c.result.failed += c.result.incrementError(err, t)
		} else {
			c.result.success++
		}
	}
}

func prepareAnnounce(seeder bool, rn uint64) []byte {
	bb := make([]byte, udpPacketSize)
	// Action
	bb[11] = 1
	if !seeder {
		bb[71] = 1
	}

	// Event
	bb[83] = 2

	// Numwant
	bb[92], bb[95] = byte(rn>>24), byte(rn>>16)

	// Port
	bb[96], bb[97] = byte(rn>>8), byte(rn)

	return bb
}

type xoroshiro128p struct {
	s0, s1 uint64
}

func (x *xoroshiro128p) next() (result uint64) {
	s0, s1 := x.s0, x.s1
	result = s0 + s1
	s1 ^= s0
	x.s0 = ((s0 << 24) | (s0 >> 40)) ^ s1 ^ (s1 << 16) // rotl(s0, 24) ^ s1 ^ (s1 << 16)
	x.s1 = (s1 << 37) | (s1 >> 27)                     // rotl(s1, 37)
	return
}

func (x *xoroshiro128p) nextIH() []byte {
	res := make([]byte, sha1.Size)
	binary.BigEndian.PutUint64(res[:8], x.next())
	binary.BigEndian.PutUint64(res[8:16], x.next())
	binary.BigEndian.PutUint32(res[16:], uint32(x.next()))
	return res
}

var (
	errTruncatedTx           = errors.New("packet was not fully sent to server")
	errTruncatedRx           = errors.New("packet was not fully received from server")
	errUnexpectedAction      = errors.New("tracker responded with unexpected action")
	errTransactionIDMismatch = errors.New("mismatch sent and received transaction IDs")
)

func (c client) announce(buf, packet []byte, transactionID uint32, connectionID uint64, infoHash []byte, peerID uint64) error {
	_ = c.conn.SetReadDeadline(time.Now().Add(readTimeout))

	binary.BigEndian.PutUint64(packet[0:8], connectionID)
	binary.BigEndian.PutUint32(packet[12:16], transactionID)
	copy(packet[16:36], infoHash)
	copy(packet[36:56], fmt.Sprintf("%020x", peerID))

	// Send announce.
	n, err := c.conn.Write(packet)
	if err != nil {
		return err
	}
	if n != 98 {
		return errTruncatedTx
	}

	// Receive response.
	n, err = c.conn.Read(buf)
	if err != nil {
		return err
	}
	if n < 20 {
		return errTruncatedRx
	}

	// Parse action.
	action := binary.BigEndian.Uint32(buf[:4])
	if action != 1 {
		if action == 3 {
			errVal := string(buf[8:n])
			return fmt.Errorf("tracker error: %s", errVal)
		}
		return errUnexpectedAction
	}

	transID := binary.BigEndian.Uint32(buf[4:8])
	if transID != transactionID {
		return errTransactionIDMismatch
	}

	return nil
}

func (c client) connect(transactionID uint32, t chan struct{}) (u uint64, err error) {
	_ = c.conn.SetReadDeadline(time.Now().Add(readTimeout))

	c.result.connectAttempts++
	defer func() {
		if err != nil {
			c.result.failedConnects += checkDeadLine(err, t)
		}
	}()
	buf := make([]byte, 16)
	copy(buf, udpConnectHeader)

	binary.BigEndian.PutUint32(buf[12:16], transactionID)

	n, err := c.conn.Write(buf)
	if err != nil {
		return 0, err
	}

	if n != 16 {
		return 0, errTruncatedTx
	}

	buf = make([]byte, 64)
	n, err = c.conn.Read(buf)
	if err != nil {
		return 0, err
	}

	if n != 16 {
		return 0, errTruncatedTx
	}

	b := buf[:n]

	action := binary.BigEndian.Uint32(b[:4])
	if action != 0 {
		return 0, errUnexpectedAction
	}

	transID := binary.BigEndian.Uint32(b[4:8])
	if transID != transactionID {
		return 0, errTransactionIDMismatch
	}

	connID := binary.BigEndian.Uint64(b[8:16])
	return connID, nil
}
