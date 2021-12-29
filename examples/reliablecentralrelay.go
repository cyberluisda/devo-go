package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/cyberluisda/devo-go/applogger"
	"github.com/cyberluisda/devo-go/devosender"
	"github.com/xujiajun/nutsdb"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func heapProfileDump(formatFileName string, withNow bool) error {
	var fileName string
	if withNow {
		fileName = fmt.Sprintf(formatFileName, time.Now().Format(time.RFC3339))
	} else {
		fileName = formatFileName
	}

	log.Printf("Saving heap profile in %s\n", fileName)

	f, err := os.Create(fileName)
	if err != nil {

		return fmt.Errorf("While creating heap profile file %s, ignoring current profile dump: %v",
			fileName, err)
	}

	pprof.WriteHeapProfile(f)
	err = f.Close()
	if err != nil {
		return fmt.Errorf("While close heap profile file %s, ignoring current profile dump: %v",
			fileName, err)
	}
	return nil
}

const (
	cpuProfileOutPath             = "/tmp/reliablecentralrelay-cpuprofile"
	heapprofileOutPathFormat      = "/tmp/reliablecentralrelay-heapprofile-%s"
	heapprofileOutEndWithClient   = "/tmp/reliablecentralrelay-heapprofile-beforeclose"
	heapprofileOutEndClosedClient = "/tmp/reliablecentralrelay-heapprofile-clientclosed"
	heapEachEvents                = 10000
)

func main() {

	keyFile := flag.String("key", "", "Devo domain key file")
	certFile := flag.String("cert", "", "Devo domain certificate file")
	chainFile := flag.String("chain", "", "Devo domain certificate file")
	usDevo := flag.Bool("us", false, "Send data to us Devo site: "+devosender.DevoCentralRelayUS)
	euDevo := flag.Bool("eu", false, "Send data to us Devo site: "+devosender.DevoCentralRelayEU)
	localRelay := flag.String("relay", "", "Send data to custom relay. This option overwrites -us and -eu options")
	msgPerSecond := flag.Uint("mps", 1, "Number of messages per second to send")
	msgBodySize := flag.Uint("size", 1024, "Lengh of the random string to send in the message")
	seconds := flag.Int("seconds", 20, "Number of seconds to be running, negative number implies \"forever\"")
	tag := flag.String("tag", "test.keep.free", "Devo tag")
	statusPath := flag.String("status", "/tmp/reliablecentralrelay.db", "Directory path where save the status")
	statusFileSize := flag.Int64("status-file-size", nutsdb.DefaultOptions.SegmentSize, "Max size per file used to persist status")
	retryDaemonWait := flag.Duration("retry-duration-checks", time.Second*30, "Time to wait between retry pending events shot")
	reconnectDaemonWait := flag.Duration("reconn-duration-checks", time.Minute, "Time to wait between two consecutive times that checks connection failed and reconnects in afirmative case")
	consolidateDaemonWait := flag.Duration("consol-duration-checks", time.Minute, "Time to wait between two consecutive times to checks and consolidate status db if needed")
	tcpTimeout := flag.Duration("tcp-timeout", time.Second*2, "Timeout to open tcp connection")
	bufferSize := flag.Uint("buffer", devosender.DefaultBufferEventsSize, "Internal status buffer size")
	displayOrigID := flag.Bool("display-orig-msg-id", false, "If true display the first ID for each mesage generated")
	cpuProfile := flag.Bool(
		"cpu-profile",
		false,
		fmt.Sprintf("Enable cpu profile and saving at the end of the program in %s", cpuProfileOutPath))
	heapProfile := flag.Bool(
		"heap-profile",
		false,
		fmt.Sprintf(
			"Enable heap profile. Heap profile will be saved each 'heap-profile-each-events' "+
				"events will be generated and saved in %s. First %%s is replaced by file creation timestamp",
			heapprofileOutPathFormat))
	heapProfileEachEvents := flag.Uint(
		"heap-profile-each-events",
		heapEachEvents,
		"Number of generated events to wait before to save heap-profile")
	flag.Parse()

	// Params validation
	if *keyFile == "" {
		// flag.PrintDefaults()
		// log.Fatalln("key can not be empty")
		keyFile = nil
	}
	if *certFile == "" {
		// flag.PrintDefaults()
		// log.Fatalln("cert can not be empty")
		certFile = nil
	}
	if *chainFile == "" {
		chainFile = nil
	}

	devoEntryPoint := *localRelay
	if *localRelay == "" {
		if *usDevo {
			devoEntryPoint = devosender.DevoCentralRelayUS
		} else if *euDevo {
			devoEntryPoint = devosender.DevoCentralRelayEU
		} else {
			devoEntryPoint = devosender.DevoCentralRelayUS
			log.Println("Neither us, eu nor rely option set. Selecting", devoEntryPoint)
		}
	}

	// CPU profiling
	if *cpuProfile {
		log.Printf("CPU profile will be saved in %s at the end of the program\n", cpuProfileOutPath)
		f, err := os.Create(cpuProfileOutPath)
		if err != nil {
			log.Fatalf("Error while create CPU profile report file: %v\n", err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	log.Println("Creating devosender Client connected to", devoEntryPoint)

	cb := devosender.NewClientBuilder().
		EntryPoint(devoEntryPoint).
		DefaultDevoTag(*tag).
		TCPTimeout(*tcpTimeout)
	if keyFile == nil || certFile == nil {
		log.Println("key file or cert file nil. Creating CLEAN connection")
	} else {
		log.Println("Configuring TLS connection")
		cb.TLSFiles(*keyFile, *certFile, chainFile)
	}

	rc, err := devosender.NewReliableClientBuilder().
		AppLogger(&applogger.WriterAppLogger{
			Writer: os.Stdout,
			Level:  applogger.DEBUG,
		}).
		BufferEventsSize(*bufferSize).
		ClientReconnDaemonInitDelay(time.Second * 15).
		ConsolidateDbDaemonInitDelay(time.Minute).
		RetryDaemonInitDelay(time.Second * 5).
		ClientReconnDaemonWaitBtwChecks(*reconnectDaemonWait).
		ConsolidateDbDaemonWaitBtwChecks(*consolidateDaemonWait).
		RetryDaemonWaitBtwChecks(*retryDaemonWait).
		DbPath(*statusPath).
		FlushTimeout(time.Second).
		DaemonStopTimeout(time.Minute).
		EnableStandByModeTimeout(time.Second).
		DbSegmentSize(*statusFileSize).
		ClientBuilder(cb).
		Build()

	if err != nil {
		log.Fatalf("Error while build client: %v", err)
	}

	if *seconds > 0 {
		log.Println("Starting to send", uint(*seconds)*(*msgPerSecond), "messages during", *seconds, "seconds to", *tag, "Devo tag")
	} else {
		log.Println("Starting to send", *msgPerSecond, "messages per second without limit")
	}

	// Capture termination and stop generate events: canceled = true
	canceled := false
	go func() {
		sigchan := make(chan os.Signal)
		signal.Notify(sigchan, os.Interrupt)
		<-sigchan // wait until signal received

		// Stop
		canceled = true
		log.Println("Stop signal recieved cancel process started!")
	}()

	// main bucle
	var totalMsgs uint
	for *seconds != 0 && !canceled {

		for i := uint(0); i < *msgPerSecond; i++ {
			payload := randomChars(*msgBodySize)
			id := rc.SendAsync(payload)

			if *displayOrigID {
				log.Println("New msg generated with id", id)
			}
		}

		totalMsgs += *msgPerSecond
		if totalMsgs%1000 == 0 {
			log.Println("msgs send until now", totalMsgs)
			log.Println("#Events send with error until now", rc.AsyncErrorsNumber())
			log.Printf("Client stats: %+v\n", rc.Stats())
		}

		if *heapProfile && totalMsgs%(*heapProfileEachEvents) == 0 {
			err := heapProfileDump(heapprofileOutPathFormat, true)
			if err != nil {
				log.Printf("Error while dump heap prefile, events %d: %v",
					totalMsgs, err)
			}
		}

		time.Sleep(time.Second)

		if *seconds > 0 {
			*seconds = *seconds - 1
		}
	}

	if canceled {
		log.Println("Canceled!")
		os.Exit(1)
	}

	log.Println("#Events send with error until now", rc.AsyncErrorsNumber())
	if rc.AsyncErrorsNumber() > 0 {
		idsWithError := rc.AsyncErrorsIds()
		_, err := rc.AsyncError(idsWithError[0])
		log.Println("First error", err)

		_, err = rc.AsyncError(idsWithError[len(idsWithError)-1])
		log.Println("Last error", err)
	}

	if *heapProfile {
		log.Println("Heap profile dump before clossing client")
		err := heapProfileDump(heapprofileOutEndWithClient, false)
		if err != nil {
			log.Printf("Error while dump heap prefile, events %d: %v",
				totalMsgs, err)
		}
	}

	log.Println("Closing client")
	err = rc.Close()
	log.Printf("Client stats: %+v\n", rc.Stats())
	if err != nil {
		log.Fatalf("Error while close client: %v", err)
	}

	if *heapProfile {
		runtime.GC()
		log.Println("Heap profile dump after closing client and GC called")
		err := heapProfileDump(heapprofileOutEndClosedClient, false)
		if err != nil {
			log.Printf("Error while dump heap prefile, events %d: %v",
				totalMsgs, err)
		}
	}
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randomChars(n uint) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}

	return string(b)
}
