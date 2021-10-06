package applogger

import (
	"bytes"
	"fmt"
)

func ExampleWriterAppLogger() {
	var buf bytes.Buffer
	lg := &WriterAppLogger{Writer: &buf, Level: DEBUG, AddNow: false}
	i := 0
	lg.Debug("Debug called", "i", i)
	i++
	lg.Debugf("Debugf called, i: %d", i)
	i++

	lg.Info("Info called", "i", i)
	i++
	lg.Infof("Infof called, i: %d", i)
	i++

	lg.Warn("Warn called", "i", i)
	i++
	lg.Warnf("Warnf called, i: %d", i)
	i++

	lg.Error("Error called", "i", i)
	i++
	lg.Errorf("Errorf called, i: %d", i)
	i++

	lg.Fatal("Fatal called", "i", i)
	i++
	lg.Fatalf("Fatalf called, i: %d", i)

	fmt.Print(buf.String())

	for i := FATAL; i <= DEBUG; i++ {
		fmt.Println("Is", LevelString(i), "enabled?:", lg.IsLevelEnabled(i))
	}
	// Output:
	// DEBUG Debug called i 0
	// DEBUG Debugf called, i: 1
	// INFO Info called i 2
	// INFO Infof called, i: 3
	// WARN Warn called i 4
	// WARN Warnf called, i: 5
	// ERROR Error called i 6
	// ERROR Errorf called, i: 7
	// FATAL Fatal called i 8
	// FATAL Fatalf called, i: 9
	// Is FATAL enabled?: true
	// Is ERROR enabled?: true
	// Is WARN enabled?: true
	// Is INFO enabled?: true
	// Is DEBUG enabled?: true

}

func ExampleNoLogAppLogger() {
	lg := &NoLogAppLogger{}
	i := 0
	lg.Debug("Debug called", "i", i)
	i++
	lg.Debugf("Debugf called, i: %d", i)
	i++

	lg.Info("Info called", "i", i)
	i++
	lg.Infof("Infof called, i: %d", i)
	i++

	lg.Warn("Warn called", "i", i)
	i++
	lg.Warnf("Warnf called, i: %d", i)
	i++

	lg.Error("Error called", "i", i)
	i++
	lg.Errorf("Errorf called, i: %d", i)
	i++

	lg.Fatal("Fatal called", "i", i)
	i++
	lg.Fatalf("Fatalf called, i: %d", i)

	lg.Log(FATAL, "Log with FATAL level called", "i", i)
	i++
	lg.Logf(FATAL, "Logf with FATAL level called, i: %d", i)

	for i := FATAL; i <= DEBUG; i++ {
		fmt.Println("Is", LevelString(i), "enabled?:", lg.IsLevelEnabled(i))
	}
	// Output:
	// Is FATAL enabled?: false
	// Is ERROR enabled?: false
	// Is WARN enabled?: false
	// Is INFO enabled?: false
	// Is DEBUG enabled?: false

}
