// Conformance sender: the REAL sentry-go SDK, unmodified, DSN-only.
package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/getsentry/sentry-go"
)

func fetchInvoice() error {
	return errors.New("invoice 4471 not found in ledger")
}

func main() {
	if err := sentry.Init(sentry.ClientOptions{
		Dsn:         os.Args[1],
		Release:     "conformance-go-1.0",
		Environment: "conformance",
	}); err != nil {
		fmt.Fprintln(os.Stderr, "init:", err)
		os.Exit(1)
	}
	if err := fetchInvoice(); err != nil {
		sentry.CaptureException(err)
	}
	if !sentry.Flush(10 * time.Second) {
		fmt.Fprintln(os.Stderr, "go-sdk: flush timed out")
		os.Exit(1)
	}
	fmt.Println("go-sdk: flushed")
}
