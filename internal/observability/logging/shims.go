package logging

import "os"

func newStdout() *os.File { return os.Stdout }
func newStderr() *os.File { return os.Stderr }
