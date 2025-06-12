/*
Copyright © 2025 jesse galley <jesse@jessegalley.net>
*/
package main

import (
	"log"
	"os"
	"runtime/pprof"

	"github.com/jessegalley/iolyzer/cmd"
)

func main() {

	cpuProfile := ""
	// cpuProfile := "./prof.pprof"
	if cpuProfile != "" {
		f, err := os.Create(cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()

		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	cmd.Execute()
}
