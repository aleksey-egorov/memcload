package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

var (
	Info  *log.Logger
	Error *log.Logger
)

type Job struct {
	pattern string
	workers int
	idfa    string
	gaid    string
	adid    string
	dvid    string
	//quitChan    chan chan bool
	//newConnChan chan net.Conn
	//recieveChan chan *Packet
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {

	workers := flag.Int("workers", 200, "Number of workers")
	logfile := flag.String("log", "", "Log file")
	pattern := flag.String("pattern", "*.tsv.gz", "Pattern")
	idfa := flag.String("idfa", "35.226.182.234:11211", "")
	gaid := flag.String("gaid", "35.232.4.163:11211", "")
	adid := flag.String("adid", "35.226.182.234:11211", "")
	dvid := flag.String("dvid", "35.232.4.163:11211", "")

	flag.Parse()

	Info = log.New(os.Stdout, "I: ", log.Ldate|log.Ltime|log.Lshortfile)
	Error = log.New(os.Stdout, "E: ", log.Ldate|log.Ltime|log.Lshortfile)

	if *logfile != "" {
		f, err := os.OpenFile(*logfile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		check(err)

		defer f.Close()
		log.SetOutput(f)
	}

	process(&Job{*pattern, *workers, *idfa, *gaid, *adid, *dvid})
}

func process(job *Job) {
	Info.Println("Processing pattern ", job.pattern)
	files, err := filepath.Glob(job.pattern)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(files)
}
