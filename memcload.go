package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

type Appsinstalled struct {
	dev_type string
	dev_id   string
	lat      float64
	lon      float64
	raw_apps []int
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
		if err != nil {
			panic(err)
		}

		defer f.Close()
		log.SetOutput(f)
	}

	process(&Job{*pattern, *workers, *idfa, *gaid, *adid, *dvid})
}

func check_err(e error) {
	if e != nil {
		Error.Println(e)
	}
}

func process(job *Job) {

	var errors int

	device_memc := map[string]string{
		"idfa": job.idfa,
		"gaid": job.gaid,
		"adid": job.adid,
		"dvid": job.dvid,
	}

	Info.Println("Processing pattern ", job.pattern)
	files, err := filepath.Glob(job.pattern)
	check_err(err)

	for _, filename := range files {

		Info.Println("File ", filename)
		f, err := os.Open(filename)
		check_err(err)
		defer f.Close()

		gr, err := gzip.NewReader(f)
		check_err(err)
		defer gr.Close()

		//n, err := io.ReadFull(gr, buf)
		//fmt.Println(buf)
		//fmt.Println(n)

		//for {
		cr := bufio.NewReader(gr)
		barr, _, err := cr.ReadLine()
		check_err(err)

		appsinstalled := parse_appsinstalled(barr)
		if appsinstalled == nil {
			errors += 1
			continue
		}

		memc_addr := device_memc[appsinstalled.dev_type]
		Info.Println(memc_addr)

		//}

	}
}

func parse_appsinstalled(barr []byte) *Appsinstalled {

	var apps []int

	str := string(barr[:])
	str = strings.TrimSpace(str)
	line_parts := strings.Split(str, "\t")

	if len(line_parts) < 5 {
		return nil
	}

	dev_type := line_parts[0]
	dev_id := line_parts[1]
	lat_str := line_parts[2]
	lon_str := line_parts[3]
	raw_apps := line_parts[4]

	if dev_type == "" || dev_id == "" {
		return nil
	}
	parts := strings.Split(raw_apps, ",")
	for _, a := range parts {
		a = strings.TrimSpace(a)
		digit, err := strconv.Atoi(a)
		check_err(err)
		apps = append(apps, digit)
	}

	lat, err := strconv.ParseFloat(lat_str, 8)
	check_err(err)
	lon, err := strconv.ParseFloat(lon_str, 8)
	check_err(err)

	return &Appsinstalled{dev_type, dev_id, lat, lon, apps}

	/*def parse_appsinstalled(line):
	line_parts = line.decode("utf-8").strip().split("\t")
	if len(line_parts) < 5:
	return
	dev_type, dev_id, lat, lon, raw_apps = line_parts
	if not dev_type or not dev_id:
	return
	try:

	apps = [int(a.strip()) for a in raw_apps.split(",")]
	except ValueError:
	apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
	logging.info("Not all user apps are digits: `%s`" % line)
	try:

		lat, lon = float(lat), float(lon)
	except ValueError:
	logging.info("Invalid geo coords: `%s`" % line)
	return AppsInstalled(dev_type, dev_id, lat, lon, apps) */
}
