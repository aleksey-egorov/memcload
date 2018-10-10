package main

import (
	"./appsinstalled"
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/golang/protobuf/proto"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var (
	Info  *log.Logger
	Error *log.Logger
	Debug *log.Logger
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
	apps     []uint32
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
	Debug = log.New(os.Stdout, "D: ", log.Ldate|log.Ltime|log.Lshortfile)

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
	var processed int
	var memc_addr string

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

		sc := bufio.NewScanner(gr)
		for sc.Scan() {

			line := sc.Text()
			line = strings.Trim(line, " ")

			appsinstalled := parse_appsinstalled(line)

			if appsinstalled == nil {
				errors += 1
				continue
			}

			memc_addr = device_memc[appsinstalled.dev_type]
			if memc_addr == "" {
				errors += 1
				Error.Println("Unknown device type: ", appsinstalled.dev_type)
				continue
			}
			ok := insert_appsinstalled(memc_addr, appsinstalled, false)
			if ok {
				processed += 1
			} else {
				errors += 1
			}
		}

		/*if not processed:
		fd.close()
		dot_rename(fn)
		continue

		err_rate = float(errors) / processed
		if err_rate < NORMAL_ERR_RATE:
		logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
		else:
		logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
		fd.close()
		dot_rename(fn)
		*/

		//}

	}
}

func parse_appsinstalled(str string) *Appsinstalled {

	var apps []uint32

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
		apps = append(apps, uint32(digit))
	}

	lat, err := strconv.ParseFloat(lat_str, 8)
	check_err(err)
	lon, err := strconv.ParseFloat(lon_str, 8)
	check_err(err)

	return &Appsinstalled{dev_type, dev_id, lat, lon, apps}

}

func insert_appsinstalled(memc_addr string, apps_installed *Appsinstalled, dry_run bool) bool {

	ua := &appsinstalled.UserApps{
		Lat:  proto.Float64(apps_installed.lat),
		Lon:  proto.Float64(apps_installed.lon),
		Apps: apps_installed.apps,
	}

	key := fmt.Sprintf("%s:%s", apps_installed.dev_type, apps_installed.dev_id)
	packed, _ := proto.Marshal(ua)

	if dry_run {
		Debug.Println("%s - %s -> %s", memc_addr, key, ua.String())
	} else {
		mc := memcache.New(memc_addr)

		err := mc.Set(&memcache.Item{
			Key:   key,
			Value: packed,
		})
		if err != nil {
			Error.Printf("Cannot write to memc %s: %v", memc_addr, err)
			return false
		}
	}
	return true

}
