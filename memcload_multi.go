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
	"reflect"
	"strconv"
	"strings"
	"time"
)

var (
	Info  *log.Logger
	Error *log.Logger
	Debug *log.Logger
)

const NORMAL_ERR_RATE = 0.01
const CONNECTION_TIMEOUT = 1000000000 // in nanoseconds, this equals to 1 sec

type Job struct {
	pattern string
	workers int
	bufsize int
	idfa    string
	gaid    string
	adid    string
	dvid    string
	dry     bool
}

type AppsInstalled struct {
	dev_type string
	dev_id   string
	lat      float64
	lon      float64
	apps     []uint32
}

type MemcItem struct {
	num  int
	key  string
	data []byte
}

type Stats struct {
	processed int
	errors    int
}

type Line struct {
	num  int
	line string
}

func main() {

	workers := flag.Int("workers", 200, "Number of workers")
	logfile := flag.String("log", "", "Log file")
	test := flag.Bool("test", false, "Test mode")
	dry := flag.Bool("dry", false, "Dry")
	bufsize := flag.Int("bufsize", 100000, "Buffer size")
	pattern := flag.String("pattern", "*.tsv.gz", "Pattern")
	idfa := flag.String("idfa", "35.226.182.234:11211", "")
	gaid := flag.String("gaid", "35.232.4.163:11211", "")
	adid := flag.String("adid", "35.226.182.234:11211", "")
	dvid := flag.String("dvid", "35.232.4.163:11211", "")
	flag.Parse()

	log_output := os.Stdout
	if *logfile != "" {
		f, err := os.OpenFile(*logfile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		log.SetOutput(f)
		log_output = f
	}

	Info = log.New(log_output, "I: ", log.Ldate|log.Ltime|log.Lshortfile)
	Error = log.New(log_output, "E: ", log.Ldate|log.Ltime|log.Lshortfile)
	Debug = log.New(os.Stdout, "D: ", log.Ldate|log.Ltime|log.Lshortfile)

	if *test == true {
		prototest()
	}

	start_time := time.Now()
	processFiles(&Job{*pattern,
		*workers,
		*bufsize,
		*idfa,
		*gaid,
		*adid,
		*dvid,
		*dry})
	stop_time := time.Now()
	elapsed_time := stop_time.Sub(start_time)

	Info.Printf("Time elapsed: %s sec", elapsed_time)
	Info.Printf("Work finished")
}

func checkErr(e error) {
	if e != nil {
		Error.Println(e)
	}
}

func processFiles(job *Job) {

	device_memc := map[string]string{
		"idfa": job.idfa,
		"gaid": job.gaid,
		"adid": job.adid,
		"dvid": job.dvid,
	}
	memc_workers := job.workers
	line_workers := 4

	Info.Printf("Processing pattern %s", job.pattern)
	Info.Printf("Memc workers: %d, line workers: %d", memc_workers, line_workers)

	files, err := filepath.Glob(job.pattern)
	checkErr(err)

	result_queue := make(chan Stats)
	memc_queues := make(map[string]chan *MemcItem)

	for dev_type, memc_addr := range device_memc {
		memc_queues[dev_type] = make(chan *MemcItem, job.bufsize)
		for i := 0; i < memc_workers/4; i++ {
			mc := memcache.New(memc_addr)
			mc.Timeout = CONNECTION_TIMEOUT
			worker_name := fmt.Sprintf("%s_%d", dev_type, i)
			go MemcWorker(mc, memc_queues[dev_type], result_queue, worker_name)
			Info.Printf("Starting memc worker %s ", worker_name)
		}
	}

	line_queue := make(chan Line, job.bufsize)
	for i := 0; i < line_workers; i++ {
		go LineWorker(line_queue, memc_queues, result_queue, job.dry)
		Info.Printf("Starting line worker %d", i)
	}

	for _, filename := range files {

		processFile(filename, device_memc, line_queue)

		// Wait until queues are empty
		for {
			memc_sum := 0
			line_sum := len(line_queue)
			for dev_type, _ := range device_memc {
				memc_sum += len(memc_queues[dev_type])
			}
			Info.Printf("Channels: %d %d %d", line_sum, memc_sum, len(result_queue))
			if line_sum == 0 && memc_sum == 0 {
				break
			}
			time.Sleep(5000000000) // 5 sec
		}
		dotRename(filename)
	}

	close(line_queue)
	for dev_type, _ := range device_memc {
		close(memc_queues[dev_type])
	}

	processed, errors := 0, 0
	for i := 0; i < len(result_queue); i++ {
		results := <-result_queue
		processed += results.processed
		errors += results.errors
		Info.Printf("%d: Processed stats %d, Errors: %d", i, results.errors, results.processed)
	}
	close(result_queue)

	err_rate := float32(errors) / float32(processed)
	if err_rate < float32(NORMAL_ERR_RATE) {
		Info.Printf("Acceptable error rate (%d). Successfull load", err_rate)
	} else {
		Error.Printf("High error rate (%f > %f). Failed load", err_rate, float32(NORMAL_ERR_RATE))

	}

}

func processFile(filename string, device_memc map[string]string, line_queue chan Line) {

	Info.Println("File ", filename)
	f, err := os.Open(filename)
	checkErr(err)
	defer f.Close()

	gr, err := gzip.NewReader(f)
	checkErr(err)
	defer gr.Close()

	Info.Println("File parsing ")
	sc := bufio.NewScanner(gr)
	line_num := 0
	for sc.Scan() {

		line := sc.Text()
		line = strings.Trim(line, " ")

		line_queue <- Line{num: line_num, line: line}
		line_num += 1
	}
}

func LineWorker(lines chan Line, memc_queue map[string]chan *MemcItem, result_queue chan Stats, dry bool) {
	errors := 0

	for line := range lines {
		appsinstalled := parseAppsInstalled(line.line)
		if appsinstalled == nil {
			errors += 1
			continue
		}

		item, err := makeMemcItem(appsinstalled, line.num)
		if err != nil {
			errors += 1
			Error.Println("Cant make MemcItem: ", err)
			continue
		}

		queue, ok := memc_queue[appsinstalled.dev_type]
		if !ok {
			errors += 1
			Error.Println("Unknown device type: ", appsinstalled.dev_type)
			continue
		}

		if dry {
			Debug.Printf("%d: Fake add %s", item.num, item.key)
		} else {
			queue <- item
		}
	}
	result_queue <- Stats{errors: errors}
}

func MemcWorker(mc *memcache.Client, items chan *MemcItem, result_queue chan Stats, worker_name string) {
	processed, errors := 0, 0
	for {
		item := <-items
		err := mc.Set(&memcache.Item{
			Key:   item.key,
			Value: item.data,
		})
		if err != nil {
			checkErr(err)
			errors += 1
		} else {
			//Debug.Printf("%d: Worker %s - memc added %s", item.num, worker_name, item.key)
			Debug.Printf("%d: Worker %s: key=%s, processed=%d, errors=%d, range=%d", item.num, worker_name, item.key, processed, errors, len(items))
			processed += 1
		}
		if len(items) == 0 {
			break
		}
	}
	Info.Printf("Worker %s: final memc processed %d, errors %d", worker_name, processed, errors)
	result_queue <- Stats{errors: errors, processed: processed}
}

func prototest() {
	Info.Println("Starting test ... ")
	sample := "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
	for _, line := range strings.Split(sample, "\n") {
		apps_installed := parseAppsInstalled(line)
		ua := &appsinstalled.UserApps{
			Lat:  proto.Float64(apps_installed.lat),
			Lon:  proto.Float64(apps_installed.lon),
			Apps: apps_installed.apps,
		}
		packed, err := proto.Marshal(ua)
		checkErr(err)

		unpacked := &appsinstalled.UserApps{}
		err = proto.Unmarshal(packed, unpacked)
		checkErr(err)

		if ua.GetLat() != unpacked.GetLat() || !reflect.DeepEqual(ua.GetApps(), unpacked.GetApps()) {
			Error.Println("Test failed!")
		} else {
			Info.Println("Test passed")
		}
		os.Exit(1)
	}
}

func dotRename(path string) {
	head := filepath.Dir(path)
	fn := filepath.Base(path)
	if err := os.Rename(path, filepath.Join(head, "."+fn)); err != nil {
		Error.Printf("Can't rename a file: %s", path)
	}
}

func parseAppsInstalled(str string) *AppsInstalled {

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
		checkErr(err)
		apps = append(apps, uint32(digit))
	}

	lat, err := strconv.ParseFloat(lat_str, 8)
	checkErr(err)
	lon, err := strconv.ParseFloat(lon_str, 8)
	checkErr(err)

	return &AppsInstalled{dev_type, dev_id, lat, lon, apps}
}

func makeMemcItem(apps_installed *AppsInstalled, num int) (*MemcItem, error) {
	ua := &appsinstalled.UserApps{
		Lat:  proto.Float64(apps_installed.lat),
		Lon:  proto.Float64(apps_installed.lon),
		Apps: apps_installed.apps,
	}
	key := fmt.Sprintf("%s:%s", apps_installed.dev_type, apps_installed.dev_id)
	packed, err := proto.Marshal(ua)
	if err != nil {
		return nil, err
	}
	return &MemcItem{num, key, packed}, nil
}
