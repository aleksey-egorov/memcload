package main

import (
	"./appsinstalled"
	"bufio"
	"compress/gzip"
	"errors"
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
	"sync"
	"time"
)

var (
	Info  *log.Logger
	Error *log.Logger
	Debug *log.Logger
)

const NORMAL_ERR_RATE = 0.01
const CONNECTION_TIMEOUT = 5

type Job struct {
	pattern  string
	mworkers int
	lworkers int
	bufsize  int
	idfa     string
	gaid     string
	adid     string
	dvid     string
	dry      bool
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

	mworkers := flag.Int("mworkers", 200, "Number of Memc workers")
	lworkers := flag.Int("lworkers", 4, "Number of Line workers")
	logfile := flag.String("log", "", "Log file")
	test := flag.Bool("test", false, "Test mode")
	dry := flag.Bool("dry", false, "Dry")
	bufsize := flag.Int("bufsize", 100000, "Buffer size")
	pattern := flag.String("pattern", "*.tsv.gz", "Pattern")
	idfa := flag.String("idfa", "35.238.204.0:11211", "")
	gaid := flag.String("gaid", "107.178.221.100:11211", "")
	adid := flag.String("adid", "35.238.204.0:11211", "")
	dvid := flag.String("dvid", "107.178.221.100:11211", "")
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
		*mworkers,
		*lworkers,
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

func processFiles(job *Job) error {

	fwg := &sync.WaitGroup{}
	lwg := &sync.WaitGroup{}
	mwg := &sync.WaitGroup{}

	device_memc := map[string]string{
		"idfa": job.idfa,
		"gaid": job.gaid,
		"adid": job.adid,
		"dvid": job.dvid,
	}

	memc_workers_dev := job.mworkers / len(device_memc) // Memc workers are divided into groups equally

	Info.Printf("Processing pattern %s", job.pattern)
	Info.Printf("Memc workers per dev: %d, line workers: %d", memc_workers_dev, job.lworkers)

	files, err := filepath.Glob(job.pattern)
	if err != nil {
		Error.Printf("Could not find files for the given pattern: %s", job.pattern)
		return err
	}

	line_queue := make(chan Line, job.bufsize)
	memc_queues := make(map[string]chan *MemcItem)
	result_queue := make(chan Stats, job.bufsize)

	startFileWorkers(files, line_queue, fwg)
	startLineWorkers(job.lworkers, line_queue, memc_queues, result_queue, job.dry, lwg)
	startMemcWorkers(memc_workers_dev, memc_queues, result_queue, device_memc, job.bufsize, mwg)

	fwg.Wait()
	Info.Printf("File workers finished their tasks")
	close(line_queue)

	lwg.Wait()
	Info.Printf("Line workers finished their tasks")
	for dev_type, _ := range device_memc {
		close(memc_queues[dev_type])
	}

	mwg.Wait()
	Info.Printf("Memc workers finished their tasks")

	result_count := len(result_queue)
	processed, errors := 0, 0
	for i := 0; i < result_count; i++ {
		results := <-result_queue
		processed += results.processed
		errors += results.errors
	}
	Info.Printf("Total: processed=%d, errors=%d, result_queue=%d", processed, errors, len(result_queue))

	if processed == 0 {
		Error.Printf("No data processed, check file format")
		return nil
	}
	err_rate := float32(errors) / float32(processed)
	if err_rate < float32(NORMAL_ERR_RATE) {
		Info.Printf("Acceptable error rate (%f). Successfull load", err_rate)
	} else {
		Error.Printf("High error rate (%f > %f). Failed load", err_rate, float32(NORMAL_ERR_RATE))
	}

	for _, filename := range files {
		dotRename(filename)
	}
	close(result_queue)

	return nil
}

func startFileWorkers(files []string, line_queue chan Line, fwg *sync.WaitGroup) {
	for _, filename := range files {
		fwg.Add(1)
		go ProcessFile(filename, line_queue, fwg)
	}
}

func startLineWorkers(line_workers int, line_queue chan Line, memc_queues map[string]chan *MemcItem,
	result_queue chan Stats, dry bool, lwg *sync.WaitGroup) {
	for i := 0; i < line_workers; i++ {
		lwg.Add(1)
		go LineWorker(line_queue, memc_queues, result_queue, dry, lwg)
		Info.Printf("Starting line worker %d", i)
	}
}

func startMemcWorkers(memc_workers_dev int, memc_queues map[string]chan *MemcItem, result_queue chan Stats,
	device_memc map[string]string, bufsize int, mwg *sync.WaitGroup) {
	for dev_type, memc_addr := range device_memc {
		memc_queues[dev_type] = make(chan *MemcItem, bufsize)
		for i := 0; i < memc_workers_dev; i++ {
			mc := memcache.New(memc_addr)
			mc.Timeout = CONNECTION_TIMEOUT * time.Second
			mwg.Add(1)
			worker_name := fmt.Sprintf("%s_%d", dev_type, i)
			go MemcWorker(mc, memc_queues[dev_type], result_queue, worker_name, mwg)
			Info.Printf("Starting memc worker %s ", worker_name)
		}
	}
}

func ProcessFile(filename string, line_queue chan Line, fwg *sync.WaitGroup) error {
	defer fwg.Done()
	Info.Println("File ", filename)
	f, err := os.Open(filename)
	if err != nil {
		Error.Printf("Can't open file: %s", filename)
		return err
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		Error.Printf("Can't open gzip Reader %v", err)
		return err
	}
	defer gr.Close()

	sc := bufio.NewScanner(gr)
	line_num := 0
	for sc.Scan() {
		line := sc.Text()
		line = strings.Trim(line, " ")
		line_queue <- Line{num: line_num, line: line}
		line_num += 1
	}
	if err := sc.Err(); err != nil {
		Error.Printf("Bufio scanner error: %v", err)
		return err
	}

	return nil
}

func LineWorker(lines chan Line, memc_queue map[string]chan *MemcItem, result_queue chan Stats, dry bool, lwg *sync.WaitGroup) {
	errors := 0
	defer lwg.Done()
	for line := range lines {
		appsinstalled, err := parseAppsInstalled(line.line)
		if appsinstalled == nil {
			errors += 1
			Error.Println("Parsing AppsInstalle error: ", err)
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
	Info.Printf("LineWorker: final errors %d, queue %d", errors, len(result_queue))
}

func MemcWorker(mc *memcache.Client, items chan *MemcItem, result_queue chan Stats, worker_name string,
	mwg *sync.WaitGroup) {
	processed, errors := 0, 0
	defer mwg.Done()
	for item := range items {
		err := mc.Set(&memcache.Item{
			Key:   item.key,
			Value: item.data,
		})
		if err != nil {
			Error.Println("Error writing to Memcached: ", err)
			errors += 1
		} else {
			processed += 1
		}

		if processed > 1000 || errors > 1000 {
			result_queue <- Stats{errors: errors, processed: processed}
			processed = 0
			errors = 0
			Debug.Printf("%d: MemcWorker %s: proc=%d, channels: memc=%d res=%d", item.num, worker_name, processed, len(items), len(result_queue))
		}
	}
	result_queue <- Stats{errors: errors, processed: processed}
	Info.Printf("MemcWorker %s: final proc=%d, err=%d, channels: memc=%d res=%d", worker_name, processed, errors, len(items), len(result_queue))
}

func prototest() {
	Info.Println("Starting test ... ")
	sample := "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
	for _, line := range strings.Split(sample, "\n") {
		apps_installed, err := parseAppsInstalled(line)
		checkErr(err)
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

func dotRename(path string) error {
	head := filepath.Dir(path)
	fn := filepath.Base(path)
	if err := os.Rename(path, filepath.Join(head, "."+fn)); err != nil {
		Error.Printf("Can't rename a file: %s", path)
		return err
	}
	return nil
}

func parseAppsInstalled(str string) (*AppsInstalled, error) {

	var apps []uint32

	str = strings.TrimSpace(str)
	line_parts := strings.Split(str, "\t")

	if len(line_parts) < 5 {
		return nil, errors.New("Some line parts missing")
	}

	dev_type := line_parts[0]
	dev_id := line_parts[1]
	lat_str := line_parts[2]
	lon_str := line_parts[3]
	raw_apps := line_parts[4]

	if dev_type == "" || dev_id == "" {
		return nil, errors.New("dev_type or dev_id missing")
	}
	parts := strings.Split(raw_apps, ",")
	for _, a := range parts {
		a = strings.TrimSpace(a)
		digit, err := strconv.Atoi(a)
		if err != nil {
			return nil, err
		}
		apps = append(apps, uint32(digit))
	}

	lat, err := strconv.ParseFloat(lat_str, 8)
	if err != nil {
		return nil, err
	}
	lon, err := strconv.ParseFloat(lon_str, 8)
	if err != nil {
		return nil, err
	}

	return &AppsInstalled{dev_type, dev_id, lat, lon, apps}, nil
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
