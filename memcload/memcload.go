package memcload

import (
	"../appsinstalled"
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

func main() {

	logfile := flag.String("log", "", "Log file")
	test := flag.Bool("test", false, "Test mode")
	dry := flag.Bool("dry", false, "Test mode")
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

	if *test == true {
		prototest()
	}

	processFiles(&Job{*pattern, *idfa, *gaid, *adid, *dvid, *dry})
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

	Info.Println("Processing pattern ", job.pattern)
	files, err := filepath.Glob(job.pattern)
	checkErr(err)

	for _, filename := range files {
		processed, errors := processFile(filename, device_memc, job.dry)

		err_rate := float32(errors) / float32(processed)
		if err_rate < NORMAL_ERR_RATE {
			Info.Printf("Acceptable error rate (%s). Successfull load", err_rate)
		} else {
			Error.Printf("High error rate (%s > %s). Failed load", err_rate, NORMAL_ERR_RATE)
			dotRename(filename)
		}
	}
}

func processFile(filename string, device_memc map[string]string, dry bool) (int, int) {

	var errors int
	var processed int
	var memc_addr string

	memc_pool := map[string]*memcache.Client{}
	for key := range device_memc {
		addr := device_memc[key]
		memc_pool[addr] = memcache.New(addr)
		memc_pool[addr].Timeout = CONNECTION_TIMEOUT
	}

	Info.Println("File ", filename)
	f, err := os.Open(filename)
	checkErr(err)
	defer f.Close()
	line_num := 0

	gr, err := gzip.NewReader(f)
	checkErr(err)
	defer gr.Close()

	sc := bufio.NewScanner(gr)
	for sc.Scan() {

		line := sc.Text()
		line = strings.Trim(line, " ")

		appsinstalled := parseAppsInstalled(line)
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

		ok := insertAppsInstalled(memc_pool, memc_addr, appsinstalled, dry, line_num)
		if ok {
			processed += 1
		} else {
			errors += 1
		}
		line_num += 1
	}
	return processed, errors
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

func insertAppsInstalled(memc_pool map[string]*memcache.Client,
	memc_addr string,
	apps_installed *AppsInstalled,
	dry bool,
	line_num int) bool {

	ua := &appsinstalled.UserApps{
		Lat:  proto.Float64(apps_installed.lat),
		Lon:  proto.Float64(apps_installed.lon),
		Apps: apps_installed.apps,
	}

	key := fmt.Sprintf("%s:%s", apps_installed.dev_type, apps_installed.dev_id)
	packed, _ := proto.Marshal(ua)

	if dry {
		Debug.Println("%s - %s -> %s", memc_addr, key, ua.String())
	} else {
		result := writeMemc(memc_pool, memc_addr, key, packed)
		if !result {
			Error.Printf("Cannot write to memc %s: %v", memc_addr)
		} else {
			Info.Printf("%d: %s", line_num, key)
		}
		return result
	}
	return true
}

func writeMemc(memc_pool map[string]*memcache.Client, memc_addr string, key string, packed []byte) bool {
	mc := memc_pool[memc_addr]
	err := mc.Set(&memcache.Item{
		Key:   key,
		Value: packed,
	})
	if err != nil {
		checkErr(err)
		return false
	}
	return true
}
