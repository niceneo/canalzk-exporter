package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/version"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/sirupsen/logrus"
	"gopkg.in/ini.v1"
	"io"
	"net/http"
	"os"
	"path"
	"sync"
	"time"
)

const (
	metricsRoute = "/metrics"
	namespace    = "canal"
	canalpath    = "/otter/canal/destinations"
)

var (
	listenAddress     = flag.String("web.listen-address", ":9311", "Address to listen on for web interface.")
	serverTimeout     = flag.Duration("web.timeout", 60*time.Second, "Timeout for responding to http requests.")
	zkTimeout         = flag.Duration("zk.timeout", 5*time.Second, "Timeout for ZooKeeper requests")
	log               = logrus.New()
	canalzk_running   *prometheus.Desc
	canalzk_timestamp *prometheus.Desc
	canalzk_cluster   *prometheus.Desc
	canalzk_up        *prometheus.Desc
)

type Sourceaddress struct {
	Address string `json:"address"`
	Port    int64  `json:"port"`
}

type Identity struct {
	Slaveid       int64          `json:"slaveId"`
	Sourceaddress *Sourceaddress `json:"sourceAddress"`
}

type Postion struct {
	Gtid      string  `json:"gtid"`
	Included  bool    `json:"included"`
	Journal   string  `json:"journalName"`
	Postion   float64 `json:"position"`
	Serverid  string  `json:"serverId"`
	Timestamp float64 `json:"timestamp"`
}

type cursor struct {
	Type     string    `json:"@type"`
	Identity *Identity `json:"identity"`
	Postion  *Postion  `json:"postion"`
}

type canalzk struct {
	Clustername string
	Zookeeper   string
	Chroot      string
	Filter      []string
}

type collector struct {
	canalzk []canalzk
	timeout time.Duration
	cfg     *ini.File
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- canalzk_timestamp
	ch <- canalzk_running
	ch <- canalzk_cluster
	ch <- canalzk_up
}
func (c *collector) Collect(ch chan<- prometheus.Metric) {
	var wg sync.WaitGroup
	for _, sect := range c.cfg.SectionStrings() {
		if sect == "DEFAULT" {
			continue
		}
		canalzk_config := &canalzk{}
		err := c.cfg.Section(sect).MapTo(canalzk_config)
		if err != nil {
			log.Println(err)
			continue
		}
		wg.Add(1)
		go c.collectNew(ch, canalzk_config, &wg)

	}
	wg.Wait()
	//var wg sync.WaitGroup
	//for _, czk := range c.canalzk {
	//	wg.Add(1)
	//	go c.collect(ch, czk, &wg)
	//}
	//wg.Wait()
}

func (c *collector) collectNew(ch chan<- prometheus.Metric, czk *canalzk, wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Println(czk.Chroot, czk.Clustername, czk.Zookeeper, czk.Filter)
	zkclient, _, err := zk.Connect([]string{czk.Zookeeper}, c.timeout, func(c *zk.Conn) { c.SetLogger(zk.DefaultLogger) })
	defer zkclient.Close()
	if err != nil {
		log.Println("connect error: ", err)
		ch <- prometheus.MustNewConstMetric(
			canalzk_up,
			prometheus.GaugeValue,
			float64(1),
			czk.Clustername, czk.Zookeeper,
		)
		return
	}
	instances, _, err := zkclient.Children(path.Join(czk.Chroot, canalpath))
	if err != nil {
		log.Println("canal zk instance path failed,err:", err, path.Join(czk.Chroot, canalpath))
		return
	}
LABELFILTER:
	for _, i := range instances {
		//Filter
		for _, f := range czk.Filter {
			if i == f {
				continue LABELFILTER
			}
		}
		//canal instance cluster
		cl, _, err := zkclient.Children(path.Join(czk.Chroot, canalpath, i, "cluster"))
		if err != nil {
			ch <- prometheus.MustNewConstMetric(
				canalzk_cluster,
				prometheus.GaugeValue,
				float64(0),
				czk.Clustername, i,
			)
			log.Println(i, err)
			continue
		}
		ch <- prometheus.MustNewConstMetric(
			canalzk_cluster,
			prometheus.GaugeValue,
			float64(len(cl)),
			czk.Clustername, i,
		)
		//canal instance running metric, 0:normal,1:abnormal
		running := 0
		r, _, err := zkclient.Get(path.Join(czk.Chroot, canalpath, i, "running"))
		if err != nil {
			running = 1
			log.Println("canal zk path err: ", err, path.Join(czk.Chroot, canalpath, i, "running"), r)
		}
		ch <- prometheus.MustNewConstMetric(
			canalzk_running,
			prometheus.GaugeValue,
			float64(running),
			czk.Clustername, i,
		)
		//canal sync mysql cursor timestamp metric
		info, _, err := zkclient.Get(path.Join(czk.Chroot, canalpath, i, "1001/cursor"))
		if err != nil {
			log.Printf("canal zk Get instance: %s,err: %s", i, err)
			continue
		}
		var cs cursor
		err1 := json.Unmarshal(info, &cs)
		if err1 != nil {
			log.Println("canal zk cursor json Unmarshal failed,err: ", err1)
		}
		ch <- prometheus.MustNewConstMetric(
			canalzk_timestamp,
			prometheus.GaugeValue,
			float64(time.Now().Unix()*1000-int64(cs.Postion.Timestamp)),
			czk.Clustername, i,
		)
	}
}

func (c *collector) collect(ch chan<- prometheus.Metric, czk canalzk, wg *sync.WaitGroup) {
	defer wg.Done()
	zkclient, _, err := zk.Connect([]string{czk.Zookeeper}, c.timeout, func(c *zk.Conn) { c.SetLogger(zk.DefaultLogger) })
	defer zkclient.Close()
	if err != nil {
		log.Println("connect error: ", err)
		ch <- prometheus.MustNewConstMetric(
			canalzk_up,
			prometheus.GaugeValue,
			float64(1),
			czk.Clustername, czk.Zookeeper,
		)
		return
	}
	instances, _, err := zkclient.Children(path.Join(czk.Chroot, canalpath))
	if err != nil {
		log.Println("canal zk instance path failed,err:", err, path.Join(czk.Chroot, canalpath))
		return
	}
	for _, i := range instances {
		//canal instance cluster
		cl, _, err := zkclient.Children(path.Join(czk.Chroot, canalpath, i, "cluster"))
		if err != nil {
			ch <- prometheus.MustNewConstMetric(
				canalzk_cluster,
				prometheus.GaugeValue,
				float64(0),
				czk.Clustername, i,
			)
			log.Println(i, err)
			continue
		}
		ch <- prometheus.MustNewConstMetric(
			canalzk_cluster,
			prometheus.GaugeValue,
			float64(len(cl)),
			czk.Clustername, i,
		)
		//canal instance running metric, 0:normal,1:abnormal
		running := 0
		r, _, err := zkclient.Get(path.Join(czk.Chroot, canalpath, i, "running"))
		if err != nil {
			running = 1
			log.Println("canal zk path err: ", err, path.Join(czk.Chroot, canalpath, i, "running"), r)
		}
		ch <- prometheus.MustNewConstMetric(
			canalzk_running,
			prometheus.GaugeValue,
			float64(running),
			czk.Clustername, i,
		)
		//canal sync mysql cursor timestamp metric
		info, _, err := zkclient.Get(path.Join(czk.Chroot, canalpath, i, "1001/cursor"))
		if err != nil {
			log.Printf("canal zk Get instance: %s,err: %s", i, err)
			continue
		}
		var cs cursor
		err1 := json.Unmarshal(info, &cs)
		if err1 != nil {
			log.Println("canal zk cursor json Unmarshal failed,err: ", err1)
		}
		ch <- prometheus.MustNewConstMetric(
			canalzk_timestamp,
			prometheus.GaugeValue,
			float64(time.Now().Unix()*1000-int64(cs.Postion.Timestamp)),
			czk.Clustername, i,
		)
	}
}

func Newcollector(config string) (*collector, error) {
	cfg, err := ini.Load(config)
	if err != nil {
		log.Printf("Fail to read file:%v", err)
		os.Exit(1)
	}
	czks := []canalzk{}
	for _, sect := range cfg.SectionStrings() {
		c := canalzk{}
		if sect == "DEFAULT" {
			continue
		}
		c.Zookeeper = cfg.Section(sect).Key("zk").String()
		c.Chroot = cfg.Section(sect).Key("chroot").String()
		c.Clustername = cfg.Section(sect).Name()
		czks = append(czks, c)
	}
	return &collector{
		canalzk: czks,
		timeout: *zkTimeout,
		cfg:     cfg,
	}, nil
}

func init() {
	prometheus.MustRegister(version.NewCollector("canalzk_exporter"))
}

func main() {
	flag.Parse()
	file, err := os.OpenFile("canalzk.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		log.Panic(err)
	}
	writers := io.Writer(file)
	log.SetOutput(writers)
	canalzk_cluster = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "zk", "cluster"),
		"canal instance cluster host ",
		[]string{"jq", "destination"},
		nil,
	)
	canalzk_timestamp = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "zk", "timestamp"),
		"canal instance sync mysql gtid @timestamp",
		[]string{"jq", "destination"},
		nil,
	)
	canalzk_running = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "zk", "running"),
		"canal instance running host",
		[]string{"jq", "destination"},
		nil,
	)
	canalzk_up = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "zk", "up"),
		"canal zk host up",
		[]string{"jq", "zkhost"},
		nil,
	)
	exporter, _ := Newcollector("canal.ini")
	prometheus.MustRegister(exporter)
	http.Handle(metricsRoute, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`
		<html>
		<head><title>Kafka ZooKeeper Exporter</title></head>
		<body>
		<p><a href='` + metricsRoute + `'>Metrics</a></p>
		</body>
		</html>
		`))
	})
	log.Infoln("listening on %s", *listenAddress)
	s := &http.Server{
		Addr:         *listenAddress,
		ReadTimeout:  *serverTimeout,
		WriteTimeout: *serverTimeout,
	}
	log.Fatal(s.ListenAndServe())
}
