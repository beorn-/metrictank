package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/grafana/metrictank/cluster/partitioner"
	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/store/cassandra"
	storeCassandra "github.com/grafana/metrictank/store/cassandra"
	"github.com/raintank/schema"
	log "github.com/sirupsen/logrus"
)

var (
	dryRun            = flag.Bool("dry-run", true, "run in dry-run mode. No changes will be made.")
	logLevel          = flag.String("log-level", "info", "log level. panic|fatal|error|warning|info|debug")
	numPartitions     = flag.Int("num-partitions", 1, "number of partitions in cluster")
	partitionScheme   = flag.String("partition-scheme", "byOrg", "method used for partitioning metrics. (byOrg|bySeries)")
	months            = flag.Int64("months", 1, "number of months to go back for the queriesCount")
	schemaIdxFile     = flag.String("schema-idx-file", "/etc/metrictank/schema-idx-cassandra.toml", "File containing the needed schemas in case database needs initializing")
	schemaStorageFile = flag.String("schema-storage-file", "/etc/metrictank/storage-schemas.conf", "File containing the retention policy")
	srcCassAddr       = flag.String("src-cass-addr", "localhost", "Address of cassandra host to migrate from.")
	srcKeyspace       = flag.String("src-keyspace", "raintank", "Cassandra keyspace in use on source.")
	srcTable          = flag.String("src-table", "metric_idx", "Cassandra table name in use on source.")
	aggsFile          = flag.String("aggregation-file", "/etc/metrictank/storage-aggregation.conf", "File containing the aggregation configurations")
	windowFactor      = flag.Int("window-factor", 20, "size of compaction window relative to TTL")
	workers           = flag.Int("delete-workers", 1, "number of deletion workers")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "mt-data-prune")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Reads metric_idx_archive table and deletes metrics from the data tables")
		fmt.Fprintf(os.Stderr, "\nFlags:\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	// counts from 0 then --
	*months--

	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	lvl, err := log.ParseLevel(*logLevel)
	if err != nil {
		log.Fatalf("failed to parse log-level, %s", err.Error())
	}
	log.SetLevel(lvl)
	log.Infof("logging level set to '%s'", *logLevel)

	srcCluster := gocql.NewCluster(*srcCassAddr)
	srcCluster.Consistency = gocql.ParseConsistency("one")
	srcCluster.Timeout = 100 * time.Second
	srcCluster.NumConns = 20
	srcCluster.ProtoVersion = 4
	srcCluster.Keyspace = *srcKeyspace
	srcSession, err := srcCluster.CreateSession()
	if err != nil {
		log.Fatalf("failed to create cql session for source cassandra. %s", err.Error())
	}

	mdata.ConfigSetup()
	mdata.ConfigProcess()

	storeConfig := cassandra.NewStoreConfig()
	storeConfig.Addrs = *srcCassAddr
	storeConfig.Keyspace = *srcKeyspace
	storeConfig.Consistency = "one"
	storeConfig.Timeout = "100000"
	storeConfig.Retries = 3
	storeConfig.CqlProtocolVersion = 4
	storeConfig.SchemaFile = *schemaStorageFile

	cassandraStore, err := cassandra.NewCassandraStore(storeConfig, mdata.TTLs())
	if err != nil {
		log.Fatalf("Could not connect to cassandra store")
	}

	err = cassandraStore.FindExistingTables(*srcKeyspace)

	definitions := make(chan *schema.MetricDefinition, 100)
	tables := make(chan dataTable, 1000)

	go getDefs(srcSession, definitions, *srcTable)
	go findTables(srcSession, definitions, tables, mdata.Schemas)

	wg := sync.WaitGroup{}
	wg.Add(*workers)
	for w := 0; w < *workers; w++ {
		go func() {
			defer wg.Done()
			cleanTables(srcSession, tables, *months)
		}()
	}
	wg.Wait()
}

type dataTable struct {
	MetricID    schema.MKey
	Name        string
	Interval    uint32
	Retention   uint32
	Aggregators []string
}

func findTables(session *gocql.Session, definitions chan *schema.MetricDefinition, tables chan dataTable, schemas conf.Schemas) {
	log.Infof("starting data deletion thread for %d months back in time", months)

	aggList := []string{"cnt", "sum", "min", "max"}

	for def := range definitions {
		log.Debugf("########################################### %s #########################################", def.Id)
		schemaI, s := mdata.Schemas.Match(def.Name, def.Interval)
		log.Debugf("metric %q with interval %d gets schemaI %d\n", def.Name, def.Interval, schemaI)
		log.Debugf("## [%q] pattern=%q prio=%d retentions=%v\n", s.Name, s.Pattern, s.Priority, s.Retentions)

		for _, r := range s.Retentions {
			t := cassandra.GetTable(uint32(r.SecondsPerPoint*r.NumberOfPoints), *windowFactor, cassandra.Table_name_format)

			var table dataTable
			if r.SecondsPerPoint > 15 {
				table = dataTable{
					MetricID:    def.Id,
					Name:        t.Name,
					Interval:    uint32(r.SecondsPerPoint),
					Retention:   uint32(r.SecondsPerPoint * r.NumberOfPoints),
					Aggregators: aggList,
				}

			} else {
				table = dataTable{
					MetricID:  def.Id,
					Name:      t.Name,
					Interval:  uint32(r.SecondsPerPoint),
					Retention: uint32(r.SecondsPerPoint * r.NumberOfPoints),
				}
			}

			tables <- table
		}
	}
	close(tables)

}

func cleanTables(session *gocql.Session, tables chan dataTable, months int64) {
	pre := time.Now()
	currentMonth := time.Now().Unix() / storeCassandra.Month_sec
	firstMonth := currentMonth - months

	var queriesCount int
	var deletionsCount int
	for table := range tables {
		for month := firstMonth; month <= currentMonth; month++ {

			queriesCount++
			var keys []string
			if len(table.Aggregators) == 0 {
				keys = append(keys, fmt.Sprintf("%s_%d", table.MetricID, month))
			} else {
				for _, agg := range table.Aggregators {
					keys = append(keys, fmt.Sprintf("%s_%s_%d_%d", table.MetricID, agg, table.Interval, month))
				}
			}

			for _, key := range keys {
				if *dryRun {
					log.Debugf("dry-run: not deleting table %s key %s", table.Name, key)
					continue
				}

				attempts := 0
				log.Debugf("deleting table %s key %s", table.Name, key)
				for {
					err := session.Query(fmt.Sprintf(
						"DELETE FROM %s where key ='%s'",
						table.Name, key,
					)).Exec()
					if err == nil {
						deletionsCount++
						break
					}

					if (attempts % 20) == 0 {
						log.Warnf("Failed to execute CQL query. it will be retried. %s", err)
					}
					sleepTime := 100 * attempts
					if sleepTime > 2000 {
						sleepTime = 2000
					}
					time.Sleep(time.Duration(sleepTime) * time.Millisecond)
					attempts++
				}
			}

		}

	}
	log.Infof("Deleted %d dataseries generating %d queriesCount in %s", deletionsCount, queriesCount, time.Since(pre).String())

}

func getDefs(session *gocql.Session, definitions chan *schema.MetricDefinition, idxTable string) {
	log.Info("starting read thread")
	defer close(definitions)
	partitioner, err := partitioner.NewKafka(*partitionScheme)
	if err != nil {
		log.Fatalf("failed to initialize partitioner. %s", err.Error())
	}
	qry := fmt.Sprintf("SELECT id, orgid, partition, name, interval, unit, mtype, tags, lastupdate from %s", idxTable)
	log.Debugf("getDefs: query was: %s", qry)
	iter := session.Query(fmt.Sprintf(qry)).Iter()

	var id, name, unit, mtype string
	var orgId, interval int
	var partition int32
	var lastupdate int64
	var tags []string
	for iter.Scan(&id, &orgId, &partition, &name, &interval, &unit, &mtype, &tags, &lastupdate) {
		mkey, err := schema.MKeyFromString(id)
		if err != nil {
			log.Errorf("could not parse ID %q: %s -> skipping", id, err.Error())
			continue
		}
		mdef := schema.MetricDefinition{
			Id:         mkey,
			OrgId:      uint32(orgId),
			Partition:  partition,
			Name:       name,
			Interval:   interval,
			Unit:       unit,
			Mtype:      mtype,
			Tags:       tags,
			LastUpdate: lastupdate,
		}
		log.Debugf("retrieved %s from src index.", mdef.Id)
		if *numPartitions == 1 {
			mdef.Partition = 0
		} else {
			p, err := partitioner.Partition(&mdef, int32(*numPartitions))
			if err != nil {
				log.Fatalf("failed to get partition id of metric. %s", err.Error())
			} else {
				mdef.Partition = p
			}
		}
		definitions <- &mdef
	}
}
