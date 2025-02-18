package monitor

import (
	"context"
	"fmt"
	"sync"
	"time"

	client "github.com/influxdata/influxdb/client/v2"
	log "github.com/cihub/seelog"
	"github.com/sundy-li/burrowx/config"
)

type Importer struct {
	msgs       chan *ConsumerFullOffset
	cfg        *config.Config
	threshold  int
	maxTimeGap int64
	influxdb   client.Client
	stopped    chan struct{}
	
	// New fields for optimization
	ctx        context.Context
	cancel     context.CancelFunc
	batchSize  int
	metrics    struct {
		pointsProcessed int64
		batchesSent     int64
		errors          int64
	}
}

// Object pool for fields map reuse
var pointPool = sync.Pool{
	New: func() interface{} {
		return make(map[string]interface{}, 3)
	},
}

func NewImporter(cfg *config.Config) (i *Importer, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	
	i = &Importer{
		msgs:      make(chan *ConsumerFullOffset, 5000), // Increased buffer
		cfg:       cfg,
		threshold: 10,
		maxTimeGap: 10,
		stopped:   make(chan struct{}),
		ctx:       ctx,
		cancel:    cancel,
		batchSize: 100,
	}
	
	if err = i.ensureConnection(); err != nil {
		cancel()
		return nil, err
	}
	
	return i, nil
}

func (i *Importer) ensureConnection() error {
	if i.influxdb == nil {
		c, err := client.NewHTTPClient(client.HTTPConfig{
			Addr:     i.cfg.Influxdb.Hosts,
			Username: i.cfg.Influxdb.Username,
			Password: i.cfg.Influxdb.Pwd,
		})
		if err != nil {
			return fmt.Errorf("failed to create InfluxDB client: %v", err)
		}
		i.influxdb = c
	}
	return nil
}

func (i *Importer) createBatchPoints() (client.BatchPoints, error) {
	return client.NewBatchPoints(client.BatchPointsConfig{
		Database:  i.cfg.Influxdb.Db,
		Precision: "s",
	})
}

func (i *Importer) getFields() map[string]interface{} {
	fields := pointPool.Get().(map[string]interface{})
	for k := range fields {
		delete(fields, k)
	}
	return fields
}

func (i *Importer) putFields(fields map[string]interface{}) {
	pointPool.Put(fields)
}

func (i *Importer) start() {
	go func() {
		bp, err := i.createBatchPoints()
		if err != nil {
			log.Errorf("Failed to create batch points: %v", err)
			return
		}

		lastCommit := time.Now().Unix()
		ticker := time.NewTicker(time.Second * time.Duration(i.maxTimeGap))
		defer ticker.Stop()

		for {
			select {
			case <-i.ctx.Done():
				return
			case msg, ok := <-i.msgs:
				if !ok {
					// Channel closed, flush remaining points
					if len(bp.Points()) > 0 {
						if err := i.influxdb.Write(bp); err != nil {
							log.Errorf("Failed to write final batch: %v", err)
						}
					}
					i.stopped <- struct{}{}
					return
				}

				tags := map[string]string{
					"topic":          msg.Topic,
					"consumer_group": msg.Group,
					"cluster":        msg.Cluster,
				}

				for partition, entry := range msg.partitionMap {
					tags["partition"] = fmt.Sprintf("%d", partition)
					
					fields := i.getFields()
					fields["logsize"] = entry.Logsize
					fields["offsize"] = entry.Offset
					fields["lag"] = entry.Logsize - entry.Offset
					
					if entry.Offset < 0 {
						fields["lag"] = -1
						i.putFields(fields)
						continue
					}

					tm := time.Unix(msg.Timestamp/1000, 0)
					pt, err := client.NewPoint("consumer_metrics", tags, fields, tm)
					i.putFields(fields)
					
					if err != nil {
						log.Errorf("Error creating point: %v", err)
						i.metrics.errors++
						continue
					}

					bp.AddPoint(pt)
					i.metrics.pointsProcessed++
				}

				// Check if we should write the batch
				if len(bp.Points()) >= i.threshold {
					if err := i.writeBatch(&bp); err != nil {
						log.Errorf("Failed to write batch: %v", err)
						continue
					}
					lastCommit = time.Now().Unix()
				}

			case <-ticker.C:
				// Time-based batch write
				if len(bp.Points()) > 0 && time.Now().Unix()-lastCommit >= i.maxTimeGap {
					if err := i.writeBatch(&bp); err != nil {
						log.Errorf("Failed to write time-based batch: %v", err)
						continue
					}
					lastCommit = time.Now().Unix()
				}
			}
		}
	}()
}

func (i *Importer) writeBatch(bpPtr *client.BatchPoints) error {
	if err := i.ensureConnection(); err != nil {
		return err
	}

	if err := i.influxdb.Write(*bpPtr); err != nil {
		return err
	}

	newBp, err := i.createBatchPoints()
	if err != nil {
		return err
	}

	*bpPtr = newBp
	i.metrics.batchesSent++
	return nil
}

func (i *Importer) saveMsg(msg *ConsumerFullOffset) {
	select {
	case i.msgs <- msg:
		// Message sent successfully
	case <-i.ctx.Done():
		// Importer is stopping
		log.Warn("Importer is stopping, message dropped")
	}
}

func (i *Importer) stop() {
	i.cancel()  // Cancel context
	close(i.msgs)
	<-i.stopped
	if i.influxdb != nil {
		i.influxdb.Close()
	}
}

func (i *Importer) runCmd(cmd string) (res []client.Result, err error) {
	if err = i.ensureConnection(); err != nil {
		return nil, err
	}

	q := client.Query{
		Command:  cmd,
		Database: i.cfg.Influxdb.Db,
	}
	
	response, err := i.influxdb.Query(q)
	if err != nil {
		return nil, err
	}
	
	if response.Error() != nil {
		return nil, response.Error()
	}
	
	return response.Results, nil
}

func (i *Importer) GetMetrics() struct {
	PointsProcessed int64
	BatchesSent     int64
	Errors          int64
} {
	return struct {
		PointsProcessed int64
		BatchesSent     int64
		Errors          int64
	}{
		PointsProcessed: i.metrics.pointsProcessed,
		BatchesSent:     i.metrics.batchesSent,
		Errors:          i.metrics.errors,
	}
}


// package monitor

// import (
// 	"fmt"
// 	"time"

// 	client "github.com/influxdata/influxdb/client/v2"

// 	log "github.com/cihub/seelog"
// 	"github.com/sundy-li/burrowx/config"
// )

// type Importer struct {
// 	msgs chan *ConsumerFullOffset
// 	cfg  *config.Config

// 	threshold  int
// 	maxTimeGap int64
// 	influxdb   client.Client
// 	stopped    chan struct{}
// }

// func NewImporter(cfg *config.Config) (i *Importer, err error) {
// 	i = &Importer{
// 		msgs:       make(chan *ConsumerFullOffset, 1000),
// 		cfg:        cfg,
// 		threshold:  10,
// 		maxTimeGap: 10,
// 		stopped:    make(chan struct{}),
// 	}
// 	// Create a new HTTPClient
// 	c, err := client.NewHTTPClient(client.HTTPConfig{
// 		Addr:     cfg.Influxdb.Hosts,
// 		Username: cfg.Influxdb.Username,
// 		Password: cfg.Influxdb.Pwd,
// 	})
// 	if err != nil {
// 		return
// 	}
// 	i.influxdb = c
// 	return
// }

// func (i *Importer) start() {
// 	// _, err := i.runCmd("create database " + i.cfg.Influxdb.Db)
// 	// if err != nil {
// 	// 	panic(err)
// 	// }
// 	go func() {
// 		bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
// 			Database:  i.cfg.Influxdb.Db,
// 			Precision: "s",
// 		})
// 		lastCommit := time.Now().Unix()
// 		for msg := range i.msgs {
// 			tags := map[string]string{
// 				"topic":          msg.Topic,
// 				"consumer_group": msg.Group,
// 				"cluster":        msg.Cluster,
// 			}

// 			for partition, entry := range msg.partitionMap {
// 				//offset is the sql keyword, so we use offsize
// 				tags["partition"] = fmt.Sprintf("%d", partition)

// 				fields := map[string]interface{}{
// 					"logsize": entry.Logsize,
// 					"offsize": entry.Offset,
// 					"lag":     entry.Logsize - entry.Offset,
// 				}
// 				if entry.Offset < 0 {
// 					fields["lag"] = -1
// 					continue
// 				}

// 				tm := time.Unix(msg.Timestamp/1000, 0)
// 				pt, err := client.NewPoint("consumer_metrics", tags, fields, tm)
// 				if err != nil {
// 					log.Error("error in add point ", err.Error())
// 					continue
// 				}
// 				bp.AddPoint(pt)
// 			}

// 			if len(bp.Points()) > i.threshold || time.Now().Unix()-lastCommit >= i.maxTimeGap {
// 				err := i.influxdb.Write(bp)
// 				if err != nil {
// 					log.Error("error in insert points ", err.Error())
// 					continue
// 				}
// 				bp, _ = client.NewBatchPoints(client.BatchPointsConfig{
// 					Database:  i.cfg.Influxdb.Db,
// 					Precision: "s",
// 				})
// 				lastCommit = time.Now().Unix()
// 			}
// 		}
// 		i.stopped <- struct{}{}
// 	}()

// }

// func (i *Importer) saveMsg(msg *ConsumerFullOffset) {
// 	i.msgs <- msg
// }

// func (i *Importer) stop() {
// 	close(i.msgs)
// 	<-i.stopped
// }

// // runCmd method is for influxb querys
// func (i *Importer) runCmd(cmd string) (res []client.Result, err error) {
// 	q := client.Query{
// 		Command:  cmd,
// 		Database: i.cfg.Influxdb.Db,
// 	}
// 	if response, err := i.influxdb.Query(q); err == nil {
// 		if response.Error() != nil {
// 			return res, response.Error()
// 		}
// 		res = response.Results
// 	} else {
// 		return res, err
// 	}
// 	return res, nil

// }

