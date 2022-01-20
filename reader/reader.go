/*
kafka reader
*/
package reader

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/alrusov/kafka"
	"github.com/alrusov/log"
	"github.com/alrusov/misc"
)

//----------------------------------------------------------------------------------------------------------------------------//

type (
	Handler interface {
		Processor(id uint64, topic string, key string, data []byte) (err error)
		SetResult(id uint64, err error) (doRetry bool)
	}
)

var (
	Log = log.NewFacility("kafka.reader") // Log facility

	lastID = uint64(0)
)

//----------------------------------------------------------------------------------------------------------------------------//

// Start
func Go(kafkaCfg *kafka.Config, consumerGroupID string, handler Handler) (err error) {
	if len(kafkaCfg.ConsumerTopics) == 0 {
		return fmt.Errorf("no consumer topics defined")
	}

	kafkaCfg.Group = consumerGroupID

	// make consumer
	conn, err := kafkaCfg.NewConsumer()
	if err != nil {
		return
	}

	go reader(kafkaCfg, conn, handler)

	misc.AddExitFunc(
		"kafka.reader",
		func(_ int, _ interface{}) {
			time.Sleep(time.Duration(kafkaCfg.Timeout)) // don't use misc.Sleep!
			Log.Message(log.INFO, "Connection closed")
		},
		nil,
	)

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

// Читатель
func reader(kafkaCfg *kafka.Config, conn *kafka.Consumer, handler Handler) {
	logSrc := kafkaCfg.Group
	Log.MessageWithSource(log.INFO, logSrc, `Started`)

	defer func() {
		conn.Close() // Delayed up to 10 seconds :(
		Log.MessageWithSource(log.INFO, logSrc, `Stopped`)
	}()

	// Topics list
	topics := make([]string, 0, len(kafkaCfg.ConsumerTopics))

	for name := range kafkaCfg.ConsumerTopics {
		topics = append(topics, name)
	}

	firstTime := true

	subscribe := func() {
		if !firstTime {
			firstTime = false
			conn.Unsubscribe()
			misc.Sleep(time.Duration(kafkaCfg.Timeout))
		}

		Log.MessageWithSource(log.INFO, logSrc, "Try to subscribe to %v", topics)

		err := conn.Subscribe(topics)
		if err != nil {
			Log.MessageWithSource(log.ERR, logSrc, "Subscribe: %s", err)
			return
		}

		Log.MessageWithSource(log.INFO, logSrc, "Subscribe OK")
	}

	subscribe()

	for {
		if !misc.AppStarted() {
			return
		}

		// reading with standard timeout
		m, err := conn.Read(0)

		if err != nil {
			switch err {
			case kafka.ErrPartitionEOF:
				// PartitionEOF consumer reached end of partition
				// Needs to be explicitly enabled by setting the `enable.partition.eof`
				// configuration property to true.
				fallthrough
			default:
				Log.MessageWithSource(log.ERR, logSrc, "read: %s", err)
				subscribe()
			}
			continue
		}

		if m == nil {
			// nothing to do
			continue
		}

		id := atomic.AddUint64(&lastID, 1)

		if Log.CurrentLogLevel() >= log.TRACE4 {
			log.MessageWithSource(log.TRACE4, logSrc, "[%d] Received %s.%d: %s = %s", id, *m.TopicPartition.Topic, m.TopicPartition.Partition, m.Key, m.Value)
		}

		var doRetry bool

		for {
			err = handler.Processor(id, *m.TopicPartition.Topic, string(m.Key), m.Value)
			if err != nil {
				Log.MessageWithSource(log.ERR, logSrc, "[%d] Processor: %s", id, err)
			} else {
				err = conn.Commit(m)
				if err != nil {
					Log.MessageWithSource(log.ERR, logSrc, "[%d] Commit: %s", id, err)
				}
			}

			doRetry = handler.SetResult(id, err)
			if !doRetry {
				break
			}

			misc.Sleep(time.Duration(kafkaCfg.RetryTimeout))
		}
	}
}

//----------------------------------------------------------------------------------------------------------------------------//
