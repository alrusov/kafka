/*
kafka reader
*/
package reader

import (
	"fmt"
	"time"

	"github.com/alrusov/kafka"
	"github.com/alrusov/log"
	"github.com/alrusov/misc"
)

//----------------------------------------------------------------------------------------------------------------------------//

type (
	Handler interface {
		Processor(topic string, key string, data []byte) (err error)
		Commited(err error)
	}
)

var (
	Log = log.NewFacility("kafka.reader") // Log facility
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
			time.Sleep(kafkaCfg.Timeout) // don't use misc.Sleep!
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
			misc.Sleep(kafkaCfg.Timeout)
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

		if Log.CurrentLogLevel() >= log.TRACE4 {
			log.MessageWithSource(log.TRACE4, logSrc, "Received %s.%d: %s = %s", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.Key, m.Value)
		}

		err = handler.Processor(*m.TopicPartition.Topic, string(m.Key), m.Value)
		if err != nil {
			Log.MessageWithSource(log.ERR, logSrc, "processor: %s", err)
		} else {
			err = conn.Commit(m)
			if err != nil {
				Log.MessageWithSource(log.ERR, logSrc, "commit: %s", err)
			}
		}

		handler.Commited(err)
	}
}

//----------------------------------------------------------------------------------------------------------------------------//
