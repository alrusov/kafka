/*
kafka reader
*/
package reader

import (
	"context"
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

	HandlerEx interface {
		Processor(id uint64, topic string, m *kafka.Message) (ctx context.Context, err error)
		SetResult(ctx context.Context, id uint64, err error) (doRetry bool)
	}
)

var (
	Log = log.NewFacility("kafka.reader") // Log facility

	lastID = uint64(0)
)

//----------------------------------------------------------------------------------------------------------------------------//

type handlerWrapper struct {
	base Handler
}

func (h *handlerWrapper) Processor(id uint64, topic string, m *kafka.Message) (ctx context.Context, err error) {
	err = h.base.Processor(id, topic, string(m.Key), m.Value)
	return nil, err
}

func (h *handlerWrapper) SetResult(ctx context.Context, id uint64, err error) (doRetry bool) {
	return h.base.SetResult(id, err)
}

//----------------------------------------------------------------------------------------------------------------------------//

// Start

func Go(kafkaCfg *kafka.Config, consumerGroupID string, handler Handler) (err error) {
	return GoEx(kafkaCfg, consumerGroupID, &handlerWrapper{base: handler})
}

func GoEx(kafkaCfg *kafka.Config, consumerGroupID string, handler HandlerEx) (err error) {
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
func reader(kafkaCfg *kafka.Config, conn *kafka.Consumer, handler HandlerEx) {
	msgSrc := kafkaCfg.Group

	Log.MessageWithSource(log.INFO, msgSrc, `Started`)

	defer func() {
		conn.Close() // Delayed up to 10 seconds :(
		Log.MessageWithSource(log.INFO, msgSrc, `Stopped`)
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

		Log.MessageWithSource(log.INFO, msgSrc, "Try to subscribe to %v", topics)

		err := conn.Subscribe(topics)
		if err != nil {
			Log.MessageWithSource(log.ERR, msgSrc, "Subscribe: %s", err)
			return
		}

		Log.MessageWithSource(log.INFO, msgSrc, "Subscribe OK")
	}

	subscribe()

	for misc.AppStarted() {
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
				Log.MessageWithSource(log.ERR, msgSrc, "read: %s", err)
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
			log.MessageWithSource(log.TRACE4, msgSrc, "[%d] Received %s.%d: %s = %s", id, *m.TopicPartition.Topic, m.TopicPartition.Partition, m.Key, m.Value)
		}

		var doRetry bool
		var ctx context.Context

		for misc.AppStarted() {
			ctx, err = handler.Processor(id, *m.TopicPartition.Topic, m)
			if err != nil {
				Log.MessageWithSource(log.ERR, msgSrc, "[%d] Processor: %s", id, err)
			}

			doRetry = handler.SetResult(ctx, id, err)
			if !doRetry {
				err = conn.Commit(m)
				if err != nil {
					Log.MessageWithSource(log.ERR, msgSrc, "[%d] Commit: %s", id, err)
				}
				break
			}

			misc.Sleep(time.Duration(kafkaCfg.RetryTimeout))
		}
	}
}

//----------------------------------------------------------------------------------------------------------------------------//
