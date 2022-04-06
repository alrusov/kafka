/*
kafka reader
*/
package reader

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alrusov/kafka"
	"github.com/alrusov/log"
	"github.com/alrusov/misc"
	"github.com/alrusov/panic"
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
		Assigned(topics []string)
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

func (h *handlerWrapper) Assigned(topics []string) {
}

//----------------------------------------------------------------------------------------------------------------------------//

// Start

func Go(kafkaCfg *kafka.Config, consumerGroupID string, handler Handler, topics ...string) (err error) {
	return GoEx(kafkaCfg, consumerGroupID, &handlerWrapper{base: handler}, topics...)
}

func GoEx(kafkaCfg *kafka.Config, consumerGroupID string, handler HandlerEx, topics ...string) (err error) {
	if len(topics) == 0 {
		// All consumer toipics
		topics = make([]string, 0, len(kafkaCfg.ConsumerTopics))

		for topic := range kafkaCfg.ConsumerTopics {
			topics = append(topics, topic)
		}
	}

	if len(topics) == 0 {
		return fmt.Errorf("no consumer topics")
	}

	kafkaCfg.Group = consumerGroupID

	wg := new(sync.WaitGroup)

	start := func(topics []string) (err error) {
		conn, err := kafkaCfg.NewConsumer()
		if err != nil {
			return
		}

		wg.Add(1)
		go reader(wg, kafkaCfg, conn, topics, handler)
		return
	}

	if kafkaCfg.ConsumeInSeparateThreads {
		for _, topic := range topics {
			err = start([]string{topic})
			if err != nil {
				return
			}
		}
	} else {
		err = start(topics)
		if err != nil {
			return
		}
	}

	misc.AddExitFunc(
		"kafka.reader",
		func(_ int, _ interface{}) {
			ch := make(chan struct{})
			go func() {
				panicID := panic.ID()
				defer panic.SaveStackToLogEx(panicID)

				wg.Wait()
				close(ch)
			}()

			select {
			case <-ch:
				Log.Message(log.INFO, "Connections closed")
			case <-time.After(time.Duration(kafkaCfg.Timeout)):
				Log.Message(log.INFO, "Close connections timeout")
			}
		},
		nil,
	)

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

// Читатель
func reader(wg *sync.WaitGroup, kafkaCfg *kafka.Config, conn *kafka.Consumer, topics []string, handler HandlerEx) {
	panicID := panic.ID()
	defer panic.SaveStackToLogEx(panicID)

	msgSrc := kafkaCfg.Group
	if kafkaCfg.ConsumeInSeparateThreads {
		msgSrc = fmt.Sprintf("%s.%s", msgSrc, topics[0])
	}

	Log.MessageWithSource(log.INFO, msgSrc, `Started`)

	defer func() {
		conn.Close() // Delayed up to 10 seconds :(
		Log.MessageWithSource(log.INFO, msgSrc, `Stopped`)
		wg.Done()
	}()

	firstTime := true

	subscribe := func() {
		if !firstTime {
			conn.Unsubscribe()
			misc.Sleep(time.Duration(kafkaCfg.Timeout))
		}
		firstTime = false

		Log.MessageWithSource(log.INFO, msgSrc, "Try to subscribe to %v", topics)

		err := conn.Subscribe(topics)
		if err != nil {
			Log.MessageWithSource(log.ERR, msgSrc, "Subscribe: %s", err)
			return
		}

		Log.MessageWithSource(log.INFO, msgSrc, "Subscribe initiated")
	}

	subscribe()

	go func() {
		panicID := panic.ID()
		defer panic.SaveStackToLogEx(panicID)

		conn.WaitingForAssign()
		handler.Assigned(topics)
	}()

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
