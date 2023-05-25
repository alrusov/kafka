/*
kafka reader
*/
package reader

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alrusov/kafka/v2"
	"github.com/alrusov/log"
	"github.com/alrusov/misc"
	"github.com/alrusov/panic"
)

//----------------------------------------------------------------------------------------------------------------------------//

type (
	Handler interface {
		Assigned(conn *kafka.Consumer, topics []string)
		Processor(id uint64, topic string, m *kafka.Message) (action Action, err error)
	}

	Action int
)

const (
	ActionRetry Action = iota
	ActionCommit
	ActionBreak
)

var (
	Log = log.NewFacility("kafka.reader") // Log facility

	lastID = uint64(0)
)

//----------------------------------------------------------------------------------------------------------------------------//

// Start

func Go(kafkaCfg *kafka.Config, consumerGroupID string, handler Handler, topics ...string) (err error) {
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
		func(_ int, _ any) {
			ch := make(chan struct{})
			go func() {
				panicID := panic.ID()
				defer panic.SaveStackToLogEx(panicID)

				wg.Wait()
				close(ch)
			}()

			select {
			case <-ch:
				Log.Message(log.INFO, "All connections are closed")
			case <-time.After(time.Duration(kafkaCfg.Timeout)):
				Log.Message(log.INFO, "Connection close timeout")
			}
		},
		nil,
	)

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

// Читатель
func reader(wg *sync.WaitGroup, kafkaCfg *kafka.Config, conn *kafka.Consumer, topics []string, handler Handler) {
	panicID := panic.ID()
	defer panic.SaveStackToLogEx(panicID)

	msgSrc := topics[0]
	if len(topics) > 1 {
		msgSrc = msgSrc + " ..."
	}

	Log.MessageWithSource(log.INFO, msgSrc, `Started`)

	defer func() {
		Log.MessageWithSource(log.INFO, msgSrc, `Stopped`)
		wg.Done()
	}()

	go func() {
		misc.WaitingForStop()

		conn.Unsubscribe()
		Log.MessageWithSource(log.DEBUG, msgSrc, `Unsubscribed`)

		// if will be enough time
		conn.Close()
		Log.MessageWithSource(log.DEBUG, msgSrc, `Connection closed`)

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
		handler.Assigned(conn, topics)
	}()

	for misc.AppStarted() {
		// reading with standard timeout
		m, err := conn.Read(0)

		if !misc.AppStarted() {
			break
		}

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

		func() {
			doCommit := false

			defer func() {
				if doCommit {
					err = conn.Commit(m)
					if err != nil {
						Log.MessageWithSource(log.ERR, msgSrc, "[%d] Commit: %s", id, err)
					}
				}
			}()

			for misc.AppStarted() {
				action, err := handler.Processor(id, *m.TopicPartition.Topic, m)
				if err != nil {
					Log.MessageWithSource(log.ERR, msgSrc, "[%d] Processor: %s", id, err)
					doCommit = true
					return
				}

				switch action {
				case ActionRetry:
					misc.Sleep(time.Duration(kafkaCfg.RetryTimeout))
					continue

				case ActionCommit:
					doCommit = true
					return

				case ActionBreak:
					doCommit = false
					return

				default:
					log.MessageWithSource(log.ERR, msgSrc, "[%d] SetResult returns unsupported Action=%d", id, action)
					doCommit = true
					return
				}
			}
		}()
	}
}

//----------------------------------------------------------------------------------------------------------------------------//
