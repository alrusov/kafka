/*
kafka reader
*/
package reader

import (
	"fmt"
	"maps"
	"slices"
	"strings"
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

	Reader struct {
		id       uint64
		active   bool
		wg       *sync.WaitGroup
		kafkaCfg *kafka.Config
		conn     *kafka.Consumer
		topics   kafka.TopicPartitions
		handler  Handler
	}
)

const (
	ActionRetry Action = iota
	ActionCommit
	ActionBreak
)

var (
	Log = log.NewFacility("kafka.reader") // Log facility

	lastReaderID = uint64(0)
	lastProcID   = uint64(0)
)

//----------------------------------------------------------------------------------------------------------------------------//

// Start

func Go(kafkaCfg *kafka.Config, consumerGroupID string, handler Handler, topics []string) (err error) {
	tp := make(kafka.TopicPartitions, len(topics))
	for i, topic := range topics {
		topic := topic
		tp[i] = kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}
	}

	_, err = GoEx(kafkaCfg, consumerGroupID, handler, tp)
	return
}

func GoEx(kafkaCfg *kafka.Config, consumerGroupID string, handler Handler, tp kafka.TopicPartitions) (reader *Reader, err error) {
	if len(tp) == 0 {
		// All consumer toipics
		tp = make(kafka.TopicPartitions, 0, len(kafkaCfg.ConsumerTopics))

		for topic := range kafkaCfg.ConsumerTopics {
			topic := topic
			p := kafka.PartitionAny
			tp = append(tp, kafka.TopicPartition{Topic: &topic, Partition: p})
		}
	}

	if len(tp) == 0 {
		err = fmt.Errorf("no consumer topics")
		return
	}

	kafkaCfg.Group = consumerGroupID

	conn, err := kafkaCfg.NewConsumer()
	if err != nil {
		return
	}

	wg := new(sync.WaitGroup)
	wg.Add(1)

	reader = &Reader{
		id:       atomic.AddUint64(&lastReaderID, 1),
		active:   true,
		wg:       wg,
		kafkaCfg: kafkaCfg,
		conn:     conn,
		topics:   tp,
		handler:  handler,
	}

	go reader.do()

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
			case <-time.After(time.Duration(kafkaCfg.RetryTimeout)):
				Log.Message(log.INFO, "Connection close timeout")
			}
		},
		nil,
	)

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func (rd *Reader) Stop() {
	rd.active = false
}

//----------------------------------------------------------------------------------------------------------------------------//

// Читатель
func (rd *Reader) do() {
	panicID := panic.ID()
	defer panic.SaveStackToLogEx(panicID)

	namesMap := make(map[string]bool, len(rd.topics))
	for _, t := range rd.topics {
		namesMap[*t.Topic] = true
	}
	names := slices.Collect(maps.Keys(namesMap))

	msgSrc := fmt.Sprintf("%d: %s", rd.id, strings.Join(names, ", "))

	Log.MessageWithSource(log.INFO, msgSrc, `Started`)

	defer func() {
		Log.MessageWithSource(log.INFO, msgSrc, `Stopped`)
		rd.wg.Done()
	}()

	go func() {
		panicID := panic.ID()
		defer panic.SaveStackToLogEx(panicID)

		misc.WaitingForStop()

		rd.conn.Unsubscribe()
		Log.MessageWithSource(log.DEBUG, msgSrc, `Unsubscribed`)

		// if will be enough time
		rd.conn.Close()
		Log.MessageWithSource(log.DEBUG, msgSrc, `Connection closed`)

	}()

	firstTime := true

	subscribe := func() {
		if !firstTime {
			rd.conn.Unsubscribe()
			misc.Sleep(time.Duration(rd.kafkaCfg.RetryTimeout))
		}
		firstTime = false

		Log.MessageWithSource(log.INFO, msgSrc, "Try to subscribe to %v", rd.topics)

		err := rd.conn.SubscribeEx(rd.topics)
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

		rd.conn.WaitingForAssign()
		rd.handler.Assigned(rd.conn, names)
	}()

	q := make(map[string]chan *kafka.Message, len(names))
	for _, topic := range names {
		topic := topic
		tq := make(chan *kafka.Message, rd.kafkaCfg.ConsumerQueueLen)
		q[topic] = tq
		go rd.topicHandler(topic, tq)
	}

	for misc.AppStarted() && rd.active {
		// reading with standard timeout
		m, err := rd.conn.Read(0)

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

		topic := *m.TopicPartition.Topic

		ch, exists := q[topic]
		if !exists {
			Log.MessageWithSource(log.ERR, msgSrc, `unknown topic "%s"`, topic)
			continue
		}

		ch <- m
	}

	for _, ch := range q {
		close(ch)
	}
}

//----------------------------------------------------------------------------------------------------------------------------//

func (rd *Reader) topicHandler(topic string, q chan *kafka.Message) {
	panicID := panic.ID()
	defer panic.SaveStackToLogEx(panicID)

	Log.Message(log.INFO, `Handler for topic "%s" started`, topic)
	defer Log.Message(log.INFO, `Handler for topic "%s" stopped`, topic)

	for misc.AppStarted() {
		id := atomic.AddUint64(&lastProcID, 1)

		m, more := <-q
		if !more {
			return
		}

		ll := Log.CurrentLogLevel()
		if ll >= log.TRACE4 {
			Log.Message(log.TRACE4, "[%s.%d] Received from %d: %s = (%d bytes) %s", topic, id, m.TopicPartition.Partition, m.Key, len(m.Value), m.Value)
		} else if ll >= log.TRACE1 {
			Log.Message(log.TRACE1, "[%s.%d] Received from %d: %s = (%d bytes)", topic, id, m.TopicPartition.Partition, m.Key, len(m.Value))
		}

		func() {
			doCommit := false

			defer func() {
				if doCommit {
					err := rd.conn.Commit(m)
					if err != nil {
						Log.Message(log.ERR, "[%s.%d] Commit: %s", topic, id, err)
					}
				}
			}()

			for misc.AppStarted() {
				action, err := rd.handler.Processor(id, *m.TopicPartition.Topic, m)
				if err != nil {
					Log.Message(log.ERR, "[%s.%d] Processor: %s", topic, id, err)
				}

				switch action {
				case ActionRetry:
					misc.Sleep(time.Duration(rd.kafkaCfg.RetryTimeout))
					continue

				case ActionCommit:
					doCommit = true
					return

				case ActionBreak:
					doCommit = false
					return

				default:
					Log.Message(log.ERR, "[%s.%d] SetResult returns unsupported Action=%d", topic, id, action)
					doCommit = true
					return
				}
			}
		}()
	}
}

//----------------------------------------------------------------------------------------------------------------------------//
