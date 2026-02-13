/*
kafka reader
*/
package reader

import (
	"context"
	"fmt"
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
		sync.Mutex
		ctx      context.Context
		cancel   context.CancelFunc
		id       uint64
		msgSrc   string
		active   atomic.Bool
		wg       *sync.WaitGroup
		kafkaCfg *kafka.Config
		conn     *kafka.Consumer
		topic    kafka.TopicPartition
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

func GoEx(kafkaCfg *kafka.Config, consumerGroupID string, handler Handler, tp kafka.TopicPartitions) (readers []*Reader, err error) {
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

	defer func() {
		if err != nil {
			// Закрыть все уже созданные readers
			for _, rd := range readers {
				rd.Stop()
				rd.Close()
			}
			readers = nil
		}
	}()

	for _, t := range tp {
		kafkaCfg.Group = consumerGroupID

		conn, e := kafkaCfg.NewConsumer()
		if e != nil {
			err = e
			return
		}

		wg := new(sync.WaitGroup)
		wg.Add(1)

		rd := &Reader{
			id:       atomic.AddUint64(&lastReaderID, 1),
			active:   atomic.Bool{},
			wg:       wg,
			kafkaCfg: kafkaCfg,
			conn:     conn,
			topic:    t,
			handler:  handler,
		}
		rd.active.Store(true)

		readers = append(readers, rd)

		go rd.do()

		name := *t.Topic
		misc.AddFinalizer(
			"kafka.reader."+name,
			func(_ int, name any) {
				ch := make(chan struct{})
				go func() {
					panicID := panic.ID()
					defer panic.SaveStackToLogEx(panicID)

					wg.Wait()
					close(ch)
				}()

				select {
				case <-ch:
					Log.Message(log.INFO, "[%s] Connection closed", name)
				case <-time.After(time.Duration(kafkaCfg.RetryTimeout)):
					Log.Message(log.INFO, "[%s] Connection close timeout", name)
				}
			},
			name,
		)
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func (rd *Reader) Stop() {
	rd.active.Store(false)
}

func (rd *Reader) Close() {
	rd.Lock()
	defer rd.Unlock()

	if rd.conn == nil {
		return
	}

	rd.conn.Unsubscribe()
	Log.MessageWithSource(log.DEBUG, rd.msgSrc, `Unsubscribed`)

	rd.conn.Close()
	Log.MessageWithSource(log.DEBUG, rd.msgSrc, `Closed`)

	rd.conn = nil
}

//----------------------------------------------------------------------------------------------------------------------------//

// Читатель
func (rd *Reader) do() {
	panicID := panic.ID()
	defer panic.SaveStackToLogEx(panicID)

	// Сейчас только один топик на читателя, но на всякий случай оставим возможность иметь несколько
	name := *rd.topic.Topic
	names := []string{name}

	rd.msgSrc = fmt.Sprintf("%d: %s", rd.id, strings.Join(names, ", "))
	rd.ctx, rd.cancel = context.WithCancel(context.Background())
	defer rd.cancel()

	Log.MessageWithSource(log.INFO, rd.msgSrc, `Started`)

	defer func() {
		Log.MessageWithSource(log.INFO, rd.msgSrc, `Stopped`)
		rd.wg.Done()
	}()

	go func() {
		panicID := panic.ID()
		defer panic.SaveStackToLogEx(panicID)

		// Ждем завершение приложения
		misc.WaitingForStop()

		// if will be enough time
		rd.Close()
		Log.MessageWithSource(log.DEBUG, rd.msgSrc, `Connection closed`)

	}()

	go func() {
		panicID := panic.ID()
		defer panic.SaveStackToLogEx(panicID)

		rd.conn.WaitingForAssign()
		rd.handler.Assigned(rd.conn, names)
	}()

	firstTime := true
	for {
		if !misc.AppStarted() {
			return
		}

		if err := rd.subscribe(firstTime); err == nil {
			break
		}
		firstTime = false

	}

	q := make(map[string]chan *kafka.Message, len(names))
	for _, topic := range names {
		topic := topic
		tq := make(chan *kafka.Message, rd.kafkaCfg.ConsumerQueueLen)
		q[topic] = tq
		go rd.topicHandler(topic, tq)
	}

	defer func() {
		for _, ch := range q {
			close(ch)
		}
	}()

	for misc.AppStarted() && rd.active.Load() {
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
				Log.MessageWithSource(log.ERR, rd.msgSrc, "read: %s", err)
				if err = rd.subscribe(false); err != nil {
					Log.MessageWithSource(log.ERR, rd.msgSrc, "subscribe: %s", err)
				}
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
			Log.MessageWithSource(log.ERR, rd.msgSrc, `unknown topic "%s"`, topic)
			continue
		}

		select {
		case ch <- m:
		case <-rd.ctx.Done():
			return
		}
	}
}

//----------------------------------------------------------------------------------------------------------------------------//

func (rd *Reader) subscribe(firstTime bool) (err error) {
	if !firstTime {
		rd.conn.Unsubscribe()
		select {
		case <-time.After(rd.kafkaCfg.RetryTimeout.D()):
		case <-rd.ctx.Done():
			return fmt.Errorf("cancelled")
		}
	}

	Log.MessageWithSource(log.INFO, rd.msgSrc, `Try to subscribe to "%s"`, rd.topic)

	err = rd.conn.SubscribeEx(kafka.TopicPartitions{rd.topic})
	if err != nil {
		return
	}

	Log.MessageWithSource(log.INFO, rd.msgSrc, "Subscription initiated")
	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func (rd *Reader) topicHandler(topic string, q chan *kafka.Message) {
	panicID := panic.ID()
	defer panic.SaveStackToLogEx(panicID)

	Log.Message(log.INFO, `Handler for topic "%s" started`, topic)
	defer Log.Message(log.INFO, `Handler for topic "%s" stopped`, topic)

	retryTimeout := rd.kafkaCfg.RetryTimeout.D()

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
					misc.Sleep(retryTimeout)
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
