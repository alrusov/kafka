/*
Работа с kafka

Используется библиотека github.com/confluentinc/confluent-kafka-go/kafka, основанная на нативном клиенте.

Так как под windows такового нет, то под ними работать не будет. Ну и не надо.
Но если приспичит, есть другие библиотеки, где клиент реализован на go. Но они, по очевидным причинам, сильно медленнее и прожорливее по памяти.
*/
package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/alrusov/config"
	"github.com/alrusov/log"
	"github.com/alrusov/misc"
)

//----------------------------------------------------------------------------------------------------------------------------//

type (
	// Конфигурация
	Config struct {
		Servers string `toml:"servers"` // Список kafka серверов

		User     string `toml:"user"`     // Пользователь
		Password string `toml:"password"` // Пароль

		Timeout config.Duration `toml:"timeout"` // Таймаут

		RetryTimeout config.Duration `toml:"retry-timeout"` // Таймаут повторной отправки

		MaxRequestSize int `toml:"max-request-size"` // Максимальный размер сообщения

		Group                    string                          `toml:"group"`                       // Группа для консьюмера
		AutoCommit               bool                            `toml:"auto-commit"`                 // Использовать auto commit для консьюмера?
		ConsumeInSeparateThreads bool                            `toml:"consume-in-separate-threads"` // Обрабатывать каждый топик в отдельном потоке
		ProducerTopics           map[string]*ProducerTopicConfig `toml:"producer-topics"`             // Список топиков продюсера с их параметрами map[virtualName]*config
		ConsumerTopics           map[string]*ConsumerTopicConfig `toml:"consumer-topics"`             // Список топиков консьюмера с их параметрами map[virtualName]*config
	}

	// Параметры топика продюсера
	ProducerTopicConfig struct {
		Active bool   `toml:"active"` // Активный?
		Type   string `toml:"type"`   // Тип топика. Произвольное необязательное значение на умотрение разработчика

		NumPartitions     int `toml:"num-partitions"`     // Количество партиций при создании
		ReplicationFactor int `toml:"replication-factor"` // Фактор репликации при создании

		RetentionTime config.Duration `toml:"retention-time"` // Время жизни данных

		RetentionSize int64 `toml:"retention-size"` // Максимальный размер для очистки по размеру

		Extra interface{} `toml:"extra"` // Произвольные дополнительные данные
	}

	// Параметры топика консьюмера
	ConsumerTopicConfig struct {
		Active   bool   `toml:"active"`   // Активный?
		Type     string `toml:"type"`     // Тип топика. Произвольное необязательное значение на умотрение разработчика
		Encoding string `toml:"encoding"` // Формат данных

		Extra interface{} `toml:"extra"` // Произвольные дополнительные данные
	}

	// Админский клиент
	AdminClient struct {
		cfg       *Config            // Конфигурация
		timeoutMS int                // Таймаут в МИЛЛИСЕКУНДАХ
		conn      *kafka.AdminClient // Соединение
	}

	// Продюсер
	Producer struct {
		cfg       *Config         // Конфигурация
		timeoutMS int             // Таймаут в МИЛЛИСЕКУНДАХ
		conn      *kafka.Producer // Соединение
	}

	// консьюмер
	Consumer struct {
		cfg             *Config         // Конфигурация
		timeoutMS       int             // Таймаут в МИЛЛИСЕКУНДАХ
		conn            *kafka.Consumer // Соединение
		initialAssigned bool            // Получен хотя бы один event AssignedPartitions
		initialCond     *sync.Cond
	}

	// Метаданные
	Metadata kafka.Metadata

	// Сообщение
	Message kafka.Message

	// Набор сообщений
	Messages []Message

	// Смещение
	Offset kafka.Offset

	// Ошибка
	Error kafka.Error
)

const (
	// Смешение - начало
	OffsetBeginning = Offset(kafka.OffsetBeginning)
	// Смещение - конец
	OffsetEnd = Offset(kafka.OffsetEnd)
	// Смещение - сохраненное в kafka
	OffsetStored = Offset(kafka.OffsetStored)
)

var (
	// Log facility
	Log = log.NewFacility("kafka")

	// Ошибка - конец данных
	ErrPartitionEOF = errors.New("partition EOF")
)

//----------------------------------------------------------------------------------------------------------------------------//

// Версия нативной библиотки
func LibraryVersion() (version string) {
	_, version = kafka.LibraryVersion()
	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func (e Error) Error() string {
	return kafka.Error(e).String()
}

//----------------------------------------------------------------------------------------------------------------------------//

// Проверка валидности Config
func (c *Config) Check(cfg interface{}) (err error) {
	msgs := misc.NewMessages()

	if c.Servers == "" {
		msgs.Add(`Undefined kafka.servers`)
	}

	if c.Timeout <= 0 {
		c.Timeout = config.ClientDefaultTimeout
	}

	if c.RetryTimeout <= 0 {
		c.RetryTimeout = c.Timeout
	}

	if c.MaxRequestSize <= 0 {
		c.MaxRequestSize = 1048576
	}

	for key, topic := range c.ProducerTopics {
		err = topic.Check(cfg)
		if err != nil {
			msgs.Add("kafka.producer-topics[%s]: %s", key, err)
			continue
		}

		if !topic.Active {
			delete(c.ProducerTopics, key)
			continue
		}
	}

	for key, topic := range c.ConsumerTopics {
		err = topic.Check(cfg)
		if err != nil {
			msgs.Add("kafka.consumer-topics[%s]: %s", key, err)
			continue
		}

		if !topic.Active {
			delete(c.ConsumerTopics, key)
			continue
		}
	}

	return msgs.Error()
}

//----------------------------------------------------------------------------------------------------------------------------//

// Проверка валидности ProducerTopicConfig
func (c *ProducerTopicConfig) Check(cfg interface{}) (err error) {
	msgs := misc.NewMessages()

	if c.NumPartitions <= 0 {
		c.NumPartitions = 1
	}

	if c.ReplicationFactor <= 0 {
		c.ReplicationFactor = 1
	}

	if c.RetentionTime <= 0 {
		c.RetentionTime = -1
	}

	if c.RetentionSize <= 0 {
		c.RetentionSize = -1
	}

	return msgs.Error()
}

//----------------------------------------------------------------------------------------------------------------------------//

// Проверка валидности ProducerTopicConfig
func (c *ConsumerTopicConfig) Check(cfg interface{}) (err error) {
	msgs := misc.NewMessages()

	return msgs.Error()
}

//----------------------------------------------------------------------------------------------------------------------------//

// Создать набор параметров для соединения
func (c *Config) makeConfigMap(isConsumer bool, extra misc.InterfaceMap) (config *kafka.ConfigMap) {
	config = &kafka.ConfigMap{
		"bootstrap.servers": c.Servers,
		"client.id":         c.User,
		"sasl.password":     c.Password,
	}

	if isConsumer {
		(*config)["group.id"] = c.Group
		(*config)["enable.auto.commit"] = c.AutoCommit
		(*config)["go.application.rebalance.enable"] = true
		(*config)["max.partition.fetch.bytes"] = c.MaxRequestSize
		(*config)["fetch.max.bytes"] = c.MaxRequestSize
	} else {
		(*config)["message.max.bytes"] = c.MaxRequestSize
		(*config)["compression.codec"] = "gzip"
	}

	for n, v := range extra {
		(*config)[n] = v
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

// Привести таймаут к миллисекунды в конфиге
func (c *Config) timeMS() int {
	return timeMS(c.Timeout)
}

// Привести таймаут к миллисекунды
func timeMS(timeout config.Duration) int {
	return int(timeout / config.Duration(time.Millisecond))
}

//----------------------------------------------------------------------------------------------------------------------------//

// Создать новое админское соединение
func (c *Config) NewAdmin() (client *AdminClient, err error) {
	return c.NewAdminEx(nil)
}

func (c *Config) NewAdminEx(extra misc.InterfaceMap) (client *AdminClient, err error) {
	conn := (*kafka.AdminClient)(nil)

	if !misc.TEST {
		conn, err = kafka.NewAdminClient(c.makeConfigMap(false, extra))
		if err != nil {
			return
		}
	}

	client = &AdminClient{
		cfg:       c,
		timeoutMS: c.timeMS(),
		conn:      conn,
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

// Закрыть админское соединение
func (c *AdminClient) Close() {
	if misc.TEST {
		return
	}

	c.conn.Close()
}

//----------------------------------------------------------------------------------------------------------------------------//

// Получить метаданные для топика. Если передано пустое имя, то всех.
func (c *AdminClient) GetMetadata(topic string) (m *Metadata, err error) {
	if misc.TEST {
		return &Metadata{}, nil
	}

	pTopic := &topic
	allTopics := topic == ""
	if allTopics {
		pTopic = nil
	}

	km, err := c.conn.GetMetadata(pTopic, allTopics, c.timeoutMS)
	if err != nil {
		return
	}

	m = (*Metadata)(km)
	return
}

//----------------------------------------------------------------------------------------------------------------------------//

// Создать топик
func (c *AdminClient) CreateTopic(name string, topic *ProducerTopicConfig) (err error) {
	if misc.TEST {
		return nil
	}

	config := make(misc.StringMap, 16)
	if topic.RetentionTime > 0 {
		config["retention.ms"] = strconv.FormatInt(int64(timeMS(topic.RetentionTime)), 10)
	}
	if topic.RetentionSize > 0 {
		config["retention.bytes"] = strconv.FormatInt(topic.RetentionSize, 10)
	}

	_, err = c.conn.CreateTopics(
		context.Background(),
		[]kafka.TopicSpecification{
			{
				Topic:             name,
				NumPartitions:     topic.NumPartitions,
				ReplicationFactor: topic.ReplicationFactor,
				Config:            config,
			},
		},
		kafka.SetAdminOperationTimeout(time.Duration(c.cfg.Timeout)),
	)

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

// Удалить топик
func (c *AdminClient) DeleteTopic(topic string) (err error) {
	if misc.TEST {
		return nil
	}

	_, err = c.conn.DeleteTopics(
		context.Background(),
		[]string{
			topic,
		},
		kafka.SetAdminOperationTimeout(time.Duration(c.cfg.Timeout)),
	)

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

// Создать новое продюсерское соединение
func (c *Config) NewProducer() (client *Producer, err error) {
	return c.NewProducerEx(nil)
}

func (c *Config) NewProducerEx(extra misc.InterfaceMap) (client *Producer, err error) {
	conn := (*kafka.Producer)(nil)

	if !misc.TEST {
		conn, err = kafka.NewProducer(c.makeConfigMap(false, extra))
		if err != nil {
			return
		}
	}

	client = &Producer{
		cfg:       c,
		timeoutMS: c.timeMS(),
		conn:      conn,
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

// Закрыть продюсерское соединение
func (c *Producer) Close() {
	if misc.TEST {
		return
	}

	c.conn.Close()
}

//----------------------------------------------------------------------------------------------------------------------------//

// Сохранить сообщения в kafka
func (c *Producer) SaveMessages(m Messages) (err error) {
	if misc.TEST {
		return nil
	}

	msgs := misc.NewMessages()

	go func() {
		for e := range c.conn.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					msgs.Add("Delivery to %s failed: %s", *ev.TopicPartition.Topic, ev.TopicPartition.Error)
				}
			}
		}
	}()

	for _, msg := range m {
		msg := msg
		err := c.conn.Produce((*kafka.Message)(&msg), nil)
		if err != nil {
			msgs.AddError(err)
		}

	}

	n := c.conn.Flush(c.timeoutMS)
	if n != 0 {
		c.conn.Purge(kafka.PurgeQueue | kafka.PurgeInFlight | kafka.PurgeNonBlocking) // будем делать повторы самостоятельно
		msgs.Add("%d events still un-flushed", n)
	}

	err = msgs.Error()
	return
}

//----------------------------------------------------------------------------------------------------------------------------//

// Создать сообщение
func NewMessage(topic string, key []byte, value []byte) Message {
	return Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          value,
	}
}

//----------------------------------------------------------------------------------------------------------------------------//

// Создать новое консьюмерское соединение
func (c *Config) NewConsumer() (client *Consumer, err error) {
	return c.NewConsumerEx(nil)
}

func (c *Config) NewConsumerEx(extra misc.InterfaceMap) (client *Consumer, err error) {
	conn := (*kafka.Consumer)(nil)

	if !misc.TEST {
		conn, err = kafka.NewConsumer(c.makeConfigMap(true, extra))
		if err != nil {
			return
		}
	}

	client = &Consumer{
		cfg:             c,
		timeoutMS:       c.timeMS(),
		conn:            conn,
		initialAssigned: false,
		initialCond:     sync.NewCond(new(sync.Mutex)),
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

// Закрыть консьюмерское соединение
func (c *Consumer) Close() {
	if misc.TEST {
		return
	}

	c.conn.Unsubscribe()
	c.conn.Close()
}

//----------------------------------------------------------------------------------------------------------------------------//

// Подписаться на топики по списку
func (c *Consumer) Subscribe(topics []string) (err error) {
	if misc.TEST {
		c.initialAssigned = true
		return nil
	}

	list := make([]kafka.TopicPartition, len(topics))

	for i, topic := range topics {
		topic := topic
		list[i] = kafka.TopicPartition{
			Topic: &topic,
		}
	}

	return c.subscribeTopics(topics)
}

func (c *Consumer) subscribeTopics(topics []string) (err error) {
	c.conn.SubscribeTopics(topics,
		func(kc *kafka.Consumer, e kafka.Event) (err error) {
			Log.Message(log.DEBUG, `Event "%T" reached (%s)`, e, e.String())

			switch e.(type) {
			case kafka.TopicPartition:

			case kafka.AssignedPartitions:
				c.initialCond.L.Lock()
				if !c.initialAssigned {
					c.initialAssigned = true
					c.initialCond.Broadcast()
				}
				c.initialCond.L.Unlock()

			case kafka.RevokedPartitions:
			}
			return
		},
	)

	return
}

// Отписаться от всех подписок
func (c *Consumer) Unsubscribe() (err error) {
	return c.conn.Unsubscribe()
}

// Ожидание получения первого AssignedPartitions
func (c *Consumer) WaitingForAssign() {
	c.initialCond.L.Lock()

	for !c.initialAssigned {
		c.initialCond.Wait()
	}

	c.initialCond.L.Unlock()
}

//----------------------------------------------------------------------------------------------------------------------------//

// Получить текущие смещения для списка топиков
func (c *Consumer) Offsets(topics []string) (offsets []Offset, err error) {
	if misc.TEST {
		return make([]Offset, len(topics)), nil
	}

	list := make([]kafka.TopicPartition, len(topics))

	for i, topic := range topics {
		topic := topic
		list[i] = kafka.TopicPartition{
			Topic: &topic,
		}
	}

	tp, err := c.conn.Committed(list, c.timeoutMS)
	if err != nil {
		return
	}

	offsets = make([]Offset, len(tp))

	for i, t := range tp {
		offsets[i] = Offset(t.Offset)
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

// Установить указатель чтения для топика
func (c *Consumer) Seek(topic string, offset Offset) (err error) {
	if misc.TEST {
		return nil
	}

	return c.conn.Seek(
		kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Offset:    kafka.Offset(offset),
		},
		c.timeoutMS,
	)
}

//----------------------------------------------------------------------------------------------------------------------------//

// Получить сообщение из топика, если оно там есть
func (c *Consumer) Read(timeout config.Duration) (message *Message, err error) {
	if misc.TEST {
		return nil, nil
	}

	tMS := c.timeoutMS

	if timeout > 0 {
		tMS = timeMS(timeout)
	}

	ev := c.conn.Poll(tMS)
	if ev == nil {
		// Ничего нет
		return nil, nil
	}

	switch e := ev.(type) {
	case *kafka.Message:
		m := (*Message)(e)
		if m.TimestampType == kafka.TimestampNotAvailable {
			m.Timestamp = misc.NowUTC()
		}
		return m, nil

	case kafka.PartitionEOF:
		return nil, ErrPartitionEOF

	case kafka.Error:
		return nil, Error(e)

	default:
		return nil, fmt.Errorf("unknown error")
	}
}

//----------------------------------------------------------------------------------------------------------------------------//

// Зафиксировать последнюю прочитанную позицию для топика
func (c *Consumer) Commit(message *Message) (err error) {
	if misc.TEST {
		return nil
	}

	_, err = c.conn.CommitMessage((*kafka.Message)(message))
	return
}

//----------------------------------------------------------------------------------------------------------------------------//
