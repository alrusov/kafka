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

		TimeoutS string        `toml:"timeout"` // Строчное представление таймаута
		Timeout  time.Duration `toml:"-"`       // Таймаут

		RetryTimeoutS string        `toml:"retry-timeout"` // Строчное представление таймаута повторной отправки
		RetryTimeout  time.Duration `toml:"-"`             // Таймаут повторной отправки

		Group      string `toml:"group"`       // Группа для консьюмера
		AutoCommit bool   `toml:"auto-commit"` // Использовать auto commit для консьюмера?

		ProducerTopics map[string]*ProducerTopicConfig `toml:"producer-topics"` // Список топиков продюсера с их параметрами map[virtualName]*config
		ConsumerTopics map[string]*ConsumerTopicConfig `toml:"consumer-topics"` // Список топиков консьюмера с их параметрами map[virtualName]*config

		//		RevProducerTopics misc.StringMap `toml:"-"` // Обратное соответствие map[topicName]virtualName
		//		RevConsumerTopics misc.StringMap `toml:"-"` // Обратное соответствие map[topicName]virtualName
	}

	// Параметры топика продюсера
	ProducerTopicConfig struct {
		Active bool `toml:"active"` // Активный?

		NumPartitions     int `toml:"num-partitions"`     // Количество партиций при создании
		ReplicationFactor int `toml:"replication-factor"` // Фактор репликации при создании

		RetentionTimeS string        `toml:"retention-time"` // Строчное представление времени жизни данных
		RetentionTime  time.Duration `toml:"-"`              // Время жизни данных

		RetentionSize int64 `toml:"retention-size"` // Максимальный размер для очистки по размеру

		Extra misc.InterfaceMap `toml:"extra"` // Произвольные дополнительные данные
	}

	// Параметры топика консьюмера
	ConsumerTopicConfig struct {
		Active   bool   `toml:"active"`   // Активный?
		Encoding string `toml:"encoding"` // Формат данных

		Extra misc.InterfaceMap `toml:"extra"` // Произвольные дополнительные данные
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
		cfg       *Config         // Конфигурация
		timeoutMS int             // Таймаут в МИЛЛИСЕКУНДАХ
		conn      *kafka.Consumer // Соединение
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

	c.Timeout, err = misc.Interval2Duration(c.TimeoutS)
	if err != nil {
		msgs.Add(`kafka.timeout: %s`, err)
	}
	if c.Timeout <= 0 {
		c.Timeout = config.ClientDefaultTimeout
	}

	c.RetryTimeout, err = misc.Interval2Duration(c.RetryTimeoutS)
	if err != nil {
		msgs.Add(`kafka.retry-timeout: %s`, err)
	}
	if c.RetryTimeout <= 0 {
		c.RetryTimeout = c.Timeout
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

	c.RetentionTime, err = misc.Interval2Duration(c.RetentionTimeS)
	if err != nil {
		msgs.AddError(err)
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
func (c *Config) makeConfigMap(isConsumer bool) (config *kafka.ConfigMap) {
	config = &kafka.ConfigMap{
		"bootstrap.servers": c.Servers,
		"client.id":         c.User,
		"sasl.password":     c.Password,
	}

	if isConsumer {
		(*config)["group.id"] = c.Group
		(*config)["enable.auto.commit"] = c.AutoCommit
		(*config)["go.application.rebalance.enable"] = true
	} else {
		(*config)["compression.codec"] = "gzip"
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

// Привести таймаут к миллисекунды в конфиге
func (c *Config) timeMS() int {
	return timeMS(c.Timeout)
}

// Привести таймаут к миллисекунды
func timeMS(timeout time.Duration) int {
	return int(timeout / time.Millisecond)
}

//----------------------------------------------------------------------------------------------------------------------------//

// Создать новое админское соединение
func (c *Config) NewAdmin() (client *AdminClient, err error) {
	conn := (*kafka.AdminClient)(nil)

	if !misc.TEST {
		conn, err = kafka.NewAdminClient(c.makeConfigMap(false))
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
		kafka.SetAdminOperationTimeout(c.cfg.Timeout),
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
		kafka.SetAdminOperationTimeout(c.cfg.Timeout),
	)

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

// Создать новое продюсерское соединение
func (c *Config) NewProducer() (client *Producer, err error) {
	conn := (*kafka.Producer)(nil)

	if !misc.TEST {
		conn, err = kafka.NewProducer(c.makeConfigMap(false))
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
	conn := (*kafka.Consumer)(nil)

	if !misc.TEST {
		conn, err = kafka.NewConsumer(c.makeConfigMap(true))
		if err != nil {
			return
		}
	}

	client = &Consumer{
		cfg:       c,
		timeoutMS: c.timeMS(),
		conn:      conn,
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

// Закрыть консьюмерское соединение
func (c *Consumer) Close() {
	if misc.TEST {
		return
	}

	c.conn.Close()
}

//----------------------------------------------------------------------------------------------------------------------------//

// Подписаться на топики по списку
func (c *Consumer) Subscribe(topics []string) (err error) {
	if misc.TEST {
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
			Log.Message(log.DEBUG, `Event "%T" reached (%#v)`, e, e)
			switch e.(type) {
			case kafka.TopicPartition:
			case kafka.AssignedPartitions:
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
func (c *Consumer) Read(timeout time.Duration) (message *Message, err error) {
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
		return (*Message)(e), nil

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
