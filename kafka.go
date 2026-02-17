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
	"maps"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	kafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/alrusov/config"
	"github.com/alrusov/log"
	"github.com/alrusov/misc"
	"github.com/alrusov/panic"
)

//----------------------------------------------------------------------------------------------------------------------------//

type (
	// Конфигурация
	Config struct {
		Servers string `toml:"servers"` // Список kafka серверов

		User     string `toml:"user"`     // Пользователь
		Password string `toml:"password"` // Пароль

		Timeout config.Duration `toml:"timeout"` // Таймаут

		RetryTimeout         config.Duration `toml:"retry-timeout"`          // Таймаут повтора операции
		RetryBeforeReconnect int             `toml:"retry-before-reconnect"` // Количество ошибок подряд для пересоединения

		MaxRequestSize   int    `toml:"max-request-size"`   // Максимальный размер сообщения
		ConsumerQueueLen int    `toml:"consumer-queue-len"` // Длина внутренней очереди для консьюмера
		AutoCommit       bool   `toml:"auto-commit"`        // Использовать auto commit для консьюмера?
		Group            string `toml:"group"`              // Группа для консьюмера
		Acks             string `toml:"acks"`               // Producer acks
		Compression      string `toml:"compression"`        // Producer compression type

		ProducerTopics map[string]*ProducerTopicConfig `toml:"producer-topics"` // Список топиков продюсера с их параметрами map[virtualName]*config
		ConsumerTopics map[string]*ConsumerTopicConfig `toml:"consumer-topics"` // Список топиков консьюмера с их параметрами map[virtualName]*config

		// consumers
		consumersMutex sync.RWMutex
		consumers      []*Consumer
	}

	// Параметры топика продюсера
	ProducerTopicConfig struct {
		Active            bool            `toml:"active"`             // Активный?
		Type              string          `toml:"type"`               // Тип топика. Произвольное необязательное значение на усмотрение разработчика
		Encoding          string          `toml:"encoding"`           // Формат данных
		UsePartitions     []uint          `toml:"use-partitions"`     // Используемые партиции. Если пусто, то все. Если не пусто, то [сейчас] используется первая из списка
		NumPartitions     int             `toml:"num-partitions"`     // Количество партиций при создании
		ReplicationFactor int             `toml:"replication-factor"` // Фактор репликации при создании
		RetentionTime     config.Duration `toml:"retention-time"`     // Время жизни данных
		RetentionSize     int64           `toml:"retention-size"`     // Максимальный размер для очистки по размеру

		Extra any `toml:"extra"` // Произвольные дополнительные данные
	}

	// Параметры топика консьюмера
	ConsumerTopicConfig struct {
		Active        bool   `toml:"active"`         // Активный?
		Type          string `toml:"type"`           // Тип топика. Произвольное необязательное значение на усмотрение разработчика
		Encoding      string `toml:"encoding"`       // Формат данных
		UsePartitions []uint `toml:"use-partitions"` // Используемые партиции. Если пусто, то все
		Offset        Offset `toml:"offset"`         // Начальное смещение. Если 0, то не используем
		Extra         any    `toml:"extra"`          // Произвольные дополнительные данные
	}

	// Админский клиент
	AdminClient struct {
		cfg       *Config            // Конфигурация
		timeout   time.Duration      // Таймаут
		timeoutMS int                // Таймаут в МИЛЛИСЕКУНДАХ
		conn      *kafka.AdminClient // Соединение
	}

	// Продюсер
	Producer struct {
		sync.Mutex
		cfg       *Config         // Конфигурация
		timeout   time.Duration   // Таймаут
		timeoutMS int             // Таймаут в МИЛЛИСЕКУНДАХ
		conn      *kafka.Producer // Соединение
	}

	// консьюмер
	Consumer struct {
		sync.Mutex
		cfg             *Config         // Конфигурация
		timeout         time.Duration   // Таймаут
		timeoutMS       int             // Таймаут в МИЛЛИСЕКУНДАХ
		configMap       kafka.ConfigMap // Параметры соединения
		conn            *kafka.Consumer // Соединение
		initialAssigned bool            // Получен хотя бы один event AssignedPartitions
		initialCond     *sync.Cond
		partitions      AssignedPartitions
		eventHandlers   []EventHandler
	}

	AssignedPartitions     map[string]AssignedPartitionsList // [topic]
	AssignedPartitionsList []int

	// Метаданные
	Metadata = kafka.Metadata

	// Сообщение
	Message = kafka.Message

	// Набор сообщений
	Messages []Message

	// Смещение
	Offset = kafka.Offset

	// Ошибка
	Error kafka.Error

	TopicPartition  = kafka.TopicPartition
	TopicPartitions = kafka.TopicPartitions

	EventHandler func(c *Consumer, assigned bool, partitions TopicPartitions)
)

const (
	// Смешение - начало
	OffsetBeginning = Offset(kafka.OffsetBeginning)
	// Смещение - конец
	OffsetEnd = Offset(kafka.OffsetEnd)
	// Смещение - сохраненное в kafka
	OffsetStored = Offset(kafka.OffsetStored)

	// PartitionAny represents any partition (for partitioning),
	// or unspecified value (for all other cases)
	PartitionAny = kafka.PartitionAny
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
func (c *Config) Check(cfg any) (err error) {
	msgs := misc.NewMessages()
	defer msgs.Free()

	if c.Servers == "" {
		msgs.Add(`Undefined kafka.servers`)
	}

	if c.Timeout <= 0 {
		c.Timeout = config.ClientDefaultTimeout
	}

	if c.RetryTimeout <= 0 {
		c.RetryTimeout = config.ClientDefaultTimeout
	}

	if c.RetryBeforeReconnect <= 0 {
		c.RetryBeforeReconnect = 8
	}

	if c.MaxRequestSize <= 0 {
		c.MaxRequestSize = 1048576
	}

	if c.ConsumerQueueLen <= 0 {
		c.ConsumerQueueLen = 16
	}

	c.Acks, err = checkAcks(c.Acks, "1")
	if err != nil {
		msgs.Add("kafka.acks: %s", err)
	}

	if c.Compression == "" {
		c.Compression = "none"
	}
	validCompression := map[string]bool{
		"none": true, "gzip": true, "snappy": true,
		"lz4": true, "zstd": true,
	}
	if !validCompression[c.Compression] {
		return fmt.Errorf("invalid compression type: %s", c.Compression)
	}

	for key, topic := range c.ProducerTopics {
		if !topic.Active {
			delete(c.ProducerTopics, key)
			continue
		}

		err = topic.Check(cfg)
		if err != nil {
			msgs.Add("kafka.producer-topics[%s]: %s", key, err)
			continue
		}
	}

	for key, topic := range c.ConsumerTopics {
		if !topic.Active {
			delete(c.ConsumerTopics, key)
			continue
		}

		err = topic.Check(cfg)
		if err != nil {
			msgs.Add("kafka.consumer-topics[%s]: %s", key, err)
			continue
		}
	}

	return msgs.Error()
}

//----------------------------------------------------------------------------------------------------------------------------//

// Проверка валидности ProducerTopicConfig
func (c *ProducerTopicConfig) Check(cfg any) (err error) {
	msgs := misc.NewMessages()
	defer msgs.Free()

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
func (c *ConsumerTopicConfig) Check(cfg any) (err error) {
	msgs := misc.NewMessages()
	defer msgs.Free()

	return msgs.Error()
}

//----------------------------------------------------------------------------------------------------------------------------//

func checkAcks(src string, defVal string) (acks string, err error) {
	acks = src

	switch acks {
	case "all", "-1", "0", "1":
		return
	case "":
		acks = defVal
		return
	default:
		err = fmt.Errorf(`illegal value "%s"`, acks)
		return
	}
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
		(*config)["acks"] = c.Acks
		(*config)["message.max.bytes"] = c.MaxRequestSize
		(*config)["compression.type"] = c.Compression
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
		timeout:   c.Timeout.D(),
		timeoutMS: c.timeMS(),
		conn:      conn,
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

// Закрыть админское соединение
func (c *AdminClient) Close() {
	if c == nil || c.conn == nil {
		return
	}

	if misc.TEST {
		return
	}

	c.conn.Close()
	c.conn = nil
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
		kafka.SetAdminOperationTimeout(c.cfg.Timeout.D()),
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
		kafka.SetAdminOperationTimeout(c.cfg.Timeout.D()),
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
		timeout:   c.Timeout.D(),
		timeoutMS: c.timeMS(),
		conn:      conn,
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

// Закрыть продюсерское соединение
func (c *Producer) Close() {
	if c == nil || c.conn == nil {
		return
	}

	if misc.TEST {
		return
	}

	c.conn.Close()
	c.conn = nil
}

//----------------------------------------------------------------------------------------------------------------------------//

// Сохранить сообщения в kafka
func (c *Producer) SaveMessages(m Messages) (err error) {
	if misc.TEST {
		return nil
	}

	ln := 0

	if Log.CurrentLogLevel() >= log.TIME {
		t0 := misc.NowUTC()

		defer func() {
			misc.LogProcessingTime(Log.Name(), "", 0, "", fmt.Sprintf(`%d messages/%d bytes saved`, len(m), ln), t0.UnixNano())
		}()
	}

	msgs := misc.NewMessages()
	defer msgs.Free()

	ex := make(chan struct{})
	defer close(ex)

	c.Lock()

	go func() {
		panicID := panic.ID()
		defer panic.SaveStackToLogEx(panicID)

		for {
			select {
			case <-ex:
				c.Unlock()
				return
			case e := <-c.conn.Events():
				switch ev := e.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						msgs.Add("Delivery to %s failed: %s", *ev.TopicPartition.Topic, ev.TopicPartition.Error)
					}
				}
			}
		}
	}()

	for _, msg := range m {
		ln += len(msg.Value)
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
	return NewMessageEx(topic, PartitionAny, key, value)
}

func NewMessageEx(topic string, partition int32, key []byte, value []byte) Message {
	return Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: partition},
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
	configMap := c.makeConfigMap(true, extra)

	if !misc.TEST {
		conn, err = kafka.NewConsumer(configMap)
		if err != nil {
			return
		}
	}

	client = &Consumer{
		cfg:             c,
		timeout:         c.Timeout.D(),
		timeoutMS:       c.timeMS(),
		configMap:       *configMap,
		conn:            conn,
		initialAssigned: false,
		initialCond:     sync.NewCond(new(sync.Mutex)),
	}

	c.consumersMutex.Lock()
	if c.consumers == nil {
		c.consumers = make([]*Consumer, 0, 16)
	}
	c.consumers = append(c.consumers, client)
	c.consumersMutex.Unlock()

	return
}

func (c *Consumer) Reconnect() (err error) {
	if misc.TEST {
		return
	}

	c.Close()

	c.conn, err = kafka.NewConsumer(&c.configMap)
	if err != nil {
		return
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

// Закрыть консьюмерское соединение
func (c *Consumer) Close() {
	if c == nil || c.conn == nil {
		return
	}

	if misc.TEST {
		return
	}

	c.Unsubscribe()
	c.conn.Close()
	c.conn = nil
}

//----------------------------------------------------------------------------------------------------------------------------//

// Подписаться на топики по списку

func (c *Consumer) Subscribe(topics []string) (err error) {
	tp := make(TopicPartitions, len(topics))
	for i, topic := range topics {
		topic := topic
		tp[i] = kafka.TopicPartition{Topic: &topic, Partition: PartitionAny}
	}

	return c.SubscribeEx(tp)
}

func (c *Consumer) SubscribeEx(tp TopicPartitions) (err error) {
	return c.subscribeTopics(tp)
}

func (c *Consumer) subscribeTopics(tp TopicPartitions) (err error) {
	if misc.TEST {
		c.initialAssigned = true
		return nil
	}

	if len(tp) == 0 {
		c.assignedPartitions(true, nil)
		return
	}

	assignNeeded := false
	topics := make(map[string]bool, len(tp))
	for _, t := range tp {
		topics[*t.Topic] = true
		if t.Partition != PartitionAny {
			assignNeeded = true
		}
	}

	if assignNeeded {
		err = c.conn.Assign(tp)
		if err != nil {
			return
		}
		c.assignedPartitions(true, tp)
	} else {
		err = c.conn.SubscribeTopics(slices.Collect(maps.Keys(topics)),
			func(kc *kafka.Consumer, e kafka.Event) (err error) {
				Log.Message(log.INFO, `Event "%T" reached (%s)`, e, e.String())

				switch e := e.(type) {
				case kafka.TopicPartition:

				case kafka.AssignedPartitions:
					c.assignedPartitions(true, e.Partitions)

				case kafka.RevokedPartitions:
					c.assignedPartitions(false, e.Partitions)
				}
				return
			},
		)
		if err != nil {
			return
		}
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//

func (c *Consumer) assignedPartitions(assigned bool, partitions TopicPartitions) {
	c.Lock()

	defer func() {
		c.Unlock()

		for _, eh := range c.eventHandlers {
			eh(c, assigned, partitions)
		}
	}()

	m := make(map[string]map[int]bool, len(c.partitions)) // [topic][partition]active
	for topic, parts := range c.partitions {
		partsMap := make(map[int]bool, len(parts))
		for _, partNum := range parts {
			partsMap[partNum] = true
		}
		m[topic] = partsMap
	}

	for _, tp := range partitions {
		partsMap, exists := m[*tp.Topic]
		if !exists {
			if !assigned {
				continue
			}
			partsMap = make(map[int]bool, 128)
			m[*tp.Topic] = partsMap
		}

		partsMap[int(tp.Partition)] = assigned
	}

	c.partitions = make(AssignedPartitions, len(m))
	for topic, partsMap := range m {
		c.partitions[topic] = make(AssignedPartitionsList, 0, len(partsMap))
		for partNum, status := range partsMap {
			if !status {
				continue
			}
			c.partitions[topic] = append(c.partitions[topic], partNum)
		}

		sort.Ints(c.partitions[topic])
	}

	if assigned && !c.initialAssigned {
		c.initialAssigned = true
		c.initialCond.Broadcast()
	}
}

//----------------------------------------------------------------------------------------------------------------------------//

// Отписаться от всех подписок
func (c *Consumer) Unsubscribe() (err error) {
	if c == nil || c.conn == nil {
		return
	}

	if misc.TEST {
		return
	}

	c.conn.Unsubscribe()
	//c.conn.Unassign()
	return
}

//----------------------------------------------------------------------------------------------------------------------------//

// Ожидание получения первого AssignedPartitions
func (c *Consumer) WaitingForAssign() {
	c.initialCond.L.Lock()
	defer c.initialCond.L.Unlock()

	for !c.initialAssigned {
		c.initialCond.Wait()
	}
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

	if timeout <= 0 {
		timeout = c.timeout
	}

	m, err := c.conn.ReadMessage(timeout)
	if err != nil {
		if e, ok := err.(kafka.Error); ok {
			if e.Code() == kafka.ErrTimedOut {
				return nil, nil
			}
		}

		return nil, err
	}

	if m.TimestampType == kafka.TimestampNotAvailable {
		m.Timestamp = misc.NowUTC()
	}

	message = (*Message)(m)

	return

	/*
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
			if e.TopicPartition.Error != nil {
				return m, e.TopicPartition.Error
			}
			return m, nil

		case kafka.PartitionEOF:
			return nil, ErrPartitionEOF

		case kafka.Error:
			return nil, Error(e)

		default:
			return nil, fmt.Errorf("unknown error")
		}
	*/
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

func (c *Consumer) AddEventHandler(h EventHandler) {
	c.Lock()
	defer c.Unlock()

	c.delEventHandler(h)
	c.eventHandlers = append(c.eventHandlers, h)
}

func (c *Consumer) DelEventHandler(h EventHandler) {
	c.Lock()
	defer c.Unlock()

	c.delEventHandler(h)
}

func (c *Consumer) delEventHandler(h EventHandler) {
	hp := reflect.ValueOf(h).Pointer()
	for i, h2 := range c.eventHandlers {
		hp2 := reflect.ValueOf(h2).Pointer()
		if hp2 == hp {
			c.eventHandlers = slices.Delete(c.eventHandlers, i, 1)
			break
		}
	}
}

//----------------------------------------------------------------------------------------------------------------------------//

func (c *Config) GetAssignedPartitions() (parts []AssignedPartitions) {
	c.consumersMutex.RLock()
	defer c.consumersMutex.RUnlock()

	if len(c.consumers) == 0 {
		return
	}

	parts = make([]AssignedPartitions, 0, len(c.consumers))

	for _, cons := range c.consumers {
		parts = append(parts, cons.GetAssignedPartitions())
	}

	return
}

func (c *Consumer) GetAssignedPartitions() (parts AssignedPartitions) {
	c.Lock()
	defer c.Unlock()

	parts = make(AssignedPartitions, len(c.partitions))
	for topic, list := range c.partitions {
		parts[topic] = slices.Clone(list)
	}

	return
}

//----------------------------------------------------------------------------------------------------------------------------//
