package mq

import (
    "log"
    "time"
    "github.com/streadway/amqp"
    "encoding/json"
)

type MessageQueue struct {
    url string
    name string
    Reconnect bool
    connection *amqp.Connection
    Channel *amqp.Channel
    queue amqp.Queue
}

func (mq *MessageQueue) NoReconnect() {
	mq.Reconnect = false
}

func (mq *MessageQueue) SetUrl(url string) {
	mq.url = url
}

func (mq *MessageQueue) Connect() bool {
	var err error
	// start with 2 seconds before retry
	var stepping = 2
	// try connection until it succeeds
	mq.connection, err = amqp.Dial(mq.url)
	for err != nil {
		log.Printf("Unable to connect to RabbitMQ.  Retrying in %d seconds...\n", stepping)
		log.Printf(err.Error())
		// wait a bit
    	time.Sleep(time.Duration(stepping) * time.Second)
    	// increase time between attempts
    	if stepping < 60 {
    		stepping = stepping * 2
    	}
    	if !mq.Reconnect {
    		return false
    	}
    	mq.connection, err = amqp.Dial(mq.url)
	}
	log.Printf("Connected to RabbitMQ")
	mq.Channel, err = mq.connection.Channel()
	FailOnError(err, "Failed to open a channel")

    mq.CreateQueues()

    return true
}

func (mq *MessageQueue) SendMatch(match []string) {
	data, _ := json.Marshal(match)
	err := mq.Channel.Publish(
		"",
		mq.name,
		false,
		false,
		amqp.Publishing {
			DeliveryMode: amqp.Persistent,
			ContentType: "application/json",
			Body: data,
		})
	FailOnError(err, "Unable to publish match")
}

func New(url string, name string) *MessageQueue {
    
    mq := new(MessageQueue)

    mq.SetUrl(url) // rabbitmq server
    mq.name = name
    mq.Reconnect = true

    return mq
}

func FailOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func (mq *MessageQueue) GetSubscriptionChannel() <-chan amqp.Delivery {
    msgs, err := mq.Channel.Consume(
		mq.queue.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	FailOnError(err, "Failed to register a consumer")
    return msgs
}

func (mq *MessageQueue) Close() {
    mq.Channel.Close();
    mq.connection.Close();
}

func (mq *MessageQueue) CreateQueues() {
	var err error
    log.Printf("Declaring queue '%s'", mq.name)
	mq.queue, err = mq.Channel.QueueDeclare(
	    mq.name, // name
		true,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	FailOnError(err, "Failed to declare recv queue")
}
