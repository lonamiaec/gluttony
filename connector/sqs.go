package connector

import (
	"errors"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/sqs"
	"strings"
	"time"
)

type sqsConnector struct {
	connData        *ConnectorData
	consumerId      string
	deliveryChan    chan []byte
	conn            *sqs.SQS
	queues          []string
	workingMessages map[string]sqs.Message
	messagesQueues  map[string]string
}

func NewSQSConnector(connData *ConnectorData) *sqsConnector {
	var auth = aws.Auth{
		AccessKey: " ",
		SecretKey: " ",
	}

	sqsConsumer := &sqsConnector{}
	sqsConsumer.connData = connData
	sqsConsumer.conn = sqs.New(auth, aws.EUWest)
	sqsConsumer.workingMessages = make(map[string]sqs.Message)
	return sqsConsumer
}

func (connector *sqsConnector) Connect() error {
	// TODO. We dont connect here, we connect on each call, it's a REST Api
	queuesUrls, err := connector.conn.ListQueues("")
	if err != nil {
		logrus.Error("Could not list SQS queues")
		return errors.New("Could not list SQS queues")
	}
	connector.storeQueues(queuesUrls)
	return nil
}

func (connector *sqsConnector) Consume(delivery chan []byte) {
	for {
		for _, queueUrl := range connector.queues {
			logrus.Debug("Listening in queue ", queueUrl)
			queue, err := connector.conn.GetQueue(queueUrl)
			if err != nil {
				logrus.Error("Could not get messages from ", queueUrl)
			}
			messages, errMessage := queue.ReceiveMessage(5)
			if errMessage != nil {
				logrus.Error(errMessage)
				logrus.Error("Error receiving messages from queue ", queueUrl)
			}
			for _, message := range messages.Messages {
				if message.Body != "" {
					connector.workingMessages[message.MD5OfBody] = message
					delivery <- []byte(message.Body)
				}
			}
		}
		time.Sleep(5)
	}
}

func (connector *sqsConnector) RemoveMessage() error {
	return nil
}

func (connector *sqsConnector) Close() error {
	connector.conn = nil
	return nil
}

func (connector *sqsConnector) storeQueues(queuesList *sqs.ListQueuesResponse) error {
	for _, tmpQueue := range queuesList.QueueUrl {
		if tmpQueue != "" {
			fmt.Println(tmpQueue)
			tmpQueue = tmpQueue[8:]
			fmt.Println(tmpQueue)
			fields := strings.SplitN(tmpQueue, "/", 3)
			connector.queues = append(connector.queues, fields[2])
		}
	}
	return nil
}
