package producer

import (
	"log"

	"github.com/Shopify/sarama"
)

type Producer interface {
	SendMessage(string, []byte) (bool, error)
	Close()
}

type producer struct {
	broker       []string
	config       *sarama.Config
	syncProducer sarama.SyncProducer
}

func NewProducer(brokers []string, config *sarama.Config) Producer {
	return &producer{
		broker:       brokers,
		config:       config,
		syncProducer: Initialize(brokers, config),
	}
}

func (p *producer) SendMessage(topic string, value []byte) (bool, error) {
	msg := sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	}
	_, _, err := p.syncProducer.SendMessage(&msg)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (p *producer) Close() {
	p.syncProducer.Close()
}

func Initialize(broker []string, config *sarama.Config) sarama.SyncProducer {
	producer, err := sarama.NewSyncProducer(broker, config)
	if err != nil {
		log.Printf("error initializing kafka producer %v \n", err)
		panic("kafka not intialized")
	}
	return producer
}
