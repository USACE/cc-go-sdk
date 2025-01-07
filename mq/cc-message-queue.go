package mq

import (
	"context"
	"fmt"
	"log"
	"net/url"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

const (
	msqQueueUriTemplate   string = "mqtt://"
	keepAlive             uint16 = 20 //time in seconds
	sessionExpiryInterval uint32 = 60 //time in seconds
)

var ctx context.Context = context.Background()

var (
	ErrNotSubscribed     = fmt.Errorf("the client is not subscribed to this topic")
	ErrAlreadySubscribed = fmt.Errorf("the client is already subscribed to this topic")
)

type SubscribeFunction func(topic string, payload []byte)

type CcMessageQueue interface {
	Send(topic string, payload []byte) error
	Subscribe(topic string, f SubscribeFunction) error
	UnSubscribe(topic string) error
}

func NewMqttMessageQueue(clientId string, serverUri string) (CcMessageQueue, error) {
	u, err := url.Parse(fmt.Sprintf("%s%s", msqQueueUriTemplate, serverUri))
	if err != nil {
		return nil, err
	}

	mq := PahoMessageQueue{
		ClientId:      clientId,
		ServerURIs:    []*url.URL{u},
		subscriptions: make(map[string]func()),
	}
	err = mq.Connect()
	return &mq, err
}

type PahoMessageQueue struct {
	ClientId      string
	ServerURIs    []*url.URL
	client        *autopaho.ConnectionManager
	subscriptions map[string]func()
}

func (pmq *PahoMessageQueue) Connect() error {
	cliCfg := autopaho.ClientConfig{
		ServerUrls:                    pmq.ServerURIs,
		KeepAlive:                     keepAlive,
		CleanStartOnInitialConnection: false,
		SessionExpiryInterval:         sessionExpiryInterval,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			log.Printf("Client: %s Connected to Message Queue\n", pmq.ClientId)
		},
		OnConnectError: func(err error) {
			log.Printf("failed Message Queue connection: %s\n", err)
		},
		ClientConfig: paho.ClientConfig{
			ClientID: pmq.ClientId,
			OnClientError: func(err error) {
				log.Printf("Message queue error: %s\n", err)
			},
			OnServerDisconnect: func(d *paho.Disconnect) {
				if d.Properties != nil {
					log.Printf("Message queue server requested disconnect: %s\n", d.Properties.ReasonString)
				} else {
					log.Printf("Message queue server requested disconnect; reason code: %d\n", d.ReasonCode)
				}
			},
		},
	}

	c, err := autopaho.NewConnection(ctx, cliCfg) // starts process; will reconnect until context cancelled
	if err != nil {
		return err
	}

	err = c.AwaitConnection(ctx)
	if err != nil {
		return err
	}
	pmq.client = c
	return nil
}

func (pmq *PahoMessageQueue) Subscribe(topic string, subscriptionFunc SubscribeFunction) error {
	if _, ok := pmq.subscriptions[topic]; ok {
		return ErrAlreadySubscribed
	}
	_, err := pmq.client.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{
			{Topic: topic, QoS: 1},
		},
	})
	if err != nil {
		return err
	}

	unsubscribeFunc := pmq.client.AddOnPublishReceived(func(pr autopaho.PublishReceived) (bool, error) {
		{
			subscriptionFunc(pr.Packet.Topic, pr.Packet.Payload)
			return true, nil
		}
	})
	pmq.subscriptions[topic] = unsubscribeFunc
	return nil
}

func (pmq *PahoMessageQueue) UnSubscribe(topic string) error {
	unsubscribeFunc, ok := pmq.subscriptions[topic]
	if !ok {
		return ErrNotSubscribed
	}
	unsubscribeFunc()
	delete(pmq.subscriptions, topic)
	return nil
}

func (pmq *PahoMessageQueue) Send(topic string, payload []byte) error {
	_, err := pmq.client.Publish(ctx, &paho.Publish{
		QoS:     1,
		Topic:   topic,
		Payload: payload,
		Retain:  true,
	})
	return err
}
