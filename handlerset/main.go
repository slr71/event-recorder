package handlerset

import (
	"context"
	"fmt"
	"strings"

	"github.com/cyverse-de/event-recorder/common"
	"github.com/cyverse-de/event-recorder/handlers"
	"github.com/cyverse-de/event-recorder/logging"
	"github.com/cyverse-de/messaging/v12"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "handlerset"})

const queueName = "event_listener"
const queueKey = "events.*.update.*"

// HandlerSet represents a set of AMQP message handlers.
type HandlerSet struct {
	amqpClient   *messaging.Client
	amqpSettings *common.AMQPSettings
	supportEmail string
	handlerFor   map[string]handlers.MessageHandler
}

// New creates a new handler set.
func New(
	amqpSettings *common.AMQPSettings,
	supportEmail string,
	handlerFor map[string]handlers.MessageHandler,
) (*HandlerSet, error) {
	wrapMsg := "unable to create the message handler set"

	// Create the AMQP client.
	amqpClient, err := messaging.NewClient(amqpSettings.URI, false)
	if err != nil {
		return nil, errors.Wrap(err, wrapMsg)
	}

	// Build and return the handler set.
	handlerSet := HandlerSet{
		amqpClient:   amqpClient,
		amqpSettings: amqpSettings,
		supportEmail: supportEmail,
		handlerFor:   handlerFor,
	}
	return &handlerSet, nil
}

// parseRoutingKey extracts the event category and update type from the delivery tag.
func (hs *HandlerSet) parseRoutingKey(tag string) (string, string, error) {
	components := strings.Split(tag, ".")
	if len(components) < 4 {
		return "", "", fmt.Errorf("routing key %s has too few components", tag)
	}
	return components[1], components[3], nil
}

// ack acknowledges a delivery and logs an error if the acknowledgement fails.
func (hs *HandlerSet) ack(delivery amqp.Delivery) {
	err := delivery.Ack(false)
	if err != nil {
		log.Errorf("unable to acknowledge delivery: %s", err.Error())
	}
}

// nack negatively acknowledges a delivery and logs an error if the acknowledgement fails.
func (hs *HandlerSet) nack(delivery amqp.Delivery, requeue bool) {
	err := delivery.Nack(false, requeue)
	if err != nil {
		log.Errorf("unable to negatively acknowledge delivery: %s", err.Error())
	}
}

// sendUnrecoverableErrorEmail sends an email to a configurable email address indicating that
// a message delivery couldn't be processed.
func (hs *HandlerSet) sendUnrecoverableErrorEmail(ctx context.Context, delivery amqp.Delivery, cause handlers.UnrecoverableError) {
	wrapMsg := "unable to send unrecoverable error notification email request"

	// Build the email request.
	request := messaging.EmailRequest{
		Subject:      "Unrecoverable Error in the Event Recorder service",
		ToAddress:    hs.supportEmail,
		TemplateName: "notifications_event_discarded",
		TemplateValues: map[string]interface{}{
			"error":        cause.Error(),
			"routing_key":  delivery.RoutingKey,
			"message_body": string(delivery.Body),
		},
	}

	// Publish the request.
	err := hs.amqpClient.PublishEmailRequestContext(ctx, &request)
	if err != nil {
		log.Errorf("%s: %s", wrapMsg, err.Error())
	}
}

// logDelivery logs some information about a message delivery for troubleshooting purposes.
func (hs *HandlerSet) logDelivery(description string, delivery amqp.Delivery) {
	log.Infof("%s: %s; %s", description, delivery.RoutingKey, delivery.Body)
}

// handleMessage handles an incoming AMQP message.
func (hs *HandlerSet) handleMessage(ctx context.Context, delivery amqp.Delivery) {
	category, updateType, err := hs.parseRoutingKey(delivery.RoutingKey)
	if err != nil {
		log.Errorf("unable to handle message: %s", err.Error())
		hs.nack(delivery, false)
		return
	}

	// Look up the handler for the category.
	handler := hs.handlerFor[category]
	if handler == nil {
		log.Infof("no handler for category '%s'; ignoring delivery", category)
		hs.ack(delivery)
		return
	}

	// Dispatch the delivery to the handler.
	err = handler.HandleMessage(ctx, updateType, delivery)
	if err != nil {
		switch val := err.(type) {
		case handlers.UnrecoverableError:
			log.Errorf("discarding message because of an unrecoverable error: %s", val.Error())
			hs.sendUnrecoverableErrorEmail(ctx, delivery, val)
			hs.logDelivery("discarded delivery", delivery)
			hs.nack(delivery, false)
		case handlers.RecoverableError:
			log.Errorf("requeuing message becuse of a recoverable error: %s", val.Error())
			hs.logDelivery("requeued delivery", delivery)
			hs.nack(delivery, true)
		case error:
			log.Errorf(
				"requeuing message because of an error that is presumed to be recoverable: %s",
				val.Error(),
			)
			hs.logDelivery("requeued delivery", delivery)
			hs.nack(delivery, true)
		}
		return
	}

	// If we get here then the delivery was processed successfully.
	hs.ack(delivery)
}

// Listen waits for incoming AMQP messages and dispatches any messages that it recieves to a handler.
func (hs *HandlerSet) Listen() error {
	wrapMsg := "error encountered while listening for incoming events"

	// Set up publishing on the AMQP client in case we need to publish any messages.
	err := hs.amqpClient.SetupPublishing(hs.amqpSettings.ExchangeName)
	if err != nil {
		return errors.Wrap(err, wrapMsg)
	}

	// Start listening for incoming messages.
	go hs.amqpClient.Listen()

	// Listen for incoming messages.
	hs.amqpClient.AddConsumer(
		hs.amqpSettings.ExchangeName,
		hs.amqpSettings.ExchangeType,
		queueName,
		queueKey,
		hs.handleMessage,
		100,
	)

	return nil
}

// Close closes a message handler set.
func (hs *HandlerSet) Close() {
	hs.amqpClient.Close()
}
