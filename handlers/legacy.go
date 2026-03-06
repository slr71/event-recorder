package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/cyverse-de/event-recorder/common"
	"github.com/cyverse-de/messaging/v12"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

// LegacyRequest represents a deserialized request for a backwards compatible notification.
type LegacyRequest struct {
	RequestType   string                 `json:"type"`
	User          string                 `json:"user"`
	Subject       string                 `json:"subject"`
	Timestamp     string                 `json:"timestamp"`
	Email         bool                   `json:"email"`
	EmailTemplate string                 `json:"email_template"`
	Payload       map[string]interface{} `json:"payload"`
	Message       string                 `json:"message"`
}

// Legacy is a message handler for events published by the backwards compatible HTTP API.
type Legacy struct {
	dbc             DatabaseClient
	messagingClient MessagingClient
}

// NewLegacy returns a new legacy event handler.
func NewLegacy(dbc DatabaseClient, messagingClient MessagingClient) *Legacy {
	return &Legacy{
		dbc:             dbc,
		messagingClient: messagingClient,
	}
}

// sendEmailRequest sends the email request for a single notification request.
func (lh *Legacy) sendEmailRequest(ctx context.Context, request *LegacyRequest) error {
	wrapMsg := "unable to send the email request"

	// Extract the email address from the notification request payload.
	var emailAddress string
	switch str := request.Payload["email_address"].(type) {
	case string:
		emailAddress = str
	default:
		return NewUnrecoverableError("%s: %s", wrapMsg, "no email address provided or invalid data type in request")
	}

	// Validate the email address.
	err := common.ValidateEmailAddress(emailAddress)
	if err != nil {
		return NewUnrecoverableError("%s: %s", wrapMsg, err.Error())
	}

	// Validate the template name.
	if request.EmailTemplate == "" {
		return NewUnrecoverableError("%s: %s", wrapMsg, "no email template provided")
	}

	// Create the email request body.
	emailRequest := &messaging.EmailRequest{
		Subject:        request.Subject,
		ToAddress:      emailAddress,
		TemplateName:   request.EmailTemplate,
		TemplateValues: request.Payload,
	}
	err = lh.messagingClient.PublishEmailRequestContext(ctx, emailRequest)
	if err != nil {
		return NewRecoverableError("%s: %s", wrapMsg, err.Error())
	}

	return nil
}

// fixTimestamp fixes a timestamp stored as a string in a map.
func fixTimestamp(m map[string]interface{}, k string) error {
	wrapMsg := fmt.Sprintf("unable to fix the timestamp in key '%s'", k)

	// Extract the current value.
	v, present := m[k]
	if !present {
		return nil
	}

	// Convert the value to a string. We only have to check types used by the json package.
	var stringValue string
	switch val := v.(type) {
	case string:
		stringValue = val
	case float64:
		stringValue = fmt.Sprintf("%d", int64(val))
	default:
		return fmt.Errorf("%s: %s", wrapMsg, "invalid data type")
	}

	// Convert the timestamp to milliseconds since the epoch.
	convertedValue, err := common.FixTimestamp(stringValue)
	if err != nil {
		return errors.Wrap(err, wrapMsg)
	}

	// Directly update the value in the map.
	m[k] = convertedValue

	return nil
}

// buildNotificationMessage formats the outgoing notification message destined
// for the Discovery Environment UI. This function changes the message payload,
// so it should only be called after an exact copy of the incoming message body
// is no longer needed.
func (lh *Legacy) buildNotificationMessage(
	request *common.Notification,
	payload *LegacyRequest,
) (*messaging.NotificationMessage, error) {
	wrapMsg := "unable to build notification message"

	// Determine the primary text of the message portion of the notification.
	messageText := payload.Message
	if messageText == "" {
		messageText = payload.Subject
	}

	// The message portion of the request sent to the UI is a JSON object.
	outgoingMessage := map[string]interface{}{
		"id":        request.ID,
		"timestamp": common.FormatTimestamp(request.TimeCreated),
		"text":      messageText,
	}

	// Ensure that the analysis start date is in the correct format if it's present.
	err := fixTimestamp(payload.Payload, "startdate")
	if err != nil {
		return nil, errors.Wrap(err, wrapMsg)
	}

	// Ensure that the analysis end date is in the correct format if it's present.
	err = fixTimestamp(payload.Payload, "enddate")
	if err != nil {
		return nil, errors.Wrap(err, wrapMsg)
	}

	// Replace underscores with spaces in the notification type.
	payload.RequestType = strings.ReplaceAll(payload.RequestType, "_", " ")

	// Build the notification message.
	notificationMessage := &messaging.NotificationMessage{
		Deleted:       request.Deleted,
		Email:         payload.Email,
		EmailTemplate: payload.EmailTemplate,
		Message:       outgoingMessage,
		Payload:       payload.Payload,
		Seen:          request.Seen,
		Subject:       request.Subject,
		Type:          strings.ReplaceAll(request.NotificationType, "_", " "),
		User:          request.User,
	}

	return notificationMessage, nil
}

// HandleMessage handles a single AMQP delivery.
func (lh *Legacy) HandleMessage(ctx context.Context, updateType string, delivery amqp.Delivery) error {
	var err error
	updateType = strings.ToLower(updateType)

	// Parse the message body.
	var request LegacyRequest
	err = json.Unmarshal(delivery.Body, &request)
	if err != nil {
		return NewUnrecoverableError("unable to parse message body: %s", err.Error())
	}

	// Parse the timestamp.
	timeCreated, err := time.Parse(time.RFC3339Nano, request.Timestamp)
	if err != nil {
		return NewUnrecoverableError("unable to parse timestamp: %s", err.Error())
	}

	// Begin a database transaction.
	tx, err := lh.dbc.Begin()
	if err != nil {
		return NewRecoverableError("uanble to begin a database transaction: %s", err.Error())
	}
	defer func() {
		err = lh.dbc.Rollback(tx)
	}()

	// Register the notification type in case it doesn't exist in the database yet.
	err = lh.dbc.RegisterNotificationType(ctx, tx, updateType)
	if err != nil {
		return NewUnrecoverableError("unable to register the notification type: %s", err.Error())
	}

	// Store the message in the database.
	storableRequest := &common.Notification{
		NotificationType: updateType,
		User:             request.User,
		Subject:          request.Subject,
		Seen:             false,
		Deleted:          false,
		TimeCreated:      timeCreated,
		Message:          string(delivery.Body),
		RoutingKey:       delivery.RoutingKey,
	}
	err = lh.dbc.SaveNotification(ctx, tx, storableRequest)
	if err != nil {
		return NewUnrecoverableError("unable to save the notification: %s", err.Error())
	}

	// Send the email request.
	if request.Email {
		err = lh.sendEmailRequest(ctx, &request)
		if err != nil {
			return err
		}
	}

	// Build the notification message.
	notificationMessage, err := lh.buildNotificationMessage(storableRequest, &request)
	if err != nil {
		return err
	}

	// Save the outgoing notificaiton in the database.
	err = lh.dbc.SaveOutgoingNotification(ctx, tx, notificationMessage)
	if err != nil {
		return err
	}

	// Count the number of unread notifications.
	unreadNotificationCount, err := lh.dbc.CountUnreadNotifications(ctx, tx, request.User)
	if err != nil {
		return err
	}

	// Add the wrapper around the notification message.
	wrappedNotificationMessage := &messaging.WrappedNotificationMessage{
		Message: notificationMessage,
		Total:   unreadNotificationCount,
	}

	// Publish the outgoing notification message.
	err = lh.messagingClient.PublishNotificationMessageContext(ctx, wrappedNotificationMessage)
	if err != nil {
		return err
	}

	// Commit the transaction.
	err = lh.dbc.Commit(tx)
	if err != nil {
		return NewRecoverableError("unable to commit the database transaction: %s", err.Error())
	}

	return err
}
