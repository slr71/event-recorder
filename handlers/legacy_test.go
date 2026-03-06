package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"regexp"
	"testing"

	"github.com/cyverse-de/event-recorder/common"
	"github.com/cyverse-de/messaging/v12"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

// MockMessagingClient provides mock implementations of the functions we need from messaging.Client.
type MockMessagingClient struct {
	PublishedNotificationMessage *messaging.WrappedNotificationMessage
	PublishedEmailRequest        *messaging.EmailRequest
}

// PublishNotificationMessage simply stores a copy of the notification message for later inspection.
func (c *MockMessagingClient) PublishNotificationMessageContext(_ context.Context, msg *messaging.WrappedNotificationMessage) error {
	c.PublishedNotificationMessage = msg
	return nil
}

// PublishEmailRequest simply stores a copy of the email request for later inspection.
func (c *MockMessagingClient) PublishEmailRequestContext(_ context.Context, req *messaging.EmailRequest) error {
	c.PublishedEmailRequest = req
	return nil
}

// NewMockMessagingClient creates a new mock messaging client for testing.
func NewMockMessagingClient() *MockMessagingClient {
	return &MockMessagingClient{
		PublishedNotificationMessage: nil,
		PublishedEmailRequest:        nil,
	}
}

// FakeNotificationID is the identifier that will be assigned to notifications by the mock database
// client.
const FakeNotificationID = "46ae63be-7030-4cdd-8eb9-66aa49fcf38b"

// FakeRoutingKey is the routing key that will be used for all AMQP delveries in this test.
const FakeRoutingKey = "events.notification.update.foo"

// MockDatabaseClient provides mock implementations of functions that handlers call to interact with the
// database.
type MockDatabaseClient struct {
	BeginCalled                bool
	CommitCalled               bool
	RollbackCalled             bool
	RegisteredNotificationType string
	SavedNotification          *common.Notification
	savedOutgoingMessage       *messaging.NotificationMessage
	unreadMessageCount         int64
}

// Begin records the fact that it was called.
func (c *MockDatabaseClient) Begin() (*sql.Tx, error) {
	c.BeginCalled = true
	return nil, nil
}

// Commit records the fact that it was called.
func (c *MockDatabaseClient) Commit(*sql.Tx) error {
	c.CommitCalled = true
	return nil
}

// Rollback records the fact that it was called.
func (c *MockDatabaseClient) Rollback(*sql.Tx) error {
	c.RollbackCalled = true
	return nil
}

// RegisterNotificationType records a notification type that has been registered.
func (c *MockDatabaseClient) RegisterNotificationType(_ context.Context, tx *sql.Tx, notificationType string) error {
	c.RegisteredNotificationType = notificationType
	return nil
}

// SaveNotification records a copy of the notification that was saved.
func (c *MockDatabaseClient) SaveNotification(_ context.Context, tx *sql.Tx, notification *common.Notification) error {
	notification.ID = FakeNotificationID
	c.SavedNotification = notification
	return nil
}

// SaveOutgoingNotification records a copy of the notification message that was saved.
func (c *MockDatabaseClient) SaveOutgoingNotification(
	_ context.Context,
	tx *sql.Tx,
	outgoingNotification *messaging.NotificationMessage,
) error {
	c.savedOutgoingMessage = outgoingNotification
	return nil
}

// CountUnreadNotifications counts the number of notifications for the user that have not been marked as read
// or deleted.
func (c *MockDatabaseClient) CountUnreadNotifications(_ context.Context, tx *sql.Tx, user string) (int64, error) {
	return c.unreadMessageCount, nil
}

// NewMockDatabaseClient creates a new mock database client for testing.
func NewMockDatabaseClient(unreadMessageCount int64) *MockDatabaseClient {
	return &MockDatabaseClient{
		BeginCalled:          false,
		CommitCalled:         false,
		RollbackCalled:       false,
		SavedNotification:    nil,
		savedOutgoingMessage: nil,
		unreadMessageCount:   unreadMessageCount,
	}
}

// getLegacyNotificationRequest returns a map that can be used as a request
// for a legacy notification.
func getLegacyNotificationRequest() map[string]interface{} {
	return map[string]interface{}{
		"type":      "analysis",
		"user":      "sarahr",
		"subject":   "some job status changed",
		"message":   "This is a test message",
		"timestamp": "2020-07-07T17:59:59-07:00",
		"payload": map[string]interface{}{
			"action":                "job_status_change",
			"analysisname":          "some job",
			"analysisdescription":   "some job description",
			"analysisstatus":        "Completed",
			"analysisstartdate":     "2020-07-07T17:59:59-07:00",
			"analysisresultsfolder": "/iplant/home/foo/analyses",
			"description":           "some job description",
			"email_address":         "sarahr@cyverse.org",
			"name":                  "some job",
			"resultfolderid":        "/iplant/home/foo/analyses",
			"startdate":             "2020-07-07T17:59:59-07:00",
			"status":                "Completed",
			"user":                  "sarahr",
		},
		"email_template": "analysis_status_change",
		"email":          true,
	}
}

// timestampFormatCorrect returns true if the format of the timestamp in the
// given message appears to be corect.
func timestampFormatCorrect(timestamp string) bool {
	re := regexp.MustCompile(`^\d+$`)
	return re.MatchString(timestamp)
}

func TestNotification(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()
	// Create the AMQP delivery for testing.
	requestBody, err := json.Marshal(getLegacyNotificationRequest())
	if err != nil {
		t.Fatalf("unable to marshal the notification request: %s", err.Error())
	}
	delivery := amqp.Delivery{Body: requestBody, RoutingKey: FakeRoutingKey}

	// The database and messaging clients along with the handler.
	databaseClient := NewMockDatabaseClient(42)
	messagingClient := NewMockMessagingClient()
	handler := NewLegacy(databaseClient, messagingClient)

	// Pass the delivery to the handler.
	err = handler.HandleMessage(ctx, "analysis", delivery)
	if err != nil {
		t.Fatalf("unxpected error returned by legacy handler: %s", err.Error())
	}

	// Verify that a transaction was created and committed.
	assert.True(databaseClient.BeginCalled, "no database transaction was started")
	assert.True(databaseClient.CommitCalled, "the database transaction was not committed")

	// Verify that the notification type was registered.
	assert.Equal("analysis", databaseClient.RegisteredNotificationType)

	// Verify that a notification was saved and spot-check a couple of fields.
	savedNotification := databaseClient.SavedNotification
	if savedNotification == nil {
		t.Fatalf("no notification was saved")
	}
	assert.Equal("analysis", savedNotification.NotificationType, "incorrect notification type")
	assert.Equal("sarahr", savedNotification.User, "incorrect user")
	assert.Equal(FakeRoutingKey, savedNotification.RoutingKey, "incorrect routing key")

	// Verify that the outgoing notification was saved in the database and spot-check a couple of fields.
	savedOutgoingMessage := databaseClient.savedOutgoingMessage
	if savedOutgoingMessage == nil {
		t.Fatalf("the outbound notification message was not recorded in the database")
	}
	assert.Equal(FakeNotificationID, savedOutgoingMessage.Message["id"], "incorrect ID")
	assert.Truef(
		timestampFormatCorrect(savedOutgoingMessage.Message["timestamp"].(string)),
		"incorrect timestamp format: %s",
		savedOutgoingMessage.Message["timestamp"].(string),
	)

	// Verify that an email request was sent and spot-check a couple of fields.
	emailRequest := messagingClient.PublishedEmailRequest
	if emailRequest == nil {
		t.Fatalf("no email request was published")
	}
	assert.Equal("some job status changed", emailRequest.Subject, "incorrect subject in email request")
	assert.Equal("sarahr@cyverse.org", emailRequest.ToAddress, "incorrect address in email request")

	// Verify that the notification was published and spot-check a couple of fields.
	notification := messagingClient.PublishedNotificationMessage
	if notification == nil {
		t.Fatalf("no notification was published")
	}
	assert.Equal(FakeNotificationID, notification.Message.Message["id"], "incorrect ID in notification")
	assert.Equal(int64(42), notification.Total, "incorrect total")
	assert.Truef(
		timestampFormatCorrect(notification.Message.Message["timestamp"].(string)),
		"incorrect timestamp format: %s",
		notification.Message.Message["timestamp"].(string),
	)

	// Spot-check some fields in the payload.
	payload, ok := notification.Message.Payload.(map[string]interface{})
	if !ok {
		t.Fatal("payload doesn't appear to be a map")
	}
	assert.Truef(
		timestampFormatCorrect(payload["startdate"].(string)),
		"incorrect timestamp format: %s",
		payload["startdate"].(string),
	)
	_, ok = payload["enddate"]
	assert.False(ok, "enddate was found in the payload when it wasn't expected")
}

func TestNotificationWithoutEmail(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()
	// Disable emails for this notification.
	req := getLegacyNotificationRequest()
	req["email"] = false

	// Create the AMQP delivery for testing.
	requestBody, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("unable to marshal the notification request: %s", err.Error())
	}
	delivery := amqp.Delivery{Body: requestBody, RoutingKey: FakeRoutingKey}

	// Create the database and messaging clients along with the handler.
	databaseClient := NewMockDatabaseClient(42)
	messagingClient := NewMockMessagingClient()
	handler := NewLegacy(databaseClient, messagingClient)

	// Pass the delivery to the handler.
	err = handler.HandleMessage(ctx, "analysis", delivery)
	if err != nil {
		t.Fatalf("unxpected error returned by legacy handler: %s", err.Error())
	}

	// Verify that a transaction was created and committed.
	assert.True(databaseClient.BeginCalled, "no database transaction was started")
	assert.True(databaseClient.CommitCalled, "the database transaction was not committed")

	// Verify that the notification type was registered.
	assert.Equal("analysis", databaseClient.RegisteredNotificationType)

	// Verify that a notification was saved.
	savedNotification := databaseClient.SavedNotification
	if savedNotification == nil {
		t.Fatalf("no notification was saved")
	}

	// Verify that an email request was not sent.
	emailRequest := messagingClient.PublishedEmailRequest
	if emailRequest != nil {
		t.Fatalf("an email request was published when none was expected")
	}

	// Verify that the notification was published.
	notification := messagingClient.PublishedNotificationMessage
	if notification == nil {
		t.Fatalf("no notification was published")
	}
}

func TestNotificationWithoutMessage(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()
	// This notification should have no message.
	req := getLegacyNotificationRequest()
	req["message"] = ""

	// Create the AMQP delivery for testing.
	requestBody, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("unable to marshal the notification request: %s", err.Error())
	}
	delivery := amqp.Delivery{Body: requestBody, RoutingKey: FakeRoutingKey}

	// Create the database and messaging clients aloing with the handler.
	databaseClient := NewMockDatabaseClient(42)
	messagingClient := NewMockMessagingClient()
	handler := NewLegacy(databaseClient, messagingClient)

	// Pass the delivery to the handler.
	err = handler.HandleMessage(ctx, "analysis", delivery)
	if err != nil {
		t.Fatalf("unexpected error returned by legacy handler: %s", err.Error())
	}

	// Verify that a transaction was created and committed.
	assert.True(databaseClient.BeginCalled, "no database transaction was started")
	assert.True(databaseClient.CommitCalled, "the database transaction was not committed")

	// Verify that the notification type was registered.
	assert.Equal("analysis", databaseClient.RegisteredNotificationType)

	// Verify that a notification was saved.
	assert.NotNil(databaseClient.SavedNotification, "no notification was saved")

	// Verify that an email request was sent.
	assert.NotNil(messagingClient.PublishedEmailRequest, "no email request was published")

	// Verify that the notification was published and verify that the message text is correct.
	notification := messagingClient.PublishedNotificationMessage
	if notification == nil {
		t.Fatalf("no notification was published")
	}
	assert.Equal(req["subject"], notification.Message.Message["text"], "incorrect message text")
}

func TestNotificationWithUpperCaseUpdateType(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()
	// Create the AMQP delivery for testing.
	requestBody, err := json.Marshal(getLegacyNotificationRequest())
	if err != nil {
		t.Fatalf("unable to marshal the notification request: %s", err.Error())
	}
	delivery := amqp.Delivery{Body: requestBody, RoutingKey: FakeRoutingKey}

	// The database and messaging clients along with the handler.
	databaseClient := NewMockDatabaseClient(42)
	messagingClient := NewMockMessagingClient()
	handler := NewLegacy(databaseClient, messagingClient)

	// Pass the delivery to the handler.
	err = handler.HandleMessage(ctx, "ANALYSIS", delivery)
	if err != nil {
		t.Fatalf("unxpected error returned by legacy handler: %s", err.Error())
	}

	// Verify that a transaction was created and committed.
	assert.True(databaseClient.BeginCalled, "no database transaction was started")
	assert.True(databaseClient.CommitCalled, "the database transaction was not committed")

	// Verify that the notification type was registered.
	assert.Equal("analysis", databaseClient.RegisteredNotificationType)

	// Verify that a notification was saved and spot-check a couple of fields.
	savedNotification := databaseClient.SavedNotification
	if savedNotification == nil {
		t.Fatalf("no notification was saved")
	}
	assert.Equal("analysis", savedNotification.NotificationType, "incorrect notification type")
	assert.Equal("sarahr", savedNotification.User, "incorrect user")
	assert.Equal(FakeRoutingKey, savedNotification.RoutingKey, "incorrect routing key")

	// Verify that the outgoing notification was saved in the database and check the notification type.
	savedOutgoingMessage := databaseClient.savedOutgoingMessage
	if savedOutgoingMessage == nil {
		t.Fatalf("the outbound notification message was not recorded in the database")
	}
	assert.Equal("analysis", savedOutgoingMessage.Type, "incorrect notification type")

	// Verify that the notification was published and check the notification type.
	notification := messagingClient.PublishedNotificationMessage
	if notification == nil {
		t.Fatalf("no notification was published")
	}
	assert.Equal("analysis", notification.Message.Type, "incorrect notification type")
}
