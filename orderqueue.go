package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"os"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

func getOrdersFromQueue() ([]Order, error) {
    ctx := context.Background()

    var orders []Order

    // Get queue name from environment variable
    orderQueueName := os.Getenv("ORDER_QUEUE_NAME")
    if orderQueueName == "" {
        log.Printf("ORDER_QUEUE_NAME is not set")
        return nil, errors.New("ORDER_QUEUE_NAME is not set")
    }

    // Get Service Bus connection string from environment variable
    connectionString := os.Getenv("AZURE_SERVICEBUS_CONNECTION_STRING")
    if connectionString == "" {
        log.Printf("AZURE_SERVICEBUS_CONNECTION_STRING is not set")
        return nil, errors.New("AZURE_SERVICEBUS_CONNECTION_STRING is not set")
    }

    // Create a client using the connection string
    client, err := azservicebus.NewClientFromConnectionString(connectionString, nil)
    if err != nil {
        log.Printf("failed to create service bus client: %v", err)
        return nil, err
    }
    defer client.Close(ctx)

    // Create a receiver
    receiver, err := client.NewReceiverForQueue(orderQueueName, nil)
    if err != nil {
        log.Printf("failed to create receiver: %v", err)
        return nil, err
    }
    defer receiver.Close(ctx)

    // Receive messages
    messages, err := receiver.ReceiveMessages(ctx, 10, nil)
    if err != nil {
        log.Printf("failed to receive messages: %v", err)
        return nil, err
    }

    for _, message := range messages {
        log.Printf("message received: %s\n", string(message.Body))

        // First, unmarshal the JSON data into a string
        var jsonStr string
        err = json.Unmarshal(message.Body, &jsonStr)
        if err != nil {
            log.Printf("failed to deserialize message: %s", err)
            return nil, err
        }

        // Then, unmarshal the string into an Order
        order, err := unmarshalOrderFromQueue([]byte(jsonStr))
        if err != nil {
            log.Printf("failed to unmarshal message: %v", err)
            return nil, err
        }

        // Add order to []order slice
        orders = append(orders, order)

        // Complete the message
        err = receiver.CompleteMessage(ctx, message, nil)
        if err != nil {
            log.Printf("failed to complete message: %v", err)
            // Consider whether you want to remove the order from the slice
            // orders = orders[:len(orders)-1]
            continue
        }
    }

    return orders, nil
}


func unmarshalOrderFromQueue(data []byte) (Order, error) {
	var order Order

	err := json.Unmarshal(data, &order)
	if err != nil {
		log.Printf("failed to unmarshal order: %v\n", err)
		return Order{}, err
	}

	// add orderkey to order
	order.OrderID = strconv.Itoa(rand.Intn(100000))

	// set the status to pending
	order.Status = Pending

	return order, nil
}
