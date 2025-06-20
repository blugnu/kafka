package kafka

import (
	"context"
)

// HandlerFunc is the function signature for a handler function.
//
// For simple handlers you may choose to implement a function with this signature
// and cast it as this type for use as a Handler (modelled on the http.HandlerFunc
// type):
//
//	kafka.TopicHandler("my-topic", kafka.HandlerFunc(func(ctx context.Context, msg *kafka.Message) error {
//	    // handle the message
//	    return nil
//	}))
//
// For more complex handlers you may choose to implement the Handler interface
// on a struct type that holds additional state or dependencies:
//
//	type MyHandler struct {
//	    db *sql.DB
//	    p kakfa.Producer
//	}
//
//	func (h *MyHandler) HandleMessage(ctx context.Context, msg *kafka.Message) error {
//	    // handle the message using h.db
//	    // ...
//	    // then produce a message to the "done" topic
//	    msg, _ := kafka.NewMessage("my-topic-done")).
//	                  WithKey(msg.Key).
//	                  Build()
//	    _, err := h.MustProduce(ctx, msg)
//	    return err
//	}
//
//	kafka.TopicHandler("my-topic", &MyHandler{
//	    db: theDB,
//	    p: theProducer,
//	})
type HandlerFunc func(context.Context, *Message) error

// HandleMessage implements the Handler interface by calling the HandlerFunc.
// If the HandlerFunc is nil, this method does nothing (except return nil).
func (fn HandlerFunc) HandleMessage(ctx context.Context, msg *Message) error {
	if fn == nil {
		return nil
	}
	return fn(ctx, msg)
}

type Handler interface {
	HandleMessage(ctx context.Context, msg *Message) error
}

// If returns a Handler that only calls the given Handler if the given condition
// function returns true for the message.  If the condition function returns false,
// the message is ignored and the Handler is not called.
func If(cond func(*Message) bool, h Handler) Handler {
	return HandlerFunc(func(ctx context.Context, msg *Message) error {
		if cond(msg) {
			return h.HandleMessage(ctx, msg)
		}
		return nil
	})
}
