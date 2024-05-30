package log

import (
	"github.com/blugnu/kafka/context"
)

type messages struct {
	Received *message `json:"received,omitempty"`
	Produced *message `json:"produced,omitempty"`
}

type field struct {
	GroupId *string   `json:"consumer,omitempty"`
	Message *messages `json:"message,omitempty"`
	Offset  *offset   `json:"offset,omitempty"`
}

// func (f *field) String() string {
// 	s := ""
// 	if f.GroupId != nil {
// 		s += fmt.Sprintf("group_id=%s, ", *f.GroupId)
// 	}
// 	if f.Message != nil {
// 		if f.Message.Received != nil {
// 			s += fmt.Sprintf("message.received=[%s], ", f.Message.Received.String())
// 		}
// 		if f.Message.Produced != nil {
// 			s += fmt.Sprintf("message.produced=[%s], ", f.Message.Produced.String())
// 		}
// 	}
// 	if f.Offset != nil {
// 		s += fmt.Sprintf("offset=%s, ", f.Offset.String())
// 	}
// 	return strings.TrimRight(s, ", ")
// }

func Field(ctx context.Context) *field {
	z := field{}
	k := &field{}
	m := &messages{}

	if groupId, isSet := context.GroupId(ctx); isSet {
		k.GroupId = &groupId
	}
	if msg := context.MessageReceived(ctx); msg != nil {
		msg := Message(msg)
		m.Received = msg
	}
	if msg := context.MessageProduced(ctx); msg != nil {
		m.Produced = Message(msg)
		m.Produced.Age = nil
	}
	if off := context.Offset(ctx); off != nil {
		k.Offset = Offset(off)
	}

	if m.Produced != nil || m.Received != nil {
		k.Message = m
	}

	if *k != z {
		return k
	}

	return nil
}
