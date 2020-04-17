package message

/** This file describes the message format between the clients and the mux.
 * NOTE: We rely on the assumption that we simplify the attributes to a limited degree.
 * This means that we only have a small number of string-based attributes 
 */

// The fields are either initialized or omitted, depending on the type of messages we
// pass to the mux
type ClientMessage struct {
	// The study we are interested on
	Study string

	// The attributes of the 
}