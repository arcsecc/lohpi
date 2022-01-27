package status

import (
	"errors"
	"fmt"
	"strconv"
)

var (
	ErrNoSQLConnectionString = errors.New("SQL connection string is not set")
)

type Code uint32

const (
	OK Code = 0

	UnkownType Code = 1

	InvalidSignature Code = 2

	NotFound Code = 4

	Unimplemented Code = 5

	NetworkEntityUnavailable Code = 6

	PermissionDenied Code = 7

	NilMessage Code = 8

	TransactionFailed Code = 9

	IndexFailed Code = 10

	InvalidIdentifier Code = 11

	Nil Code = 12

	SigningError Code = 13
)

type Status struct {
	code Code
	msg  string
}

func (s Status) String() string {
	return s.msg
}

func (s Status) Code() Code {
	return s.code
}

func (s Status) Marshal() ([]byte, error) {
	return nil, nil
}

func (c Code) String() string {
	switch c {
	case OK:
		return "OK"
	case UnkownType:
		return "UnkownMessageType"
	case InvalidSignature:
		return "InvalidSignature"
	case NotFound:
		return "NotFound"
	case Unimplemented:
		return "Unimplemented"
	case NetworkEntityUnavailable:
		return "NetworkEntityUnavailable"
	case PermissionDenied:
		return "PermissionDenied"
	case NilMessage:
		return "NilMessage"
	case TransactionFailed:
		return "TransactionFailed"
	case IndexFailed:
		return "IndexFailed"
	case Nil:
		return "Nil"
	case SigningError:
		return "SigningError"
	default:
		return "Code(" + strconv.FormatInt(int64(c), 10) + ")"
	}
}

func Error(c Code, msg string) error {
	return fmt.Errorf("Code: %s\tMessage: %s", c.String(), msg)
}

func Errorf(c Code, format string, a ...interface{}) error {
	return fmt.Errorf("Code: %s\tMessage: %s", c.String(), fmt.Sprintf(format, a...))
}
