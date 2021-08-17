package codes

import (
	"strconv"
)

func (c Code) String() string {
	switch c {
	case OK:
		return "OK"
	case UnknownRecipient: 
		return "UnknownRecipient"
	case Timeout: 
		return "Timeout"
	case DatasetNotFound: 
		return "DatasetNotFound"
	case DatasetNotAvailable: 
		return "DatasetNotAvailable"
	default:
		return "Code(" + strconv.FormatInt(int64(c), 10) + ")"
	}
}