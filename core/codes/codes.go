package codes

type Code uint32

const (
	// OK is returned on success.
	OK Code = 0

	// The recipient of the message is unknowns.
	UnknownRecipient Code = 1

	// A timeout occured.
	Timeout Code = 2

	// The given dataset was not found.
	DatasetNotFound Code = 3

	// The dataset was not available for a client.
	DatasetNotAvailable Code = 4

	// The signature of a message could not be verified.
	InvalidMessageSignature Code = 5
)

