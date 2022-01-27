package util

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/lestrrat-go/jwx/jws"
	"io"
	"net"
	"net/http"
	"strings"
)

type MalformedParserReponse struct {
	Status int
	Msg    string
}

func (mr *MalformedParserReponse) Error() string {
	return mr.Msg
}

func DecodeJSONBody(w http.ResponseWriter, r *http.Request, contentType string, dst interface{}) error {
	if r.Header.Get("Content-Type") != contentType {
		msg := "Content-Type header is not " + contentType
		return &MalformedParserReponse{Status: http.StatusUnsupportedMediaType, Msg: msg}
	}

	// 1 MB max
	r.Body = http.MaxBytesReader(w, r.Body, 1048576)

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	err := dec.Decode(&dst)
	if err != nil {
		var syntaxError *json.SyntaxError
		var unmarshalTypeError *json.UnmarshalTypeError

		switch {
		case errors.As(err, &syntaxError):
			msg := fmt.Sprintf("Request body contains badly-formed JSON (at position %d)", syntaxError.Offset)
			return &MalformedParserReponse{Status: http.StatusBadRequest, Msg: msg}

		case errors.Is(err, io.ErrUnexpectedEOF):
			msg := fmt.Sprintf("Request body contains badly-formed JSON")
			return &MalformedParserReponse{Status: http.StatusBadRequest, Msg: msg}

		case errors.As(err, &unmarshalTypeError):
			msg := fmt.Sprintf("Request body contains an invalid value for the %q field (at position %d)", unmarshalTypeError.Field, unmarshalTypeError.Offset)
			return &MalformedParserReponse{Status: http.StatusBadRequest, Msg: msg}

		case strings.HasPrefix(err.Error(), "json: unknown field "):
			fieldName := strings.TrimPrefix(err.Error(), "json: unknown field ")
			msg := fmt.Sprintf("Request body contains unknown field %s", fieldName)
			return &MalformedParserReponse{Status: http.StatusBadRequest, Msg: msg}

		case errors.Is(err, io.EOF):
			msg := "Request body must not be empty"
			return &MalformedParserReponse{Status: http.StatusBadRequest, Msg: msg}

		case err.Error() == "http: request body too large":
			msg := "Request body must not be larger than 1MB"
			return &MalformedParserReponse{Status: http.StatusRequestEntityTooLarge, Msg: msg}

		default:
			return err
		}
	}

	err = dec.Decode(&struct{}{})
	if err != io.EOF {
		msg := "Request body must only contain a single JSON object"
		return &MalformedParserReponse{Status: http.StatusBadRequest, Msg: msg}
	}

	return nil
}

// Copies the headers from h into a new map of string slices.
func CopyHeaders(h map[string][]string) map[string][]string {
	m := make(map[string][]string)
	for key, val := range h {
		m[key] = val
	}
	return m
}

// Assigns the headers in dest from src
func SetHeaders(src, dest map[string][]string) {
	for k, v := range src {
		dest[k] = v
	}
}

// Streams the data from the response 'r' into the response writer 'w'.
func StreamToResponseWriter(r *bufio.Reader, w http.ResponseWriter, bufSize int) error {
	buffer := make([]byte, bufSize)

	for {
		len, err := r.Read(buffer)
		if len > 0 {
			_, err = w.Write(buffer[:len])
			if err != nil {
				return err
			}
		}

		if err != nil {
			if err == io.EOF {
				return nil
			} else {
				return err
			}
		}
	}
	return nil
}

func ExstractPbClient(r *http.Request) (*pb.Client, error) {
	token, err := getBearerToken(r)
	if err != nil {
		return nil, err
	}

	msg, err := jws.ParseString(string(token))
	if err != nil {
		return nil, err
	}

	s := msg.Payload()
	if s == nil {
		return nil, errors.New("Payload was nil")
	}

	c := struct {
		Name         string `json:"name"`
		Oid          string `json:"oid"`
		EmailAddress string `json:"email"`
	}{}

	if err := json.Unmarshal(s, &c); err != nil {
		return nil, err
	}

	var ipAddress string

	// Validate IP address format
	if len(r.Header.Values("ip_address")) > 0 {
		ipAddress = r.Header.Values("ip_address")[0]
		if net.ParseIP(ipAddress) == nil {
			return nil, fmt.Errorf("IP address %s is invalid\n", ipAddress)
		}
	}

	return &pb.Client{
		Name:         c.Name,
		ID:           c.Oid,
		EmailAddress: c.EmailAddress,
		IpAddress:    ipAddress,
	}, nil
}

func getBearerToken(r *http.Request) ([]byte, error) {
	authHeader := r.Header.Get("Authorization")
	authHeaderContent := strings.Split(authHeader, " ")
	if len(authHeaderContent) != 2 {
		return nil, errors.New("Token not provided or malformed")
	}
	return []byte(authHeaderContent[1]), nil
}
