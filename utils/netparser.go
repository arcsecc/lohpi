package utils 

import (
    "encoding/json"
  //  "errors"
    "fmt"
//    "io"
    "net/http"
    "strings"

    "github.com/golang/gddo/httputil/header"
)

type MalformedRequest struct {
    Status int
    Msg    string
}

func (mr *MalformedRequest) Error() string {
    return mr.Msg
}

func DecodeJSONBody(w http.ResponseWriter, r *http.Request, dst interface{}) error {
