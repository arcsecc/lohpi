package mux

/* This file contains methods that use the Lohpi network for queries */
import (
_	"io"
_	"archive/zip"
	"context"
	"bytes"
_	"net/url"
	"errors"
	"time"
	log "github.com/sirupsen/logrus"
	"github.com/lestrrat-go/jwx/jws"
	"strconv"
	"fmt"
	"net/http"
	"encoding/json"
_	"os"

	"github.com/golang/protobuf/proto"
	//log "github.com/sirupsen/logrus"
	"github.com/arcsecc/lohpi/core/message"
	pb "github.com/arcsecc/lohpi/protobuf"
)

// Fetches the information about a dataset
func (m *Mux) datasetMetadata(w http.ResponseWriter, req *http.Request, dataset, nodeAddr string, ctx context.Context) ([]byte, error) {
	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Create dataset request
	msg := &pb.Message{
		Type: message.MSG_TYPE_GET_DATASET_METADATA_URL,
		DatasetRequest: &pb.DatasetRequest{
			Identifier: dataset, 
    		ClientToken: nil,
		},
	}

	// Marshal the request
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	// Sign it
	r, s, err := m.ifritClient.Sign(data)
	if err != nil {
		return nil, err
	}

	msg.Signature = &pb.MsgSignature{R: r, S: s}
	data, err = proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	ch := m.ifritClient.SendTo(nodeAddr, data)

	select {
	case resp := <-ch:
		respMsg := &pb.Message{}
		if err := proto.Unmarshal(resp, respMsg); err != nil {
			panic(err)
		}
		
		if err := m.verifyMessageSignature(respMsg); err != nil {
			panic(err)
		}

		return m.getMetadata(w, req, respMsg.GetStringValue(), newCtx)
	case <-newCtx.Done():
		log.Println("Timeout in 'func (m *Mux) datasetMetadata()'")
	}

	return nil, err
}

func (m *Mux) dataset(w http.ResponseWriter, req *http.Request, dataset, nodeAddr string, clientToken []byte, ctx context.Context) {
	newCtx, cancel := context.WithDeadline(ctx, time.Now().Add(time.Second * 15)) // set proper time to wait
	defer cancel()

	// Create dataset request
	msg := &pb.Message{
		Type: message.MSG_TYPE_GET_DATASET_URL,
		DatasetRequest: &pb.DatasetRequest{
			Identifier: dataset,
			ClientToken: clientToken,
		},
	}

	// Marshal the request
	data, err := proto.Marshal(msg)
	if err != nil {
		log.Fatalln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Sign it
	r, s, err := m.ifritClient.Sign(data)
	if err != nil {
		log.Fatalln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	msg.Signature = &pb.MsgSignature{R: r, S: s}
	data, err = proto.Marshal(msg)
	if err != nil {
		log.Fatalln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	ch := m.ifritClient.SendTo(nodeAddr, data)

	select {
	case resp := <-ch:
		respMsg := &pb.Message{}
		if err := proto.Unmarshal(resp, respMsg); err != nil {
			log.Fatalln(err.Error())
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		
		if err := m.verifyMessageSignature(respMsg); err != nil {
			log.Fatalln(err.Error())
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}

		if respMsg.GetDatasetResponse().GetIsAllowed() {
			// If we tried to fetch the dataset but it failed, rollback on checking it out
			if err := m.datasetRequest(w, req, respMsg.GetDatasetResponse().GetURL(), ctx); err != nil {
				if err := m.rollbackCheckout(nodeAddr, dataset, ctx); err != nil {
					log.Errorln(err.Error())
				}
				return
			}

			// Get client-related fiels
			_, oid, err := m.getClientIdentifier(clientToken)
			if err != nil {
				if err := m.rollbackCheckout(nodeAddr, dataset, ctx); err != nil {
					log.Errorln(err.Error())
				}
				return
			}
			
			m.insertCheckedOutDataset(dataset, oid)

		} else {
			err := fmt.Errorf(respMsg.GetDatasetResponse().GetErrorMessage())
			log.Errorln(err.Error())
			http.Error(w, http.StatusText(http.StatusUnauthorized) + ": " + err.Error(), http.StatusUnauthorized)
			return 
		}
	case <-newCtx.Done():
		log.Println(err.Error())
		http.Error(w, http.StatusText(http.StatusRequestTimeout), http.StatusRequestTimeout)
		return 
	}
}

func (m *Mux) getClientIdentifier(token []byte) (string, string, error) {
	msg, err := jws.ParseString(string(token))
	if err != nil {
		return "", "", err
	}

	s := msg.Payload()
	if s == nil {
		return "", "", errors.New("Payload was nil")
	}

	c := struct{
		Name string		`json:"name"`
		Oid string		`json:"oid"`
	}{}

	if err := json.Unmarshal(s, &c); err != nil {
    	return "", "", err
	}
	
	return c.Name, c.Oid, nil
}

// TODO: use context and refine me otherwise
func (m *Mux) getMetadata(w http.ResponseWriter, r *http.Request, remoteUrl string, ctx context.Context) ([]byte, error) {
	request, err := http.NewRequest("GET", remoteUrl, nil)
	if err != nil {
		return nil, err
	}

	httpClient := &http.Client{
		Timeout: time.Duration(20 * time.Second),
	}

	response, err := httpClient.Do(request)
    if err != nil {
        return nil, err
    }

	if response.StatusCode == http.StatusOK {
		buf := new(bytes.Buffer)
		_, err := buf.ReadFrom(response.Body)
		if err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}

	return nil, errors.New("Could not fetch metadata from host")
}

// TODO: use context request
func (m *Mux) datasetRequest(w http.ResponseWriter,  req *http.Request, remoteUrl string, ctx context.Context) error {
	request, err := http.NewRequest("GET", remoteUrl, nil)
	if err != nil {
		log.Println(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return err
	}

	httpClient := &http.Client{
		Timeout: time.Duration(10 * time.Minute),
	}

	response, err := httpClient.Do(request)
    if err != nil {
		log.Println(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return err
    }

	buf := new(bytes.Buffer)
	buf.ReadFrom(response.Body)

	// Something failed. Rollback dataset checkout
	if response.StatusCode != http.StatusOK {
		log.Errorf("Error from remote archive: %s\n", buf.Bytes())
		err := fmt.Errorf("Could not checkout dataset from the node")
		http.Error(w, http.StatusText(http.StatusInternalServerError) + ": " + err.Error(), http.StatusInternalServerError)
		return err
	}

	w.WriteHeader(response.StatusCode)
	req.Header.Add("Content-Length", strconv.Itoa(len(buf.Bytes())))

	// Octet stream is default
	contentType := req.Header.Get("Content-Type")
	if contentType == "" {
		w.Header().Set("Content-Type", "application/octet-stream")
	}

	w.Header().Set("Content-Type", contentType)
	//w.Header().Set("Content-Disposition", req.Header.Get("Content-Type"))
	w.Write(buf.Bytes())

	return nil
}

// Returns true if the dataset identifier exists at the node, returns false otherwise. 
func (m *Mux) datasetExistsAtNode(node, dataset string, ctx context.Context) (bool, error) {
	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	msg := &pb.Message{
		Type:        message.MSG_TYPE_DATASET_EXISTS,
		Sender:      m.pbNode(),
		StringValue: dataset,
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return false, err
	}

	r, s, err := m.ifritClient.Sign(data)
	if err != nil {
		return false, err
	}

	msg.Signature = &pb.MsgSignature{R: r, S: s}
	data, err = proto.Marshal(msg)
	if err != nil {
		return false, err
	}

	ch := m.ifritClient.SendTo(node, data)
	select {
	case resp := <-ch:
		respMsg := &pb.Message{}
		if err := proto.Unmarshal(resp, respMsg); err != nil {
			return false, err
		}
		
		if err := m.verifyMessageSignature(respMsg); err != nil {
			return false, err
		}
		
		if respMsg.GetBoolValue() {
			return true, nil
		}
	case <-newCtx.Done():
		log.Println("Timeout in func '(m *Mux) datasetExistsAtNode()'")
		return false, errors.New("Timeout in func '(m *Mux) datasetExistsAtNode()'")
	}

	return false, nil
}

// TODO: handle ctx
func (m *Mux) rollbackCheckout(nodeAddr, dataset string, ctx context.Context) error {
	msg := &pb.Message{
		Type:        message.MSG_TYPE_ROLLBACK_CHECKOUT,
		Sender:      m.pbNode(),
		StringValue: dataset,
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	r, s, err := m.ifritClient.Sign(data)
	if err != nil {
		return err
	}

	msg.Signature = &pb.MsgSignature{R: r, S: s}
	data, err = proto.Marshal(msg)
	if err != nil {
		return err
	}

	ch := m.ifritClient.SendTo(nodeAddr, data)
	select {
	case resp := <-ch:
		respMsg := &pb.Message{}
		if err := proto.Unmarshal(resp, respMsg); err != nil {
			return err
		}
		
		if err := m.verifyMessageSignature(respMsg); err != nil {
			return err
		}
		
	case <-ctx.Done():
		err := errors.New("Could not verify dataset checkout rollback")
		return err
	}

	return nil
}

func (m *Mux) insertCheckedOutDataset(dataset, clientId string) {
	m.clientCheckoutMapLock.Lock()
	defer m.clientCheckoutMapLock.Unlock()
	if m.clientCheckoutMap[dataset] == nil {
		m.clientCheckoutMap[dataset] = make([]string, 0)
	}
	m.clientCheckoutMap[dataset] = append(m.clientCheckoutMap[dataset], clientId)
}

func (m *Mux) getCheckedOutDatasetMap() map[string][]string {
	m.clientCheckoutMapLock.RLock()
	defer m.clientCheckoutMapLock.RUnlock()
	return m.clientCheckoutMap
}

func (m *Mux) datasetIsInvalidated(dataset string) bool {
	l := m.revokedDatasets()

	for e := l.Front(); e != nil; e = e.Next() {
		if e.Value == dataset {
			return true
		}
	}

	return false
}