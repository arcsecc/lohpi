package mux

/* This file contains methods that use the Lohpi network for queries */
import (
_	"io"
_	"archive/zip"
	"context"
	"encoding/json"
	"bytes"
_	"net/url"
	"errors"
	"time"
	"log"
	"strconv"
	"fmt"
	"net/http"
_	"os"

	"sync"

	"github.com/golang/protobuf/proto"
	//log "github.com/sirupsen/logrus"
	"github.com/arcsecc/lohpi/core/message"
	pb "github.com/arcsecc/lohpi/protobuf"
)

// Returns a JSON object that contains all globally unique dataset identifiers
func (m *Mux) datasetIdentifiers(ctx context.Context) ([]byte, error) {
	jsonOutput := struct {
		Sets []string
	}{
		Sets: make([]string, 0),
	}

	ids := make(chan []string)
	errChan := make(chan error)

	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, node := range m.StorageNodes() {
		n := node
		go func() {
			defer close(ids)
			defer close(errChan)
			identifiers, err := m.requestDatasetIdentifiers(n.GetIfritAddress(), newCtx)
			if err != nil {
				errChan <-err
				return
			}

			ids <-identifiers
		}()
	}

	select {
	case id := <-ids:
		jsonOutput.Sets = append(jsonOutput.Sets, id...)
	case <-newCtx.Done():
		return nil, fmt.Errorf("Timeout in 'func (m *Mux) datasetIdentifiers()")
	case err := <-errChan:
		return nil, err
	}

	return json.Marshal(jsonOutput)
}

func (m *Mux) requestDatasetIdentifiers(addr string, ctx context.Context) ([]string, error) {
	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	msg := &pb.Message{
		Type:   message.MSG_TYPE_GET_DATASET_IDENTIFIERS,
		Sender: m.pbNode(),
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	r, s, err := m.ifritClient.Sign(data)
	if err != nil {
		return nil, err
	}

	msg.Signature = &pb.MsgSignature{R: r, S: s}
	data, err = proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	ch := m.ifritClient.SendTo(addr, data)
	select {
	case resp := <-ch:
		respMsg := &pb.Message{}
		if err := proto.Unmarshal(resp, respMsg); err != nil {
			return nil, err
		}

		if err := m.verifyMessageSignature(respMsg); err != nil {
			return nil, err
		}

		return respMsg.GetStringSlice(), nil
	case <-newCtx.Done():
		return nil, fmt.Errorf("Fetching dataset identifiers from %s failed", addr)
	}

	return nil, nil
}

// Fetches the information about a dataset
func (m *Mux) datasetMetadata(w http.ResponseWriter, req *http.Request, dataset string, ctx context.Context) ([]byte, error) {
	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	nodeAddr, err := m.probeNetworkForDataset(dataset, newCtx)
	if err != nil {
		return nil, err
	}
	if nodeAddr == "" {
		return nil, errors.New("Dataset is not in network")
	}

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

		return m.getMetadata(w, req, respMsg.GetSender().GetHttpAddress(), respMsg.GetStringValue(), newCtx)
	case <-newCtx.Done():
		log.Println("Timeout in 'func (m *Mux) datasetMetadata()'")
	}

	return nil, err
}

func (m *Mux) dataset(w http.ResponseWriter, req *http.Request, dataset string, clientToken []byte, ctx context.Context) {
	newCtx, cancel := context.WithDeadline(ctx, time.Now().Add(time.Second * 15)) // set proper time to wait
	defer cancel()

	nodeAddr, err := m.probeNetworkForDataset(dataset, newCtx)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError) + ": " + err.Error(), http.StatusInternalServerError)
		return
	}
	if nodeAddr == "" {
		err := fmt.Errorf("Dataset '%s' is not in network", dataset)
		http.Error(w, http.StatusText(http.StatusNotFound) + ": " + err.Error(), http.StatusNotFound)
		return 
	}

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
		log.Println(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Sign it
	r, s, err := m.ifritClient.Sign(data)
	if err != nil {
		log.Println(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	msg.Signature = &pb.MsgSignature{R: r, S: s}
	data, err = proto.Marshal(msg)
	if err != nil {
		log.Println(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	ch := m.ifritClient.SendTo(nodeAddr, data)

	select {
	case resp := <-ch:
		respMsg := &pb.Message{}
		if err := proto.Unmarshal(resp, respMsg); err != nil {
			log.Println(err.Error())
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		
		if err := m.verifyMessageSignature(respMsg); err != nil {
			log.Println(err.Error())
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}

		if respMsg.GetDatasetResponse().GetIsAllowed() {
			m.datasetRequest(w, req, respMsg.GetDatasetResponse().GetURL(), ctx)
		} else {
			err := fmt.Errorf("Incompatible client access attributes")
			log.Println(err.Error())
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			return 
		}
	case <-newCtx.Done():
		log.Println(err.Error())
		http.Error(w, http.StatusText(http.StatusRequestTimeout), http.StatusRequestTimeout)
		return 
	}
}

// TODO: use context and refine me otherwise
func (m *Mux) getMetadata(w http.ResponseWriter, r *http.Request, host, remoteUrl string, ctx context.Context) ([]byte, error) {
	request, err := http.NewRequest("GET", "http://" + host + "/metadata/", nil)
	if err != nil {
		return nil, err
	}

	q := request.URL.Query()
    q.Add("remote-url", remoteUrl)
    request.URL.RawQuery = q.Encode()

	httpClient := &http.Client{
		Timeout: time.Duration(10 * time.Second),
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

	return nil, errors.New("Could not fetch dataset from host")
}

// TODO: use context request
func (m *Mux) datasetRequest(w http.ResponseWriter,  req *http.Request, remoteUrl string, ctx context.Context) {
	request, err := http.NewRequest("GET", remoteUrl, nil)
	if err != nil {
		log.Println(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	httpClient := &http.Client{
		Timeout: time.Duration(10 * time.Minute),
	}

	response, err := httpClient.Do(request)
    if err != nil {
		log.Println(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
    }

	buf := new(bytes.Buffer)
	buf.ReadFrom(response.Body)
	
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
}

// Returns the address of the Lohpi node that stores the given dataset
func (m *Mux) probeNetworkForDataset(dataset string, ctx context.Context) (string, error) {
	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	strChan := make(chan string)
	errChan := make(chan error)
	existsChan := make(chan bool)
	
	members := m.StorageNodes()

	if len(members) == 0 {
		return "", errors.New("No such dataset in network:" + dataset)
	}

	var wg sync.WaitGroup

	// Ask all nodes. Write address to strChan if address is found
	for _, n := range members {
		node := n
		wg.Add(1)
		go func() {
			defer wg.Done()
			exists, err := m.datasetExistsAtNode(node.GetIfritAddress(), dataset, newCtx)
			if err != nil {
				errChan <-err
				return
			}

			if exists {
				strChan <-node.GetIfritAddress()
			} else {
				existsChan <-false
			}
		}()
	}

	// Close all channels when all writers are done
	go func() {
		wg.Wait()
		defer close(errChan)
		defer close(strChan)
		defer close(existsChan)
	}()

	missed := 0

	select {
	case s := <-strChan:
		return s, nil
	case err := <-errChan:
		return "", err
	case <-existsChan:
		missed++
		if missed == len(members) {
			return "", errors.New("No such dataset in network:" + dataset)
		}
	case <-newCtx.Done():
		log.Println("Timeout in func '(m *Mux) probeNetworkForDataset()'")
	}
	return "", nil
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