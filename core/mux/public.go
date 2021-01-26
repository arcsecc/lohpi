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
	"fmt"
	"net/http"
_	"os"

	"sync"

	"github.com/golang/protobuf/proto"
	//log "github.com/sirupsen/logrus"
	"github.com/tomcat-bit/lohpi/core/message"
	pb "github.com/tomcat-bit/lohpi/protobuf"
)

// Returns a JSON object that contains all globally unique dataset identifiers
func (m *Mux) datasetIdentifiers(ctx context.Context) ([]byte, error) {
	jsonOutput := struct {
		Sets []string
	}{
		Sets: make([]string, 0),
	}

	ids := make(chan []string, 1)
	defer close(ids)
	
	errChan := make(chan error)
	defer close(errChan)

	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, node := range m.StorageNodes() {
		n := node
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, _ := context.WithCancel(ctx)
			identifiers, err := m.requestDatasetIdentifiers(n.GetIfritAddress(), c)
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
	case <-ctx.Done():
		return nil, fmt.Errorf("Fetching dataset identifiers timeout")
	case err := <-errChan:
		return nil, err
	}

	wg.Wait()

	return json.Marshal(jsonOutput)
}

func (m *Mux) requestDatasetIdentifiers(addr string, ctx context.Context) ([]string, error) {
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
	case <-ctx.Done():
		return nil, fmt.Errorf("Fetching dataset identifiers from %s failed", addr)
	}

	return nil, nil
}

// Fetches the information about a dataset
func (m *Mux) datasetMetadata(w http.ResponseWriter, req *http.Request, dataset string, ctx context.Context) ([]byte, error) {
	nodeAddr, err := m.probeNetworkForDataset(dataset)
	if err != nil {
		return nil, err
	}
	if nodeAddr == "" {
		return nil, errors.New("Dataset is not in network")
	}

	// Create dataset request
	msg := &pb.Message{
		Type: message.MSG_TYPE_GET_DATASET_METADATA_URL,
		StringValue: dataset,
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

		return m.getMetadata(w, respMsg.GetSender().GetHttpAddress(), respMsg.GetStringValue(), ctx)
	}

	return nil, err
}

func (m *Mux) dataset(w http.ResponseWriter, req *http.Request, dataset string, ctx context.Context) ([]byte, error) {
	nodeAddr, err := m.probeNetworkForDataset(dataset)
	if err != nil {
		return nil, err
	}
	if nodeAddr == "" {
		return nil, errors.New("Dataset is not in network")
	}

	// Create dataset request
	msg := &pb.Message{
		Type: message.MSG_TYPE_GET_DATASET_URL,
		StringValue: dataset,
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

		return m.getZipArchive(w, respMsg.GetSender().GetHttpAddress(), respMsg.GetStringValue(), ctx)
	}

	return nil, err
}

func (m *Mux) getMetadata(w http.ResponseWriter, host, remoteUrl string, ctx context.Context) ([]byte, error) {
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
		buf.ReadFrom(response.Body)
		return buf.Bytes(), nil
	}
	return nil, errors.New("Could not fetch dataset from host.")
}

func (m *Mux) getZipArchive(w http.ResponseWriter, host, remoteUrl string, ctx context.Context) ([]byte, error) {
	request, err := http.NewRequest("GET", "http://" + host + "/archive/", nil)
	if err != nil {
		return nil, err
	}

	q := request.URL.Query()
    q.Add("remote-url", remoteUrl)
    request.URL.RawQuery = q.Encode()

	httpClient := &http.Client{
		Timeout: time.Duration(10 * time.Minute),
	}

	response, err := httpClient.Do(request)
    if err != nil {
        return nil, err
    }

	if response.StatusCode == http.StatusOK {
		buf := new(bytes.Buffer)
		buf.ReadFrom(response.Body)
		return buf.Bytes(), nil
	}
	return nil, errors.New("Could not fetch dataset from host.")
}

// Returns the address of the Lohpi node that stores the given dataset
func (m *Mux) probeNetworkForDataset(dataset string) (string, error) {
	//|var wg sync.WaitGroup

	for _, n := range m.StorageNodes() {
		node := n
		//wg.Add(1)
		//|go func() {
			//defer wg.Done()
			exists, err := m.datasetExistsAtNode(node.GetIfritAddress(), dataset)
			if err != nil {
				return "", err
			}

			if exists {
				return node.GetIfritAddress(), nil
			}
	}

	//wg.ait()

	return "", nil
}

// Returns true if the dataset identifier exists at the node, returns false otherwise. 
func (m *Mux) datasetExistsAtNode(node, dataset string) (bool, error) {
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
	}

	return false, nil
}