package mux

/* This file contains methods that use the Lohpi network for queries */
import (
_	"io"
_	"archive/zip"
	"context"
	"encoding/json"
	"net/url"
	"bytes"
	"errors"
	"fmt"
	"net/http"
_	"os"
	"time"
	"sync"

	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
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
	var wg sync.WaitGroup

	for _, node := range m.StorageNodes() {
		n := node
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, cancel := context.WithTimeout(ctx, time.Second * 5)
			defer cancel()

			identifiers, err := m.requestDatasetIdentifiers(n.GetIfritAddress(), c)
			if err != nil {
				log.Errorln(err)
				return
			}

			ids <-identifiers
		}()
	}

	wg.Wait()
	close(ids)

	select {
	case id := <-ids:
		jsonOutput.Sets = append(jsonOutput.Sets, id...)
	case <-ctx.Done():
		log.Println(ctx.Err())
	}
	
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
		log.Printf("Fetching dataset identifiers from %s failed\n", addr)
	}

	return nil, nil
}

// Fetches the information about a dataset
func (m *Mux) datasetMetadata(dataset string, ctx context.Context) ([]byte, error) {
	node, ok := m.datasetMap()[dataset]
	if !ok {
		return nil, fmt.Errorf("No such dataset '%s' in network", dataset)
	}

	msg := &pb.Message{
		Type:        message.MSG_TYPE_GET_DATASET_INFO,
		StringValue: dataset,
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
	ch := m.ifritClient.SendTo(node.GetIfritAddress(), data)

	jsonOutput := struct {
		Name     string
		Metadata map[string]string
	}{
		Name:     dataset,
		Metadata: make(map[string]string),
	}

	select {
	case response := <-ch:
		msgResp := &pb.Metadata{}
		if err := proto.Unmarshal(response, msgResp); err != nil {
			return nil, err
		}

		if err := m.verifyMessageSignature(msg); err != nil {
			return nil, err
		}

		for k, v := range msgResp.GetMapField() {
			jsonOutput.Metadata[k] = v
		}

	/*case <-ctx.Done():
		log.Println(ctx.Err())*/
	}

	return json.Marshal(jsonOutput)
}

func (m *Mux) dataset(dataset string, w http.ResponseWriter, ctx context.Context) error {
	nodeAddr, err := m.probeNetworkForDataset(dataset)
	if err != nil {
		return err
	}
	if nodeAddr == "" {
		return errors.New("Dataset is not in network!")
	}

	// Create dataset request
	msg := &pb.Message{
		Type: message.MSG_TYPE_GET_DATASET_URL,
		StringValue: dataset,
	}

	// Marshal the request
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	// Sign it
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
			panic(err)
		}
		
		if err := m.verifyMessageSignature(respMsg); err != nil {
			panic(err)
		}
		
		archiveUrl := "http://" + respMsg.GetStringValue()
		finalURL, err := url.ParseRequestURI(archiveUrl)
		if err != nil {
			panic(err)
		}
		
		if err := m.getZipArchive(w, finalURL); err != nil {
			panic(err)
		}
	}

	return nil
}

func (m *Mux) getZipArchive(w http.ResponseWriter, finalURL *url.URL) error {
	log.Println("URL:", finalURL.String())
	req := &http.Request{
        Method:   "GET",
		URL:      finalURL,
	}

	c := http.Client{
		Timeout: time.Duration(1 * time.Minute),		// TODO: set sane value
	}

	res, err := c.Do(req)
	if err != nil {
		log.Fatalf("%v", err)
	}

	w.WriteHeader(res.StatusCode)
	buf := new(bytes.Buffer)
	buf.ReadFrom(res.Body)
	w.Write(buf.Bytes())
	return nil
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