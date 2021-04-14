package directoryserver

/* This file contains methods that use the Lohpi network for queries */
import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/arcsecc/lohpi/core/message"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/golang/protobuf/proto"
	"github.com/lestrrat-go/jwx/jws"
	log "github.com/sirupsen/logrus"
	"net/http"
	"strconv"
	"time"
)

// Fetches the information about a dataset
func (d *DirectoryServerCore) datasetMetadata(w http.ResponseWriter, req *http.Request, dataset, nodeAddr string, ctx context.Context) ([]byte, error) {
	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Create dataset request
	msg := &pb.Message{
		Type: message.MSG_TYPE_GET_DATASET_METADATA_URL,
		DatasetRequest: &pb.DatasetRequest{
			Identifier:  dataset,
			ClientToken: nil,
		},
	}

	// Marshal the request
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	// Sign it
	r, s, err := d.ifritClient.Sign(data)
	if err != nil {
		return nil, err
	}

	msg.Signature = &pb.MsgSignature{R: r, S: s}
	data, err = proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	ch := d.ifritClient.SendTo(nodeAddr, data)

	select {
	case resp := <-ch:
		respMsg := &pb.Message{}
		if err := proto.Unmarshal(resp, respMsg); err != nil {
			return nil, err
		}

		if err := d.verifyMessageSignature(respMsg); err != nil {
			return nil, err
		}

		return d.getMetadata(w, req, respMsg.GetStringValue(), newCtx)
	case <-newCtx.Done():
		log.Debugln("Timeout in 'func (d *DirectoryServerCore) datasetMetadata()'")
		return nil, errors.New("Timeout while fetching ")
	}

	return nil, nil
}

func (d *DirectoryServerCore) dataset(w http.ResponseWriter, req *http.Request, dataset, nodeAddr string, clientToken []byte, ctx context.Context) {
	// Create dataset request
	msg := &pb.Message{
		Type: message.MSG_TYPE_GET_DATASET_URL,
		DatasetRequest: &pb.DatasetRequest{
			Identifier:  dataset,
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
	r, s, err := d.ifritClient.Sign(data)
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

	ch := d.ifritClient.SendTo(nodeAddr, data)

	select {
	case resp := <-ch:
		respMsg := &pb.Message{}
		if err := proto.Unmarshal(resp, respMsg); err != nil {
			log.Fatalln(err.Error())
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}

		if err := d.verifyMessageSignature(respMsg); err != nil {
			log.Fatalln(err.Error())
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}

		// If dataset is available, send it to the client
		if respMsg.GetDatasetResponse().GetIsAllowed() {
			// If we tried to fetch the dataset but it failed, rollback on checking it out
			if err := d.datasetRequest(w, req, respMsg.GetDatasetResponse().GetURL(), ctx); err != nil {
				if err := d.rollbackCheckout(nodeAddr, dataset, ctx); err != nil {
					log.Errorln(err.Error())
				}
				return
			}

			// Get client-related fields
			_, oid, err := d.getClientIdentifier(clientToken)
			if err != nil {
				if err := d.rollbackCheckout(nodeAddr, dataset, ctx); err != nil {
					log.Errorln(err.Error())
				}
				return
			}

			d.insertCheckedOutDataset(dataset, oid)

		} else {
			err := fmt.Errorf(respMsg.GetDatasetResponse().GetErrorMessage())
			log.Errorln(err.Error())
			http.Error(w, http.StatusText(http.StatusUnauthorized)+": "+err.Error(), http.StatusUnauthorized)
			return
		}
	/*case <-newCtx.Done():
		log.Println(err.Error())
		http.Error(w, http.StatusText(http.StatusRequestTimeout), http.StatusRequestTimeout)
		return*/
	}
}

func (d *DirectoryServerCore) getClientIdentifier(token []byte) (string, string, error) {
	msg, err := jws.ParseString(string(token))
	if err != nil {
		return "", "", err
	}

	s := msg.Payload()
	if s == nil {
		return "", "", errors.New("Payload was nil")
	}

	c := struct {
		Name string `json:"name"`
		Oid  string `json:"oid"`
	}{}

	if err := json.Unmarshal(s, &c); err != nil {
		return "", "", err
	}

	return c.Name, c.Oid, nil
}

// TODO: use context and refine me otherwise
func (d *DirectoryServerCore) getMetadata(w http.ResponseWriter, r *http.Request, remoteUrl string, ctx context.Context) ([]byte, error) {
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

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(response.Body)
	if err != nil {
		return nil, err
	}

	if response.StatusCode == http.StatusOK {
		return buf.Bytes(), nil
	} else {
		log.Errorf("Response from remote data repository: %s\n", string(buf.Bytes()))
	}

	return nil, errors.New("Could not fetch metadata from host")
}

// TODO: use context request
func (d *DirectoryServerCore) datasetRequest(w http.ResponseWriter, req *http.Request, remoteUrl string, ctx context.Context) error {
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
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
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

// TODO: handle ctx
func (d *DirectoryServerCore) rollbackCheckout(nodeAddr, dataset string, ctx context.Context) error {
	msg := &pb.Message{
		Type:        message.MSG_TYPE_ROLLBACK_CHECKOUT,
		Sender:      d.pbNode(),
		StringValue: dataset,
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	r, s, err := d.ifritClient.Sign(data)
	if err != nil {
		return err
	}

	msg.Signature = &pb.MsgSignature{R: r, S: s}
	data, err = proto.Marshal(msg)
	if err != nil {
		return err
	}

	ch := d.ifritClient.SendTo(nodeAddr, data)
	select {
	case resp := <-ch:
		respMsg := &pb.Message{}
		if err := proto.Unmarshal(resp, respMsg); err != nil {
			return err
		}

		if err := d.verifyMessageSignature(respMsg); err != nil {
			return err
		}

	case <-ctx.Done():
		err := errors.New("Could not verify dataset checkout rollback")
		return err
	}

	return nil
}

func (d *DirectoryServerCore) insertCheckedOutDataset(dataset, clientId string) {
	d.clientCheckoutMapLock.Lock()
	defer d.clientCheckoutMapLock.Unlock()
	if d.clientCheckoutMap[dataset] == nil {
		d.clientCheckoutMap[dataset] = make([]string, 0)
	}
	d.clientCheckoutMap[dataset] = append(d.clientCheckoutMap[dataset], clientId)
}

func (d *DirectoryServerCore) getCheckedOutDatasetMap() map[string][]string {
	d.clientCheckoutMapLock.RLock()
	defer d.clientCheckoutMapLock.RUnlock()
	return d.clientCheckoutMap
}

func (d *DirectoryServerCore) datasetIsInvalidated(dataset string) bool {
	l := d.revokedDatasets()

	for e := l.Front(); e != nil; e = e.Next() {
		if e.Value == dataset {
			return true
		}
	}

	return false
}
