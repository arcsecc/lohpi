package mux

import (
	"archive/zip"
	"errors"
_	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
_	"context"
	"sync"
	"time"
	"os"
	"io"

	"github.com/tomcat-bit/lohpi/pkg/message"
	pb "github.com/tomcat-bit/lohpi/protobuf"
	"github.com/golang/protobuf/proto"
	"github.com/gorilla/mux"
	logging "github.com/inconshreveable/log15"
)

// TODO: need more fields here (number of files, etc...)
type datasetResponse struct {
	Name string
}

func (m *Mux) HttpHandler() error {
	r := mux.NewRouter()
	log.Printf("MUX: Started HTTP server on port %d\n", m.config.MuxHttpPort)

	// Public methods exposed to data users (usually through cURL)
	r.HandleFunc("/network", m.network).Methods("GET")

	// List all datasets
	r.HandleFunc("/datasets", m.getNetworkDatasetIdentifiers).Methods("GET")

	// List metadata of a particular dataset
	r.HandleFunc("/dataset_info/{id}", m.getDatasetMetadata).Methods("GET")

	// Fetch 
	r.HandleFunc("/datasets/{id}", m.getDataset).Methods("GET")

	m.httpServer = &http.Server{
		Handler: r,
		ReadTimeout: time.Duration(5 * time.Minute),
		// graceful shutdown too...
	}

	err := m.httpServer.Serve(m.httpListener)
	if err != nil {
		logging.Error(err.Error())
		return err
	}
	return nil
}

// Lazily fetch objects from all the nodes
func (m *Mux) getNetworkDatasetIdentifiers(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	jsonOutput := struct {
		Sets []string
	}{
		Sets: make([]string, 0),
	}

	var l sync.Mutex

	var wg sync.WaitGroup
	for _, v := range m.StorageNodes() {
		value := v
		wg.Add(1)

		go func() {
			msg := &pb.Message{
				Type: message.MSG_TYPE_GET_DATASET_IDENTIFIERS,
				Sender: m.pbNode(),
			}

			data, err := proto.Marshal(msg)
			if err != nil {
				panic(err)
			}

			r, s, err := m.ifritClient.Sign(data)
			if err != nil {
				panic(err)
			}
	
			msg.Signature = &pb.MsgSignature{R: r, S: s}
			data, err = proto.Marshal(msg)
			if err != nil {
				panic(err)
			}

			ch := m.ifritClient.SendTo(value.GetAddress(), data)
			select {
			case resp := <-ch: 
				respMsg := &pb.Message{}
				if err := proto.Unmarshal(resp, respMsg); err != nil {
					panic(err)
				}

				if err := m.verifyMessageSignature(respMsg); err != nil {
					panic(err)
				}

				l.Lock()
				ids := respMsg.GetStringSlice()
				jsonOutput.Sets = append(jsonOutput.Sets, ids...)
				l.Unlock()
			}
			wg.Done()
		}()
	}

	wg.Wait()
	j, err := json.Marshal(jsonOutput)
	if err != nil {
		// TODO: log error to files
		http.Error(w, http.StatusText(http.StatusInternalServerError) + ": " + err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	w.Write(j)
}

// Fetches the information about a dataset
func (m *Mux) getDatasetMetadata(w http.ResponseWriter, req *http.Request) {
	dataset := mux.Vars(req)["id"]
	if dataset == "" {
		errMsg := fmt.Errorf("Missing project identifier")
		http.Error(w, http.StatusText(http.StatusBadRequest)+": " + errMsg.Error(), http.StatusBadRequest)
        return
	}

	// Ask all nodes if dataset exists

	node, ok := m.datasetMap()[dataset]
	if !ok {
		errMsg := fmt.Errorf("No such dataset '%s' in network", dataset)
		http.Error(w, http.StatusText(http.StatusBadRequest)+": " + errMsg.Error(), http.StatusBadRequest)
        return
	}

	msg := &pb.Message{
		Type: message.MSG_TYPE_GET_DATASET_INFO,
		StringValue: dataset,
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}

	r, s, err := m.ifritClient.Sign(data)
	if err != nil {
		panic(err)
	}
	
	msg.Signature = &pb.MsgSignature{R: r, S: s}
	ch := m.ifritClient.SendTo(node.GetAddress(), data)

	jsonOutput := struct{
		Name string
		Metadata map[string]string
	}{
		Name: dataset,
		Metadata: make(map[string]string),
	}

	select {
	case response := <-ch:
		msgResp := &pb.Metadata{}
		if err := proto.Unmarshal(response, msgResp); err != nil {
			panic(err)
		}
		
		if err := m.verifyMessageSignature(msg); err != nil {
			panic(err)
		}

		for k, v := range msgResp.GetMapField() {
			jsonOutput.Metadata[k] = v
		}
	}

	j, err := json.Marshal(jsonOutput)
	if err != nil {
		// TODO: log error to files
		http.Error(w, http.StatusText(http.StatusInternalServerError) + ": " + err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	w.Write(j)
}

// Returns the address of the Lohpi node that stores the given dataset
func (m *Mux) probeNetworkForDataset(dataset string) string {
	var nodeAddr string

	var wg sync.WaitGroup
	for _, v := range m.StorageNodes() {
		value := v
		wg.Add(1)

		go func() {
			msg := &pb.Message{
				Type: message.MSG_TYPE_DATASET_EXISTS,
				Sender: m.pbNode(),
				StringValue: dataset,
			}

			data, err := proto.Marshal(msg)
			if err != nil {
				panic(err)
			}

			r, s, err := m.ifritClient.Sign(data)
			if err != nil {
				panic(err)
			}
	
			msg.Signature = &pb.MsgSignature{R: r, S: s}
			data, err = proto.Marshal(msg)
			if err != nil {
				panic(err)
			}

			ch := m.ifritClient.SendTo(value.GetAddress(), data)
			select {
			case resp := <-ch: 
				respMsg := &pb.Message{}
				if err := proto.Unmarshal(resp, respMsg); err != nil {
					panic(err)
				}

				if err := m.verifyMessageSignature(respMsg); err != nil {
					panic(err)
				}

				if respMsg.GetBoolValue() {
					nodeAddr = value.GetAddress()
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()

	return nodeAddr
}

// Handler used to fetch an entire dataset. Writes a zip file to the client
func (m *Mux) getDataset(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()

	dataset := mux.Vars(req)["id"]
	if dataset == "" {
		errMsg := fmt.Errorf("Missing project identifier")
		http.Error(w, http.StatusText(http.StatusBadRequest)+": " + errMsg.Error(), http.StatusBadRequest)
        return
	}

	nodeAddr := m.probeNetworkForDataset(dataset)
	if nodeAddr == "" {
		panic(errors.New("Dataset is not in network!"))
	}

	// Open stream to node in order to fetch an entire dataset
	in, replyChan := m.ifritClient.OpenStream(nodeAddr, 100, 100)

	// Create dataset request
	dsRequest := &pb.StreamRequest{
		Type: message.STREAM_DATASET,
		Dataset: dataset,
	} 

	// TODO: sign it 
	data, err := proto.Marshal(dsRequest)
	if err != nil {
		panic(err)
	}

	in <- data
	close(in)

	file := &pb.File{}
	
	var wg sync.WaitGroup
	wg.Add(1)
	fileChan := make(chan *pb.File, 10)

	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"dataset.zip\""))
	w.Header().Set("Content-Type", "application/zip")
	// set content length?
	
	zipWriter := zip.NewWriter(w)
    defer zipWriter.Close()

	go func() {
		defer close(fileChan)

		defer wg.Done()
		for {
			select {
				case resp := <-replyChan:
					if err := proto.Unmarshal(resp, file); err != nil {
						panic(err)
					}

					if file.GetTrailing() {
						return
					}

					if err = defalteZip(zipWriter, file); err != nil {
						panic(err)
					}
				}
			}
	}()

	wg.Wait()

	log.Println("Done")
}

func defalteZip(zipWriter *zip.Writer, file *pb.File) error {
	f, err := os.OpenFile(file.GetFilename(), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
    if err != nil {
        log.Fatal(err)
	}
	
	_, err = f.Write(file.GetContent())
	if err != nil {
		panic(err)
	}

    if err := f.Close(); err != nil {
        log.Fatal(err)
	}

    fileToZip, err := os.Open(file.GetFilename())
    if err != nil {
        return err
    }
    defer fileToZip.Close()

    // Get the file information
    info, err := fileToZip.Stat()
    if err != nil {
        return err
    }

    header, err := zip.FileInfoHeader(info)
    if err != nil {
        return err
    }

    // Using FileInfoHeader() above only uses the basename of the file. If we want
    // to preserve the folder structure we can overwrite this with the full path.
    header.Name = file.GetFilename()

    // Change to deflate to gain better compression
    // see http://golang.org/pkg/archive/zip/#pkg-constants
    header.Method = zip.Deflate

    writer, err := zipWriter.CreateHeader(header)
    if err != nil {
        return err
    }
    _, err = io.Copy(writer, fileToZip)
    return err
}