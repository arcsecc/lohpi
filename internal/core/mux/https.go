package mux
/*
import (
	"fmt"
	"log"
	"net/http"
	"errors"

	"github.com/gorilla/mux"
	logging "github.com/inconshreveable/log15"
	pb "github.com/tomcat-bit/lohpi/protobuf"
	"github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/net/context"
)

func (m *Mux) HttpsHandler() error {
	log.Printf("MUX: Started HTTPS server on port %d\n", m.portNum)
	r := mux.NewRouter()

	// Utilities used in experiments
	r.HandleFunc("/network", m.network)
	//mux.HandleFunc("/get_study_data", m.GetStudyData)

	m.httpServer = &http.Server{
		Handler:   r,
		TLSConfig: m.serverConfig,
	}

	err := m.httpServer.ServeTLS(m.listener, "", "")
	if err != nil {
		logging.Error(err.Error())
		return err
	}
	return nil
}

func (m *Mux) StudyList(context.Context, *empty.Empty) (*pb.Studies, error) {
	m.cache.FetchRemoteStudyLists()
	studies := &pb.Studies{
		Studies: make([]*pb.Study, 0),
	}

	for s := range m.cache.Studies() {
		study := pb.Study{
			Name: s,
		}

		studies.Studies = append(studies.Studies, &study)
	}

	//studies.Studies = append(studies.Studies, &pb.Study{Name: "kake",})
	return studies, nil
}

func (m *Mux) Handshake(ctx context.Context, node *pb.Node) (*pb.IP, error) {
	if !m.cache.NodeExists(node.GetName()) {
		m.cache.UpdateNodes(node.GetName(), node.GetAddress())
	} else {
		errMsg := fmt.Sprintf("Node '%s' already exists in network\n", node.GetName())
		return nil, errors.New(errMsg)
	}
	log.Printf("Added %s to map with IP %s\n", node.GetName(), node.GetAddress())
	return &pb.IP{
		Ip: m.ifritClient.Addr(),
	}, nil
}

func (m Mux) StudyMetadata(context.Context, *pb.Study) (*pb.Metadata, error) {
	return nil, nil 
}

// Given a study, GetStudyData returns a stream of files available to the
// requester. The invoker
/*func (m *Mux) GetStudyData(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodGet {
		http.Error(w, "Expected GET method", http.StatusMethodNotAllowed)
		return
	}

	//attrMap := map[string]string
	fmt.Printf("INFO: %s\n", r.TLS.PeerCertificates[0])
}*/
