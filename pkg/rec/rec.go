package rec

import (
	"fmt"
	"io/ioutil"
	"log"
	"crypto/x509/pkix"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
	"context"
	"sync"
	"os"
	pb "github.com/tomcat-bit/lohpi/protobuf" 
	empty "github.com/golang/protobuf/ptypes/empty"

	//"github.com/tomcat-bit/lohpi/internal/core/mux"
	//"github.com/tomcat-bit/lohpi/internal/core/policy"
	"github.com/tomcat-bit/lohpi/pkg/comm"
	"github.com/tomcat-bit/lohpi/pkg/netutil"
	"github.com/gorilla/mux"
)

type RecConfig struct {
	MuxIP					string		`default:"127.0.1.1:8081"`
	PolicyStoreIP 			string		`default:"127.0.1.1:8082"`
	LohpiCaAddr 			string		`default:"127.0.1.1:8301"`
	RecIP 					string 		`default:"127.0.1.1:8084"`
	HttpPort				int  		`default:8085`
	UpdatePoliciesPerMinute int 		`default:30`
	UpdateStudiesPerMinute 	int			`default:10`
	RecDirectory			string 		`required:"true"`
}

type service interface {
	Register()
	Handshake()
	SetMetadata(pb.Metadata)
}

type Rec struct {
	name string

	// Crypto
	cu *comm.CryptoUnit
	
	// Configuration for this REC
	config *RecConfig

	// Tickers
	durationPolicyUpdates time.Duration

	// gRPC clients
	muxClient *comm.MuxGRPCClient
	psClient *comm.PolicyStoreGRPCClient

	objectHeadersMap map[string]*pb.ObjectHeader
	objectHeadersLock sync.RWMutex

	httpListener net.Listener
	httpServer   *http.Server

	broadcastPoliciesChan chan bool 
	stopBroadcastingChan chan bool
}

func NewRec(config *RecConfig) (*Rec, error) {
	portStr := strings.Split(config.RecIP, ":")[1]
	port, err := strconv.Atoi(portStr)
	listener, err := netutil.ListenOnPort(port)
	if err != nil {
		return nil, err
	}

	pk := pkix.Name{
		Locality: []string{listener.Addr().String()},
	}
	
	cu, err := comm.NewCu(pk, config.LohpiCaAddr)
	if err != nil {
		return nil, err
	}

	psClient, err := comm.NewPolicyStoreClient(cu.Certificate(), cu.CaCertificate(), cu.Priv())
	if err != nil {
		return nil, err
	}

	// Initiate HTTP connection without TLS.
	httpListener, err := netutil.ListenOnPort(config.HttpPort)
	if err != nil {
		return nil, err
	}

	rec := &Rec{
		name:					"Regional ethics committee",
		config: 				config,
				
		objectHeadersMap:		make(map[string]*pb.ObjectHeader),
		objectHeadersLock:		sync.RWMutex{},
		
		psClient: 				psClient,
		httpListener: 			httpListener,

		broadcastPoliciesChan: 	make(chan bool),
		stopBroadcastingChan:	make(chan bool),
	}
		
	return rec, nil
}

func (r *Rec) Start() {
	go r.HttpHandler()
	go r.broadcastPolicies()
}

func (r *Rec) HttpHandler() error {
	router := mux.NewRouter()
	log.Printf("REC: Started HTTP server on port %d\n", r.config.HttpPort)

	router.HandleFunc("/objects", r.getObjectHeaders).Methods("GET")
	router.HandleFunc("/set_policy", r.setPolicyHandler).Methods("POST")
	router.HandleFunc("/help", r.httpHelp)

	r.httpServer = &http.Server{
		Handler: router,
		// use timeouts?
	}

	err := r.httpServer.Serve(r.httpListener)
	if err != nil {
		log.Fatalf(err.Error())
	}
	return nil
}

// Invoked each time a HTTP client requests information about the studies
func (r *Rec) getObjectHeaders(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	// TODO: move header values to somewhere else
	// REMOVE when in production
	w.Header().Set("Access-Control-Request-Method", "GET")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Connect to policy store
	conn, err := r.psClient.Dial(r.config.PolicyStoreIP)
	if err != nil {
		panic(err)
	}
	
	defer conn.CloseConn()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 20)
	defer cancel()

	headers, err := conn.GetObjectHeaders(ctx, &empty.Empty{})
	if err != nil {
		panic(err)
	}
	
	w.WriteHeader(http.StatusOK)
	for _, h := range headers.GetObjectHeaders() {
		fmt.Fprintln(w, h.GetName())
	}
}

// Invoked each time a new policy is set
func (r *Rec) setPolicyHandler(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()

	w.Header().Set("Access-Control-Request-Method", "POST")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Fetch the project identifier
	query := req.URL.Query()
	objectKey := query.Get("object_id")
	if objectKey == "" {
		errMsg := fmt.Errorf("Missing project identifier from request")
		http.Error(w, http.StatusText(http.StatusBadRequest)+": " + errMsg.Error(), http.StatusBadRequest)
        return
	}

	// Fetch the policy content
	err := req.ParseMultipartForm(32 << 20)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.MultipartForm == nil || req.MultipartForm.File == nil {
		err := fmt.Errorf("Expected policy file in POST request")
		http.Error(w, http.StatusText(http.StatusBadRequest) + ": " + err.Error(), http.StatusBadRequest)
		return
	}

	// Fetch the policy file from the request
	f, _, err := req.FormFile("file")
	if err != nil {
		log.Printf("Error accessing file: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	defer f.Close()

	pFileContents, err := ioutil.ReadAll(f)
	if err != nil {
		panic(err)
	}

	conn, err := r.psClient.Dial(r.config.PolicyStoreIP)
	if err != nil {
		log.Fatalf(err.Error())
	}

	defer conn.CloseConn()
	ctx, cancel := context.WithTimeout(context.Background(), 2 * time.Minute)
	defer cancel()

	_, err = conn.SetPolicy(ctx, &pb.Policy{
		Issuer: "REC",
    	ObjectName: objectKey,
    	Content: pFileContents,
	})
	if err != nil {
		log.Fatalf(err.Error())
	}
/*	
	// Evaluate the study
	objectName := req.PostFormValue("object")
	if !r.objectExists(objectName) {
		fmt.Fprintf(w, "Object %s is not known to REC\n", objectName)
		return
	}

	// Process the policy file
	policyFile, policyFileHeader, err := req.FormFile("policy")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer policyFile.Close()
	
	// Read the multipart file from the client
	buf := bytes.NewBuffer(nil)
	if _, err := io.Copy(buf, policyFile); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	p := &pb.Policy{
		Issuer: 	r.name,
		ObjectName:	objectName,
		Filename:	policyFileHeader.Filename,
		Content:	buf.Bytes(),
	}

	// Send the new policy to the policy store
	if err := r.publishStudyPolicy(p); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	// Store the policy in the map
	r.setObjectPolicy(objectName, p)

	if err := r.storeObjectHeader(p); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}*/
}

func (r *Rec) startBroadcastPoliciesHandler(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()

	r.broadcastPoliciesChan <- true
}

func (r *Rec) stopBroadcastPoliciesHandler(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	log.Println("kake")
	fmt.Fprintf(w, "OK!")
	r.stopBroadcastingChan <- true
}

func (r *Rec) broadcastPolicies() {
	for {
		select {
		case <-r.broadcastPoliciesChan:
			for {
				select {
				case <- r.stopBroadcastingChan:
					log.Println("Breaking from inner loop")
					break
				}
				log.Println("Running!")
			}
		case <- r.stopBroadcastingChan:
			log.Println("Stopping stuff")
		}
	}
}

// Performs a handshake, given the node. 
func (r *Rec) Handshake(ctx context.Context, node *pb.Node) (*pb.HandshakeResponse, error) {
	hr := &pb.HandshakeResponse{}
	return hr, nil
}

// Prints verbose information about REC to the HTTP client
func (r *Rec) httpHelp(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `The REC process is a running entity that pushes study-centric policies to the policy store. 
	Whenever a node is explicitly loaded with study data and a policy,
	REC is anounced using the study name, study subjects and policy. REC then stores this data.
	The following HTTP end-points are avaiable:` + "\n\n")
	fmt.Fprintf(w, `/studies -- GET method` + "\n" + `Returns a list of 2-tuples, where the first element is the study name	and the second element is the policy in effect.` + "\n\n")
	fmt.Fprintf(w, `/study/set_policy -- POST method` + "\n\n" + `Requires a study name and a policy file as parameters.
	Use multipart form (-F in cURL) to do this` + "\n")
}

// Stores the given study in REC, along with the metadata and the policy
func (r *Rec) StoreObjectHeader(ctx context.Context, objectHeader *pb.ObjectHeader) (*empty.Empty, error) {
	// Creates the given study 
	r.insertObjectHeader(objectHeader)

	r.setObjectPolicy(objectHeader.GetName(), objectHeader.GetPolicy())
	
	// Store the default policy
	if err := r.storeObjectHeader(objectHeader.GetPolicy()); err != nil {
		panic(err)
	}

	return &empty.Empty{}, nil
}

// Stores the given metadata on disk
// Not used
func (r *Rec) storeMetadata(study string, metadata []byte) error {
	filePath := r.config.RecDirectory + "/" + "metadata" + "/" + study + ".json"
	mdFile, err := os.Create(filePath)

	if err != nil {
		return err
	}

	_, err = mdFile.Write(metadata)
	if err != nil {
		return err
	}

	err = mdFile.Close()
	if err != nil {
		return err
	}
	return nil 
}

// Stores the given policy on disk
func (r *Rec) storeObjectHeader(p *pb.Policy) error {
	filePath := r.config.RecDirectory + "/" + "policies" + "/" + p.GetObjectName() + ".json"
	mdFile, err := os.Create(filePath)

	if err != nil {
		return err
	}

	_, err = mdFile.Write(p.GetContent())
	if err != nil {
		return err
	}

	err = mdFile.Close()
	if err != nil {
		return err
	}
	return nil 
}

// Sends the given policy to the policy store
func (r *Rec) publishStudyPolicy(p *pb.Policy) error {
	conn, err := r.psClient.Dial(r.config.PolicyStoreIP)
	if err != nil {
		log.Fatalf(err.Error())
	}

	defer conn.CloseConn()
	ctx, cancel := context.WithTimeout(context.Background(), 2 * time.Minute)
	defer cancel()

	_, err = conn.SetPolicy(ctx, p)
	if err != nil {
		log.Fatalf(err.Error())
	}

	return nil
}

// TODO: create a nice public library for these operations :)
func (r *Rec) setupDirectoryTree(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0755); err != nil {
			return err
		}
	}
	return nil 
}

func (r *Rec) Stop() {
	// Stop grpc and http server...
}