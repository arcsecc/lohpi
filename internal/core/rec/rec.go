package rec

import (
	"math/rand"
	"reflect"
	"log"
	"crypto/x509/pkix"
	"strconv"
	"strings"
	"time"
	"context"
	"os"
	pb "github.com/tomcat-bit/lohpi/protobuf" 
	empty "github.com/golang/protobuf/ptypes/empty"

	//"github.com/tomcat-bit/lohpi/internal/core/mux"
	//"github.com/tomcat-bit/lohpi/internal/core/policy"
	"github.com/tomcat-bit/lohpi/internal/comm"
	"github.com/tomcat-bit/lohpi/internal/netutil"
)

type Config struct {
	MuxIP					string		`default:"127.0.1.1:8080"`
	PolicyStoreIP 			string		`default:"127.0.1.1:8082"`
	LohpiCaAddr 			string		`default:"127.0.1.1:8301"`
	RecIP 					string 		`default:"127.0.1.1:8083"`
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
	config *Config

	// Tickers
	durationPolicyUpdates time.Duration
	durationUpdateStudies time.Duration

	// gRPC server
	recServer *gRPCServer

	// gRPC clients
	muxClient *comm.MuxGRPCClient
	psClient *comm.PolicyStoreGRPCClient

	// In-memory map. subject -> list of subjects in this study
	studySubjects map[string][]string

	// Study identifier -> policy object
	studyPolicies map[string]*pb.Policy
}

func NewRec(config *Config) (*Rec, error) {
	ip, err := netutil.LocalIP()
	if err != nil {
		return nil, err
	}

	pk := pkix.Name{
		Locality: []string{ip},
	}

	cu, err := comm.NewCu(pk, config.LohpiCaAddr)
	if err != nil {
		return nil, err
	}

	portStr := strings.Split(config.RecIP, ":")[1]
	port, err := strconv.Atoi(portStr)
	listener, err := netutil.ListenOnPort(port)
	if err != nil {
		return nil, err
	}

	server, err := newRecGRPCServer(cu.Certificate(), cu.CaCertificate(), cu.Priv(), listener)
	if err != nil {
		return nil, err
	}

	muxClient, err := comm.NewMuxGRPCClient(cu.Certificate(), cu.CaCertificate(), cu.Priv())
	if err != nil {
		return nil, err
	}

	psClient, err := comm.NewPolicyStoreClient(cu.Certificate(), cu.CaCertificate(), cu.Priv())
	if err != nil {
		return nil, err
	}

	rec := &Rec{
		name:					"Regional ethics committee",
		config: 				config,
		durationPolicyUpdates: 	time.Minute / time.Duration(config.UpdatePoliciesPerMinute),
		durationUpdateStudies:  time.Minute / time.Duration(config.UpdateStudiesPerMinute),
		recServer: 				server,
		muxClient: 				muxClient, 
		studySubjects:			make(map[string][]string),
		studyPolicies:			make(map[string]*pb.Policy),
		psClient: 				psClient,
	}

	rec.recServer.Register(rec)
		
	return rec, nil
}

func (r *Rec) Start() {
	log.Println("REC running at", r.recServer.Addr())
	go r.recServer.Start()
	
	policyUpdatesTicker := time.Tick(r.durationPolicyUpdates)

	// Setup directories containing the studies and the metadata
	r.setupDirectoryTree(r.config.RecDirectory + "/" + "metadata")
	r.setupDirectoryTree(r.config.RecDirectory + "/" + "policies")

	//for i := 0; i < 100; i++ {
	for {
		select {
		case <-policyUpdatesTicker:
			r.pushPolicy()
		}
	}
}

func (r *Rec) Handshake(ctx context.Context, node *pb.Node) (*pb.HandshakeResponse, error) {
	hr := &pb.HandshakeResponse{}
	return hr, nil
}

// Stores the given study in REC. It does not include the actual data; it only stores metadata
// and sets a default policy.
func (r *Rec) SetStudy(ctx context.Context, study *pb.Study) (*empty.Empty, error) {
	// Add study to internal memory map. Assign the subjects to that study.
	// Performs a complete overwrite of the data structures
	if r.studySubjects[study.GetName()] == nil {
		r.studySubjects[study.GetName()] = make([]string, 0)
		r.studySubjects[study.GetName()] = append(r.studySubjects[study.GetName()], study.GetMetadata().GetSubjects()...)
	} else {
		for _, sub := range study.GetMetadata().GetSubjects() {
			if !r.subjectInStudy(sub, study.GetName()) {
				r.studySubjects[study.GetName()] = append(r.studySubjects[study.GetName()], sub)
			}
		}
	}

	// Store the metadata on stable storage
	if err := r.storeMetadata(study.GetName(), study.GetMetadata().GetContent()); err != nil {
		panic(err)	// use more descriptive error
	}

	// Default policy
	p := r.defaultPolicy(study.GetName())
	
	// Store the default policy
	if err := r.storePolicy(p); err != nil {
		panic(err)
	}

	return &empty.Empty{}, nil
}

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

func (r *Rec) storePolicy(p *pb.Policy) error {
	filePath := r.config.RecDirectory + "/" + "policies" + "/" + p.GetStudyName() + ".json"
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

// Sets the default policy for the study. The version
func (r *Rec) defaultPolicy(study string) *pb.Policy {	
	rand.Seed(time.Now().UnixNano())
	str := RandStringBytes(10)
	log.Println("REC policy:", str)
	p := &pb.Policy{
		Issuer: 	"REC", 
		StudyName: 	study,
		Filename:	study + ".json",
	//	Content: 	[]byte("Default policy from rec"), // set casbin stuff here 
		Content: []byte(string(str)),
	}
	r.studyPolicies[study] = p
	return p
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
    b := make([]byte, n)
    for i := range b {
        b[i] = letterBytes[rand.Intn(len(letterBytes))]
    }
    return string(b)
}

func (r *Rec) subjectInStudy(subject, study string) bool {
	subjects := r.studySubjects[study]
	for _, s := range subjects {
		if s == subject {
			return true
		}
	}
	return false
}

func (r *Rec) pushPolicy() {
	// Fetch a random study and an assoicated policy
	if len(r.studySubjects) < 1 {
		return
	}

	study := r.getRandomStudy()
	log.Printf("Pushing REC policy for '%s'", study)

	conn, err := r.psClient.Dial(r.config.PolicyStoreIP)
	if err != nil {
		log.Fatalf(err.Error())
	}

	defer conn.CloseConn()
	ctx, cancel := context.WithTimeout(context.Background(), 2 * time.Minute)
	defer cancel()

	p := r.studyPolicies[study]

	_, err = conn.SetPolicy(ctx, p)
	if err != nil {
		log.Fatalf(err.Error())
	}
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

// Returns a random study
func (r *Rec) getRandomStudy() string {
	studyIdx := rand.Int() % len(r.studySubjects) 
	studies := reflect.ValueOf(r.studyPolicies).MapKeys()
	return studies[studyIdx].String()
}

func (r *Rec) Shutdown() {

}