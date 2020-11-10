package node

import (
	"errors"
	"strings"
	"archive/zip"
	"os"
	"path/filepath"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"fmt"

	pb "github.com/tomcat-bit/lohpi/protobuf" 
	"github.com/tomcat-bit/lohpi/pkg/netutil"
	"github.com/gorilla/mux"
)

const PROJECTS_DIRECTORY = "projects"

func (n *Node) setHttpListener() error {
	if n.config.HttpPort == 0 {
		log.Println("Port number is 0. Will not spawn a new HTTP server.")		
		return nil 
	}

	// Initiate HTTP connection without TLS.
	httpListener, err := netutil.ListenOnPort(n.config.HttpPort)
	if err != nil {
		return err
	}

	n.httpListener = httpListener
	return nil
}

func (n *Node) startHttpHandler() error {
	// Before serving files, remove previous states from disk
	if err := os.RemoveAll(PROJECTS_DIRECTORY); err != nil {
		return err
	}

	router := mux.NewRouter()
	log.Printf("%s: Started HTTP server on port %d\n", n.name, n.config.HttpPort)

	router.HandleFunc("/help", n.httpHelp)
	router.HandleFunc("/objects", n.getObjectHeaders).Methods("GET")
	router.HandleFunc("/upload", n.uploadProject).Methods("POST")
	router.HandleFunc("/remove", n.removeProject).Methods("DELETE")
	router.HandleFunc("/checked-out", n.checkedOutFiles).Methods("GET")

	router.HandleFunc("/subjects", n.getSubjects).Methods("GET") //???

	n.httpServer = &http.Server{
		Handler: router,
		// use timeouts?
	}

	err := n.httpServer.Serve(n.httpListener)
	if err != nil {
		log.Fatalf(err.Error())
	}
	return nil
}

func (n *Node) removeProject(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	w.Header().Set("Access-Control-Request-Method", "DELETE")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Require GET parameters (object key)
	query := r.URL.Query()
	objectKey := query.Get("object_id")
	if objectKey == "" {
		errMsg := errors.New("Missing project/object ID.")
		http.Error(w, http.StatusText(http.StatusBadRequest)+": " + errMsg.Error(), http.StatusBadRequest)
        return
	}
	
	if !n.objectHeaderExists(objectKey) {
		errMsg := fmt.Errorf("No such project '%s'", objectKey)
		http.Error(w, http.StatusText(http.StatusNotFound)+": " + errMsg.Error(), http.StatusNotFound)
        return
	}

	// Check if the files are checked out... return error if some files are
	if err := n.deleteProjectFiles(objectKey); err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError) + ": " + err.Error(), http.StatusInternalServerError)
		return
	}

	n.deleteObjectHeader(objectKey)	
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Successfully removed project '" + objectKey + "' from the storage server\n"))
}

func (n *Node) checkedOutFiles(w http.ResponseWriter, r *http.Request) {
	
}

// TODO: delete me?
func (n *Node) getSubjects(w http.ResponseWriter, r *http.Request) {

}

// Returns the list of nodes 
func (n *Node) getObjectHeaders(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	// TODO: move header values to somewhere else
	// REMOVE when in production
	w.Header().Set("Access-Control-Request-Method", "GET")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	
	w.WriteHeader(http.StatusOK)
	headers := n.objectHeaders()

	for _, h := range headers {
		fmt.Fprintln(w, h.GetName())
	}
}

// HTTP handler for uploading a zip file to the server
func (n *Node) uploadProject(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	w.Header().Set("Access-Control-Request-Method", "POST")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Create directory into which we store the zip files
	if s, err := os.Stat(PROJECTS_DIRECTORY); err != nil {
        if os.IsNotExist(err) {
			if cerr := os.MkdirAll(PROJECTS_DIRECTORY, 0755); cerr != nil {
                log.Fatalf("Creating target: %s", err)
			}
		} else {
			log.Fatalf("Error accessing '%s': %s", PROJECTS_DIRECTORY, err)
		}
	} else if !s.IsDir() {
		log.Fatalf("Target '%s' is not a directory", PROJECTS_DIRECTORY)
	}

	if err := r.ParseMultipartForm(32 << 20); err != nil {
		errMsg := errors.New("Could not read multipart form")
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": " + errMsg.Error(), http.StatusInternalServerError)
		log.Printf("Parsing multipart form: %s", err)
		return
    }
	
	// Require GET parameters (object key)
	query := r.URL.Query()
	objectKey := query.Get("object_id")
	if objectKey == "" {
		errMsg := fmt.Errorf("Missing project identifier")
		http.Error(w, http.StatusText(http.StatusBadRequest)+": " + errMsg.Error(), http.StatusBadRequest)
        return
	}
	
	// If project is already stored on the node, return error
	if n.objectHeaderExists(objectKey) {
		errMsg := fmt.Errorf("Project with ID '%s' already exists", objectKey)
		http.Error(w, http.StatusText(http.StatusConflict)+": " + errMsg.Error(), http.StatusConflict)
        return
	}

	// Fetch the .zip file from the POST request
	f, h, err := r.FormFile("file")
    if err != nil {
    	log.Printf("Error accessing file: %s", err)
        http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
        return
    }
    defer f.Close()

	// Store the file at disk. Create a separate project directory for each project (object)
	projectDir := filepath.Join(PROJECTS_DIRECTORY, objectKey)
	if err := os.Mkdir(projectDir, os.ModePerm); err != nil {
    	log.Println(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Open the output file 
	uploadedZipFilePath := filepath.Join(PROJECTS_DIRECTORY, objectKey, filepath.Base(h.Filename))
    t, err := os.OpenFile(uploadedZipFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
    if err != nil {
		log.Printf("Opening output file: %s", err)
		errMsg := fmt.Errorf("Error uploading zip file to storage node.")
        http.Error(w, http.StatusText(http.StatusInternalServerError) + ": " + errMsg.Error(), http.StatusInternalServerError)
        return
    }
    defer t.Close()

    // Copy the zip file
    if _, err = io.Copy(t, f); err != nil {
		log.Printf("Copying to output file: %s", err)
		errMsg := fmt.Errorf("Error processing uploaded zip file to storage node.")
        http.Error(w, http.StatusText(http.StatusInternalServerError) + ": " + errMsg.Error(), http.StatusInternalServerError)
    	return
	}

	// Inflate .zip file into targetZipDirectory
	targetZipDirectory := strings.TrimSuffix(uploadedZipFilePath, ".zip")
	log.Println("targetZipDirectory:", targetZipDirectory)
	err = n.inflateZipDirectory(uploadedZipFilePath, targetZipDirectory)
	if err != nil {
		log.Println(err.Error())
		errMsg := fmt.Errorf("Error processing uploaded zip file to storage node.")
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+errMsg.Error(), http.StatusInternalServerError)
    	return
	} 

	// 
	inflatedDirectoryName := strings.TrimSuffix(filepath.Base(h.Filename), ".zip")
	inflatedDir := filepath.Join(PROJECTS_DIRECTORY, objectKey, inflatedDirectoryName, inflatedDirectoryName)
	
	// Index the project files in the project directory. Detele everything if it fails.
	if err := n.indexProject(objectKey, inflatedDir); err != nil {
		// Remove everything, starting from the "object key" directory
		e := os.RemoveAll(filepath.Join(PROJECTS_DIRECTORY, objectKey))
    	if e != nil { 
			log.Println(err.Error())
			errMsg := fmt.Errorf("Error processing uploaded zip file to storage node.")
			http.Error(w, http.StatusText(http.StatusInternalServerError) + ": " + errMsg.Error(), http.StatusInternalServerError)
			return
		}
		
		log.Println(err.Error())
		errMsg := fmt.Errorf("Error indexing project in storage node.")
		http.Error(w, http.StatusText(http.StatusBadRequest) + ": " + errMsg.Error(), http.StatusBadRequest)
    	return
	}

	// Remove .zip file
	e := os.Remove(uploadedZipFilePath) 
    if e != nil { 
		log.Println(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		return
	} 
	
	w.WriteHeader(http.StatusCreated)
    w.Write([]byte("Successfully uploaded project directory to the storage server\n"))
}

// Inflates src into dest
func (n *Node) inflateZipDirectory(src, dest string) error {
	exists, err := n.pathExists(dest)
	if err != nil {
		return err
	}

	if exists {
		return fmt.Errorf("Project directory '%s' already exists. Choose another project root directory name", dest)
	}

	// Remove the trailing "zip" pattern from the file name
	var filenames []string

	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}

	defer r.Close()

	for _, f := range r.File {
        // Store filename/path for returning and using later on
        fpath := filepath.Join(dest, f.Name)

        // Check for ZipSlip. More Info: http://bit.ly/2MsjAWE
        if !strings.HasPrefix(fpath, filepath.Clean(dest) + string(os.PathSeparator)) {
            return fmt.Errorf("%s: illegal file path", fpath)
        }

        filenames = append(filenames, fpath)

        if f.FileInfo().IsDir() {
            os.MkdirAll(fpath, os.ModePerm)
            continue
        }

        // Make File
        if err = os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
            return err
        }

        outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
        if err != nil {
            return err
        }

        rc, err := f.Open()
        if err != nil {
            return err
        }

        _, err = io.Copy(outFile, rc)

        // Close the file without defer to close before next iteration of loop
        outFile.Close()
        rc.Close()

        if err != nil {
            return err
        }
    }
    return nil
}

// Indexes the project into the node. It reads the dir directory and performs sanity checks
// before adding it to the note. Returns error if anything went wrong
func (n *Node) indexProject(objectKey, dir string) error {
	log.Println("Looking for 'policy.json' file...")
	
	// Relative path of inflated zip directory
	//relativeDirPath := strings.Split(dir, "/")[1]
	policyFilePath := dir + "/" + "policy.json"
	exists, err := n.pathExists(policyFilePath)
	if err != nil {
		return err
	}
	log.Println("policyFilePath:", policyFilePath)
	if !exists {
		return errors.New("Upload failed: 'policy.json' does not exist in the zip file");
	}

	policyContent, err := ioutil.ReadFile(policyFilePath)
	if err != nil {
		return err
	}

	if n.objectHeaderExists(objectKey) {
		log.Println("Overwriting existing project " + objectKey)
	}

	// Store project data in the node. Use project data name as key 
	n.insertObjectHeader(objectKey, &pb.ObjectHeader{
		Name: 			objectKey, 
		DirectoryName: 	dir, //relativeDirPath,
    	Node: 			n.pbNode(),
		Policy: &pb.Policy{
			Issuer: "client", // overhaul this?
			Content: policyContent,
		},
	})

	// Update the policy store with the newest project list
	if err := n.sendObjectHeaderList(n.PolicyStoreIP); err != nil {
		return err
	}
		
	// MUX TOO
	
	return nil 
}

func (n *Node) httpHelp(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Node help here\n")
	defer r.Body.Close()
}

// Deletes the project files assoicated with objectKey
func (n *Node) deleteProjectFiles(objectKey string) error {
	// Create directory into which we store the zip files
	if _, err := os.Stat(PROJECTS_DIRECTORY); err != nil {
		if os.IsNotExist(err) {
			log.Println(err.Error())
			return err
		} else {
			log.Println(err.Error())
			return err
		}
	}

	if !n.objectHeaderExists(objectKey) {
		return fmt.Errorf("No such object id: '%s'", objectKey)
	}

	projectDir := filepath.Join(PROJECTS_DIRECTORY, objectKey)
	exists, err := n.pathExists(projectDir)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("DOESN'T EXIST"); // TODO: overhaul me
	}

	if err := os.RemoveAll(projectDir); err != nil {
		return err
	}

	return nil
}

func (n *Node) pathExists(path string) (bool, error) {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		} else {
			return false, err
		}
		return true, nil
	}
	return true, nil
}