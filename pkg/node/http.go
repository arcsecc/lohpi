package node

import (
_	"errors"
	"strings"
	"archive/zip"
	"os"
	"path/filepath"
	"io"
	"log"
	"net/http"
	"fmt"

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
	router := mux.NewRouter()
	log.Printf("%s: Started HTTP server on port %d\n", n.name, n.config.HttpPort)

	router.HandleFunc("/help", n.httpHelp)
	router.HandleFunc("/objects", n.getObjectHeaders).Methods("GET")
	router.HandleFunc("/subjects", n.getSubjects).Methods("GET")
	router.HandleFunc("/upload", n.uploadProject).Methods("POST")

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

func (n *Node) getSubjects(w http.ResponseWriter, r *http.Request) {

}

func (n *Node) getObjectHeaders(w http.ResponseWriter, r *http.Request) {
//	headers := n.objectHeaders()
}

// TODO: perform sanity checks in this method
func (n *Node) uploadProject(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	
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
    	log.Fatalf("Parsing form: %s", err)
    }
	
	// Fetch the .zip file from the POST request
	f, h, err := r.FormFile("file")
    if err != nil {
    	log.Printf("Error accessing file: %s", err)
        http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
        return
    }
    defer f.Close()

	// Store the file at disk
	zipfilePath := filepath.Join(PROJECTS_DIRECTORY, filepath.Base(h.Filename))
    t, err := os.OpenFile(zipfilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
    if err != nil {
    	log.Printf("Opening output file: %s", err)
        http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
        return
    }
    defer t.Close()

    log.Println("Copying to output file...")
    if _, err = io.Copy(t, f); err != nil {
    	log.Printf("Copying to output file: %s", err)
        http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
    	return
	}

	// Inflate .zip file
	targetDir := strings.TrimSuffix(zipfilePath, ".zip")
	err = n.inflateZipDirectory(zipfilePath, targetDir)
	if err != nil {
		log.Println(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
    	return
	} 
	
	// Index the project files in the project directory. Detele everything if it fails.
	if err := n.indexProject(targetDir); err != nil {
		// detele zipDirFilePath and dirPath
		log.Println(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
    	return
	}

	// Remove .zip file
	e := os.Remove(zipfilePath) 
    if e != nil { 
		log.Println(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
	} 
	
	w.WriteHeader(http.StatusOK)
    w.Write([]byte("Successfully uploaded project directory to the storage server"))
}

// Inflates 'zipDirFilePath' the 'projects' directory
func (n *Node) inflateZipDirectory(src, dest string) error {
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
            // Make Folder
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

// Indexes the project into the node. Returns a verbose error if something went wrong
func (n *Node) indexProject(dir string) error {
	log.Println("Looking for 'policy.conf' file...")
	// TODO: put policy.conf into constant field
	
	exists, err := n.pathExists(dir + "/" + "policy.conf")
	if err != nil {
		return err
	}
	if !exists {
		//return errors.New("Policy file does not exist in zip file");
	}

	

	return nil 
}

func (n *Node) httpHelp(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Node help here\n")
	defer r.Body.Close()
}

func (n *Node) pathExists(path string) (bool, error) {
	if _, err := os.Stat(path	); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		} else {
			return false, err
		}
		return true, nil
	}
	return true, nil
}