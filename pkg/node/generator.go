package node

/* This file contains utilities only meant for generating 
 * dummy data. Not to be used in production.
 */
/*
import (
	"encoding/json"
	"bufio"
	"log"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"time"

	pb "github.com/tomcat-bit/lohpi/protobuf" 
	"github.com/pkg/xattr"
	_ "github.com/casbin/casbin/persist/file-adapter"
)

var errSubjectExists = errors.New("Subject already exists")

const NODE_DIR = "storage_nodes"
const DATA_USER_DIR = "data users"
const METADATA_DIR = "metadata"
const PROTOCOL_DIR = "protcol"
const SUBJECTS_DIR = "subjects"
const STUDIES_DIR = "studies"
const METADATA_FILE = "metadata.json"

// Based on the contents in 'msg', tell the FUSE daemon to create dummy files and assoicated
// meta-data. The operation is ddempotent; the file that is effected is deleted and rewritten over again
// per each invocation.
/*
func (n *Node) generateData(load *pb.Load) error {
	objectHeader := load.GetObjectHeader()
	minFiles := load.GetMinfiles()
	maxFiles := load.GetMaxfiles()

	// There can be multiple subjects per NodePopulator
	if err := n.generateObjectData(objectHeader, minFiles, maxFiles); err != nil {
		return err
	}

	// Store all nescessary meta-data (included the policies assoicated with the study and enrolled subjects)
	if err := n.storeMetaData(objectHeader); err != nil {
		return err
	}

	return nil
}

// Creates files, policies and meta-data from the incoming 
func (n *Node) generateObjectData(objectHeader *pb.ObjectHeader, minFiles, maxFiles uint32) error {
	// Object's file path. Remove it if it exists and recreate everything from scratch
	objectDirectoryPath := fmt.Sprintf("%s/%s/%s", n.rootDir, STUDIES_DIR, objectHeader.GetName())
	exists, err := fileExists(objectDirectoryPath)
	if err != nil {
		panic(err)
	}
	if exists {
		if err := deleteDirectoryTree(objectDirectoryPath); err != nil {
			panic(err)
		}			
	}

	// Insert new entry, even if it already exists. Re-create everything from scratch
	n.insertObjectHeader(objectHeader.GetName(), objectHeader)
	if err := n.createObjectFileTree(objectHeader.GetName()); err != nil {
		panic(err)
	}
	
	// For each subject in the study, (re-)create the study directory 
	for _, s := range objectHeader.GetMetadata().GetSubjects() {
		// Rewrite entire map during tests and demonstrations. To be removed later
		n.eraseObjectsFromSubject(s)
		subjectObjectsDirPath := fmt.Sprintf("%s/%s/%s/%s/%s", n.rootDir, SUBJECTS_DIR, s, STUDIES_DIR, objectHeader.GetName())

		n.eraseObjectsFromSubject(objectHeader.GetName())
		n.insertSubject(s, objectHeader.GetName())
		if err := createDirectory(subjectObjectsDirPath); err != nil {
			panic(err)
		}

		// Used to generate a random number of files, each of which contain arbitrary data
		numFiles := getRandomInt(int(minFiles), int(maxFiles))
		fileSize := getRandomInt(100, 1000)
		
		// Create the files in the "subjects" part of the FUSE file tree. Any existing files are deleted and replaced by
		// the files given by the msg parameters,
		if err := n.createSubjectStudyFiles(s, objectHeader.GetName(), numFiles, fileSize); err != nil {
			return err
		}
		
		if err := n.setAccessAttributes(s, objectHeader, numFiles); err != nil {
			return err
		}

		// Create the directories in the "studies" part of the file tree
		if err := n.addSubjectStudyFilesToStudy(s, objectHeader.GetName(), numFiles); err != nil {
			return err
		}
	}

	return nil
}

func (n *Node) setAccessAttributes(subject string, objectHeader *pb.ObjectHeader, numFiles int) error {
	dirPath := fmt.Sprintf("%s/%s/%s/%s/%s", n.rootDir, SUBJECTS_DIR, subject, STUDIES_DIR, objectHeader.GetName())

	for i := 1; i <= numFiles; i++ {
		filePath := fmt.Sprintf("%s/file_%d", dirPath, i)
		
		exists, err := fileExists(filePath)
		if err != nil {
			panic(err)
		}
		if !exists {
			panic(errors.New("File does not exist"))
		}

		if err := xattr.Set(filePath, XATTR_PREFIX + n.attrKey, objectHeader.GetPolicy().GetContent()); err != nil {
			panic(err)
		}
	}

	return nil 
}

// NOTE: we are using only one simple string that represents 
func (n *Node) setExtentedAttributes(subject string, objectHeader *pb.ObjectHeader, numFiles int) error {
	log.Println("xattr:", objectHeader.GetPolicy())

	dirPath := fmt.Sprintf("%s/%s/%s/%s/%s", n.rootDir, SUBJECTS_DIR, subject, STUDIES_DIR, objectHeader.GetName())

	for i := 1; i <= numFiles; i++ {
		filePath := fmt.Sprintf("%s/file_%d", dirPath, i)
		
		exists, err := fileExists(filePath)
		if err != nil {
			panic(err)
		}
		if !exists {
			panic(errors.New("File does not exist"))
		}

		var anyJson map[string]interface{}
		json.Unmarshal(objectHeader.GetPolicy().GetContent(), &anyJson)
		policy := anyJson["policy"].(map[string]interface{})	// TODO: Check if 'policy' is set

		for k, v := range policy {
			// xattr key
			log.Println(k)
			list := v.([]interface{})

			attrs := make([]byte, 0)
			for _, l := range list {
				a, _ := l.(string)
				b := []byte(a)
				attrs = append(attrs, b...)
			}

			// DOES IT WORK?
			if err := xattr.Set(filePath, XATTR_PREFIX+k, attrs); err != nil {
				panic(err)
			}
		}
	}

	return nil 
}

// Stores the meta-data in a .json file
func (n *Node) storeMetaData(objectHeader *pb.ObjectHeader) error {
	//d *pb.Metadata, study string) error {
	objectName := objectHeader.GetName()

	// There exists exactly one JSON file for each study
	// Path where we store the JSON file for a particular study
	jsonPath := fmt.Sprintf("%s/%s/%s/%s/%s", n.rootDir, STUDIES_DIR, objectName, METADATA_DIR, METADATA_FILE)

	// Overwrite file if it exists
	file, err := os.OpenFile(jsonPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}

	defer file.Close()

	// File writer
	w := bufio.NewWriter(file)
	_, err = w.WriteString(string(objectHeader.GetMetadata().GetContent()))
	if err != nil {
		return err
	}

	if err := w.Flush(); err != nil {
		return err
	}

	return nil
}

// Creates a directory tree from the root to 'dirPath' if it does not exist
func createDirectory(dirPath string) error {
	if _, err := os.Stat(dirPath); err != nil {
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			return err
		}
	}
	return nil
}

// Deletes the entire directory tree, excluding 'dirPath'
func deleteDirectoryTree(dirPath string) error {
	ok, err := fileExists(dirPath)
	if !ok && err == nil {
		return nil
	}

	currentDir, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return err
	}

	for _, d := range currentDir {
		if err := os.RemoveAll(path.Join([]string{dirPath, d.Name()}...)); err != nil {
			return err
		}
	}

	return nil
}

// Returns true if 'path' exists, returns false otherwise
func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

// Adds 'object' to the set of known studies for this node. It also creatres the directories
// needed to store this study.
func (n *Node) createObjectFileTree(objectID string) error {
	dirPath := fmt.Sprintf("%s/%s/%s", n.rootDir, STUDIES_DIR, objectID)
	if err := createDirectory(dirPath); err != nil {
		return err
	}

	dirPath = fmt.Sprintf("%s/%s/%s/%s", n.rootDir, STUDIES_DIR, objectID, DATA_USER_DIR)
	if err := createDirectory(dirPath); err != nil {
		return err
	}

	dirPath = fmt.Sprintf("%s/%s/%s/%s", n.rootDir, STUDIES_DIR, objectID, METADATA_DIR)
	if err := createDirectory(dirPath); err != nil {
		return err
	}

	dirPath = fmt.Sprintf("%s/%s/%s/%s", n.rootDir, STUDIES_DIR, objectID, PROTOCOL_DIR)
	if err := createDirectory(dirPath); err != nil {
		return err
	}

	dirPath = fmt.Sprintf("%s/%s/%s/%s", n.rootDir, STUDIES_DIR, objectID, SUBJECTS_DIR)
	if err := createDirectory(dirPath); err != nil {
		return err
	}

	return nil
}

// Returns true if the subject is enrolled in the study, returns false otherwise
func (n *Node) subjectParticipatesInObject(subject, object string) bool {
	subs := n.fs.Subjects()
	for _, o := range subs[subject] {
		if object == o {
			return true
		}
	}
	return false
}

func (n *Node) createSubjectObjectsDirectory(subject, object string) error {
	// Assign the study to the set of studies the given subject is enrolled in
	// Create the directories in the "subjects" side of the tree
	dirPath := fmt.Sprintf("%s/%s/%s/%s/%s", n.rootDir, SUBJECTS_DIR, subject, STUDIES_DIR, object)
	return createDirectory(dirPath)
}

// Return a number in the range [a, b]
func getRandomInt(a, b int) int {
	rand.Seed(time.Now().UnixNano())
	return a + rand.Intn(b - a + 1)
}

// Re-writes a subject's files and assigns them to a study. This is already done in the
// in-memory maps. Now we reflect those states on disk
// missing attrs parameter... TOOD: add them (and a proper struct somewhere as well!)
func (n *Node) createSubjectStudyFiles(subject, objectName string, numFiles, fileSize int) error {
	// First, create the actual files in the "subjects/bulk.Subject/studies/bulk.Study" directory
	dirPath := fmt.Sprintf("%s/%s/%s/%s/%s", n.rootDir, SUBJECTS_DIR, subject, STUDIES_DIR, objectName)

	// Delete all files to set a clean state of the file tree
	if err := deleteDirectoryTree(dirPath); err != nil {
		panic(err)
		return err
	}

	for i := 1; i <= numFiles; i++ {
		filePath := fmt.Sprintf("%s/file_%d", dirPath, i)
		studyFile, err := os.Create(filePath)

		if err != nil {
			panic(err)
			return err
		}

		fileContents := make([]byte, fileSize)
		_, err = rand.Read(fileContents)
		if err != nil {
			panic(err)
			return err
		}

		n, err := studyFile.Write(fileContents)
		if err != nil {
			fmt.Printf("Should write %d bytes -- wrote %d instead\n", fileSize, n)
			panic(err)
			return err
		}
		err = studyFile.Close()
		if err != nil {
			fmt.Printf("Closing file... %s\n", err)
			panic(err)
			return err
		}
	}
	return nil
}

func (n  *Node) addSubjectStudyFilesToStudy(subject, study string, numFiles int) error {
	dirPath := fmt.Sprintf("%s/%s/%s/%s/%s", n.rootDir, STUDIES_DIR, study, SUBJECTS_DIR, subject)

	// Need a clean directory to begin with
	pathToDelete := fmt.Sprintf("%s/%s/%s/%s/%s", n.rootDir, STUDIES_DIR, study, SUBJECTS_DIR, subject)
	if err := deleteDirectoryTree(pathToDelete); err != nil {
		panic(err)
		return err
	}

	if err := createDirectory(dirPath); err != nil {
		panic(err)
		return err
	}

	filePathDir := fmt.Sprintf("%s/%s/%s/%s/%s", n.rootDir, SUBJECTS_DIR, subject, STUDIES_DIR, study)
	targetPathDir := fmt.Sprintf("%s/%s/%s/%s/%s", n.rootDir, STUDIES_DIR, study, SUBJECTS_DIR, subject)
	for i := 1; i <= numFiles; i++ {
		filePath := fmt.Sprintf("%s/file_%d", filePathDir, i)
		targetPath := fmt.Sprintf("%s/file_%d", targetPathDir, i)
		if err := os.Symlink(filePath, targetPath); err != nil {
			panic(err)
			return err
		}
	}
	return nil
}

/*
func (self *Ptfs) SetSubjectPolicy(subject, object, filename, modelText string) error {
	fmt.Println("Setting new subject-study policy...")

	if !n.subjectParticipatesInObject(subject, object) {
		errMsg := fmt.Sprintf("Subject '%s' does not exist in study '%s'", subject, object)
		return errors.New(errMsg)
	}

	m := model.Model{}
	m.LoadModelFromText(modelText)

	// Write model text to disk and use the adapter
	modelFilePath := fmt.Sprintf("%s/%s/%s/%s/%s/%s", n.rootDir, STUDIES_DIR, object, SUBJECTS_DIR, subject, filename)
	log.Println("modelFilePath:", modelFilePath)
	file, err := os.OpenFile(modelFilePath, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}

	defer file.Close()

	_, err = file.Write([]byte(modelText))
	if err != nil {
		panic(err)
	}

	//a := fileadapter.NewAdapter(modelFilePath)
	e := casbin.NewEnforcer(m)
	
	// Create the map containing the policy enforcement.
	// Safely overwrite any previous map entries (and underlying maps)
	// TODO: avoid overwriting?
	// LOCK?
	n.subjectPolicies[subject] = make(map[string]*casbin.Enforcer)
	n.subjectPolicies[subject][object] = e
	return nil
}

// Return a number in the range [a, b]
func getRandomInt(a, b int) int {
	rand.Seed(time.Now().UnixNano())
	return a + rand.Intn(b - a + 1)
}

func (self *Ptfs) ObjectHeaders() {
	return nil 
}

// Returns the file meta-data assoicated with the study
/*func (self *Ptfs) StudyMetaData(msg pb.NodeMessage) ([]byte, error) {
	// Check if the study is known to the FUSE daemon
	if !n.objectExists(msg.Study) {
		return nil, errors.New("Study does not exist")
	} else {
		fmt.Printf("Study %s exists\n", msg.Study)
	}

	// TODO: file can be too large, so we need an interface for streaming!

	// TODO: do not return the subject permissions -- filter them away!
	jsonPath := fmt.Sprintf("%s/%s/%s/%s/%s", n.rootDir, STUDIES_DIR, msg.Study, METADATA_DIR, METADATA_FILE)
	return ioutil.ReadFile(jsonPath)
}*

// Set the policy for all subjects enrolled in the study using the given model
func (self *Ptfs) SetStudyPolicy(study, fileName, model string) error {
	/*subjects, ok := n.studySubjects[study]
	if !ok {
		return nil
	}

	log.Println("study, fileName, model:", study, fileName, model)

	for _, s := range subjects {
		if err := n.SetSubjectPolicy(s, study, fileName, model); err != nil {
			log.Println(err.Error())
		}
	}*

	return nil
}*/
