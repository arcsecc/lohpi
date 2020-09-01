/** Contains a diverse collection of utilities meant for the FUSE to do
 * trivial operartions (lookups and the like)
 */

package fuse

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"time"

	"github.com/casbin/casbin"
	"github.com/casbin/casbin/model"
	_ "github.com/casbin/casbin/persist/file-adapter"
)

var errSubjectExists = errors.New("Subject already exists")

// Creates a directory tree from the root to 'dirPath' if it does not exist
func CreateDirectory(dirPath string) error {
	if _, err := os.Stat(dirPath); err != nil {
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			fmt.Println("DIRRRR:", dirPath)
			return err
		}
	}
	return nil
}

// Deletes the entire directory tree, excluding "dirPath"
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

// Returns true if the subject is known to the filesystem, returns false otherwise
func (self *Ptfs) subjectExists(subject string) bool {
	if _, ok := self.subjectStudies[subject]; ok {
		return true
	}
	return false
}

func (self *Ptfs) createSubject(subjectID string) error {
	self.subjectStudies[subjectID] = make([]string, 0)
	dirPath := fmt.Sprintf("%s/%s/%s/%s", self.mountDir, SUBJECTS_DIR, subjectID, STUDIES_DIR)
	return CreateDirectory(dirPath)
}

func (self *Ptfs) studyExists(study string) bool {
	if _, ok := self.studies[study]; ok {
		return true
	}
	return false
}

// Adds 'study' to the set of known studies for this node. It also creatres the directories
// needed to store this study.
func (self *Ptfs) createStudy(study string) error {
	// Add the study string to the map of known studies
	self.studies[study] = ""
	dirPath := fmt.Sprintf("%s/%s/%s", self.mountDir, STUDIES_DIR, study)
	if err := CreateDirectory(dirPath); err != nil {
		return err
	}

	dirPath = fmt.Sprintf("%s/%s/%s/%s", self.mountDir, STUDIES_DIR, study, DATA_USER_DIR)
	if err := CreateDirectory(dirPath); err != nil {
		return err
	}

	dirPath = fmt.Sprintf("%s/%s/%s/%s", self.mountDir, STUDIES_DIR, study, METADATA_DIR)
	if err := CreateDirectory(dirPath); err != nil {
		return err
	}

	dirPath = fmt.Sprintf("%s/%s/%s/%s", self.mountDir, STUDIES_DIR, study, PROTOCOL_DIR)
	if err := CreateDirectory(dirPath); err != nil {
		return err
	}

	dirPath = fmt.Sprintf("%s/%s/%s/%s", self.mountDir, STUDIES_DIR, study, SUBJECTS_DIR)
	if err := CreateDirectory(dirPath); err != nil {
		return err
	}

	return nil
}

// Returns true if the subject is enrolled in the study, returns false otherwise
func (self *Ptfs) subjectIsEnrolledInStudy(subject, study string) bool {
	for _, s := range self.subjectStudies[subject] {
		if study == s {
			return true
		}
	}
	return false
}

func (self *Ptfs) enrollSubjectIntoStudy(subject, study string) error {
	// Assign the study to the set of studies the given subject is enrolled in
	self.subjectStudies[subject] = append(self.subjectStudies[subject], study)
	self.studySubjects[study] = append(self.studySubjects[study], subject)

	// Create the directories in the "subjects" side of the tree
	dirPath := fmt.Sprintf("%s/%s/%s/%s/%s", self.mountDir, SUBJECTS_DIR, subject, STUDIES_DIR, study)
	if err := CreateDirectory(dirPath); err != nil {
		return err
	}
	return nil
}

// Re-writes a subject's files and assigns them to a study. This is already done in the
// in-memory maps. Now we reflect those states on disk
// missing attrs parameter... TOOD: add them (and a proper struct somewhere as well!)
func (self *Ptfs) createSubjectStudyFiles(subject, study string, numFiles, fileSize int) error {
	// First, create the actual files in the "subjects/bulk.Subject/studies/bulk.Study" directory
	dirPath := fmt.Sprintf("%s/%s/%s/%s/%s", self.mountDir, SUBJECTS_DIR, subject, STUDIES_DIR, study)

	// Delete all files to set a clean state of the file tree
	if err := deleteDirectoryTree(dirPath); err != nil {
		return err
	}

	for i := 1; i <= numFiles; i++ {
		filePath := fmt.Sprintf("%s/file_%d", dirPath, i)
		studyFile, err := os.Create(filePath)

		if err != nil {
			return err
		}

		fileContents := make([]byte, fileSize)
		_, err = rand.Read(fileContents)
		if err != nil {
			return err
		}

		// BUG: file won't write
		n, err := studyFile.Write(fileContents)
		if err != nil {
			fmt.Printf("Should write %d bytes -- wrote %d instead\n", fileSize, n)
		}
		err = studyFile.Close()
		if err != nil {
			fmt.Printf("Closing file... %s\n", err)
			return err
		}
	}
	return nil
}

func (self *Ptfs) addSubjectStudyFilesToStudy(subject, study string, numFiles int) error {
	dirPath := fmt.Sprintf("%s/%s/%s/%s/%s", self.mountDir, STUDIES_DIR, study, SUBJECTS_DIR, subject)

	// Need a clean directory to begin with
	pathToDelete := fmt.Sprintf("%s/%s/%s/%s/%s", self.mountDir, STUDIES_DIR, study, SUBJECTS_DIR, subject)
	if err := deleteDirectoryTree(pathToDelete); err != nil {
		return err
	}

	if err := CreateDirectory(dirPath); err != nil {
		return err
	}

	filePathDir := fmt.Sprintf("%s/%s/%s/%s/%s", self.mountDir, SUBJECTS_DIR, subject, STUDIES_DIR, study)
	targetPathDir := fmt.Sprintf("%s/%s/%s/%s/%s", self.mountDir, STUDIES_DIR, study, SUBJECTS_DIR, subject)
	for i := 1; i <= numFiles; i++ {
		filePath := fmt.Sprintf("%s/file_%d", filePathDir, i)
		targetPath := fmt.Sprintf("%s/file_%d", targetPathDir, i)
		if err := os.Symlink(filePath, targetPath); err != nil {
			return err
		}
	}
	return nil
}

// Set the policy for a study which contains files owned by the subject.
// Expanded explanation: there are many policies per study. Each policy describes the
// access permissions assoicated with a study that is granted by a subject.
// In terms of Casbin, each policy objects contains the following model:
/*
	- Request definition: r = sub, obj, act
	- Policy definition: p = sub, obj, act
	- Policy effect: e = some(where (p.eft == allow))
	- m = r.sub.Attr_1 == p.sub.Attr_1 && r.sub.Attr_2 == p.sub.Attr_2 \
		r.sub.Attr_3 == p.sub.Attr_3


	Each model describes the relation between the study and the subject's policy
	enforced by the node.
*/
func (self *Ptfs) SetSubjectPolicy(subject, study, filename, modelText string) error {
	fmt.Println("Setting new subject-study policy...")

	if !self.subjectIsEnrolledInStudy(subject, study) {
		errMsg := fmt.Sprintf("Subject '%s' does not exist in study '%s'", subject, study)
		return errors.New(errMsg)
	}

	m := model.Model{}
	m.LoadModelFromText(modelText)

	// Write model text to disk and use the adapter
	modelFilePath := fmt.Sprintf("%s/%s/%s/%s/%s/%s", self.mountDir, STUDIES_DIR, study, SUBJECTS_DIR, subject, filename)
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
	self.subjectPolicies[subject] = make(map[string]*casbin.Enforcer)
	self.subjectPolicies[subject][study] = e
	return nil
}

// Return a number in the range [a, b]
func getRandomInt(a, b int) int {
	rand.Seed(time.Now().UnixNano())
	return a + rand.Intn(b-a+1)
}
