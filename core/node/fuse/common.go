/** Contains a diverse collection of utilities meant for the FUSE to do 
 * trivial operartions (lookups and the like)
 */

package fuse

import (
	"fmt"
	"os"
	"errors"
	"math/rand"
	"io/ioutil"
	"path"
)

var errSubjectExists = errors.New("Subject already exists")

 // Creates a directory tree from the root to 'dirPath' if it does not exist
func CreateDirectory(dirPath string) error {
	fmt.Printf("Dir path: %s\n", dirPath)
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, 0755); err != nil {
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
	if _, ok := self.subjects[subject]; ok {
		return true
	}
	return false
}

func (self *Ptfs) createSubject(subjectID string) error {
	self.subjects[subjectID] = make([]string, 0)
	dirPath := fmt.Sprintf("%s/%s/%s/%s", self.mountDir, SUBJECTS_DIR, subjectID, STUDIES_DIR)
	return CreateDirectory(dirPath)
}

func (self *Ptfs) studyExists(study string) bool {
	if _, ok := self.studies[study]; ok {
		return true
	}
	return false
}

func (self *Ptfs) createStudy(study string) error {
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
	for _, s := range self.subjects[subject] {
		if study == s {
			return true
		}
	}
	return false
}

func (self *Ptfs) enrollSubjectIntoStudy(subject, study string) {
	// Assign the study to the set of studies the given subject is enrolled in
	self.subjects[subject] = append(self.subjects[subject], study)

	// Create the directories in the "subjects" side of the tree
	dirPath := fmt.Sprintf("%s/%s/%s/%s/%s", self.mountDir, SUBJECTS_DIR, subject, STUDIES_DIR, study)
	if err := CreateDirectory(dirPath); err != nil {
		panic(err)
	}
}

// Re-writes a subject's files and assigns them to a study. This is already done in the 
// in-memory maps. Now we reflect those states on disk
// missing attrs parameter... TOOD: add them (and a proper struct somewhere as well!)
func (self *Ptfs) createSubjectStudyFiles(subject, study string, numFiles, fileSize int) error {
	// First, create the actual files in the "subjects/bulk.Subject/studies/bulk.Study" directory

	dirPath := fmt.Sprintf("%s/%s/%s/%s/%s", self.mountDir, SUBJECTS_DIR, subject, STUDIES_DIR, study)
	
	// Delete all files to set a clean state of the file tree
	// NOTE: is this really needed?
	if err := deleteDirectoryTree(dirPath); err != nil {
		return err
	}

	for i := 1; i <= numFiles; i++ {
		filePath := fmt.Sprintf("%s/file_%d", dirPath, i)
		studyFile, err := os.Create(filePath)

		if err != nil {
			panic(err)
		}
	
		fileContents := make([]byte, fileSize)
		_, err = rand.Read(fileContents)
		if err != nil { 
			panic(err)
		}
			
		// BUG: file won't write
		n, err := studyFile.Write(fileContents)
		if err != nil {
			fmt.Errorf("Should write %d bytes -- wrote %d instead\n", fileSize, n)
		}
		err = studyFile.Close()
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func (self *Ptfs) addSubjectStudyFilesToStudy(subject, study string, numFiles int) error {
	dirPath := fmt.Sprintf("%s/%s/%s/%s/%s", self.mountDir, STUDIES_DIR, study, SUBJECTS_DIR, subject)
	fmt.Printf("TARGET:: %s\n", dirPath)

	// Need a clean directory to begin with...
	pathToDelete := fmt.Sprintf("%s/%s/%s/%s/%s", self.mountDir, STUDIES_DIR, study, SUBJECTS_DIR, subject)
	fmt.Printf("pathToDelete: %s\n", pathToDelete)
	if err := deleteDirectoryTree(pathToDelete); err != nil {
		panic(err)
	}

	fmt.Printf("Creating dir: %s\n", dirPath)
	if err := CreateDirectory(dirPath); err != nil {
		return err
	}

	filePathDir := fmt.Sprintf("%s/%s/%s/%s/%s", self.mountDir, SUBJECTS_DIR, subject, STUDIES_DIR, study)
	targetPathDir := fmt.Sprintf("%s/%s/%s/%s/%s", self.mountDir, STUDIES_DIR, study, SUBJECTS_DIR, subject)

	fmt.Printf("target path: %s\n", targetPathDir)

	for i := 1; i <= numFiles; i++ {
		filePath := fmt.Sprintf("%s/file_%d", filePathDir, i)
		targetPath := fmt.Sprintf("%s/file_%d", targetPathDir, i)
		if err := os.Symlink(filePath, targetPath); err != nil {
			return err
		}
	}

	return nil
}

/*func (self *Ptfs) assignSubjectStudyFiles(subject, study string, numFiles, fileSize int) error {
	filePathDir := fmt.Sprintf("%s/%s/%s/%s/%s", self.mountDir, SUBJECTS_DIR, subject, STUDIES_DIR, study)
	targetPathDir := fmt.Sprintf("%s/%s/%s/%s/%s", self.mountDir, STUDIES_DIR, study, SUBJECTS_DIR, subject)

	fmt.Printf("target path: %s\n", targetPathDir)

	for i := 1; i <= numFiles; i++ {
		filePath := fmt.Sprintf("%s/file_%d", filePathDir, i)
		targetPath := fmt.Sprintf("%s/file_%d", targetPathDir, i)
		if err := os.Symlink(filePath, targetPath); err != nil {
			return err
		}
	}

	return nil
}*/