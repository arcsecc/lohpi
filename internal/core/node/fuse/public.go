package fuse

/** This file contains FUSE methods used by the system to populate the FUSE filesystem
 * and perform other administrative actions otherwise not possible to do from outside these
 * entry points. The invocations originate from the mux's HTTP server
 */

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"io/ioutil"
	"os"

	"github.com/tomcat-bit/lohpi/internal/core/message"
	pb "github.com/tomcat-bit/lohpi/protobuf" 
)

// Based on the contents in 'msg', tell the FUSE daemon to create dummy files and assoicated
// meta-data. The operation is ddempotent; the file that is effected is deleted and rewritten over again
// per each invocation.
func (self *Ptfs) FeedBulkData(load *pb.Load) error {
	study := load.GetStudyName()
	minFiles := load.GetMinfiles()
	maxFiles := load.GetMaxfiles()
	//policy_attributes := msg.MetaData.Meta_data_info.PolicyAttriuteStrings()

	// There can be multiple subjects per NodePopulator
	// TODO: split this up into more functions.
	// TODO: use several go-routines here as well to make it go a little faster!
	for _, subject := range load.GetSubjects() {
		log.Println("kakekakopsdksapodk")
		if err := self.BulkDataRunner(subject, study, minFiles, maxFiles); err != nil {
			return err
		}
	}

	// Store all nescessary meta-data (included the policies assoicated with the study and enrolled subjects)
	if err := self.StoreMetaData(load.GetMetadata(), study); err != nil {
		return err
	}
	return nil
}

// Creates files, policies and meta-data from the incoming NodePopulator
//func (self *Ptfs) BulkDataRunner(subject, study string, minFiles, maxFiles uint32, policy_attributes map[string][]string) error {
	func (self *Ptfs) BulkDataRunner(subject, study string, minFiles, maxFiles uint32) error {
	// Check if subject's data is already stored by FUSE
	if self.subjectExists(subject) {
		fmt.Printf("Subject exists. Continuing...\n")
	} else {
		fmt.Printf("Subject does not exist. Adding a completely new set of node data\n")
		if err := self.createSubject(subject); err != nil {
			return err
		}
	}

	// We need to check if the study exists. If it does not exist, create the
	// msg.Study directory and its default sub-directories. The default directories
	// are not populated with the subject's files.
	if !self.studyExists(study) {
		if err := self.createStudy(study); err != nil {
			return err
		}
	}

	// To begin with, we take care of the "subjects" part of the FUSE file tree.
	// We assume that the subject exists (although ensure that it actually does!).
	// If the given msg's study does not exist, create it and populate the "msg.Study"
	// directory. What "enrollSubjectIntoStudy" is simply add msg.Study into the msg.Subject map
	// Later, we realize those changes on disk -- these are just in-memory states.
	if !self.subjectIsEnrolledInStudy(subject, study) {
		fmt.Printf("Enrolling %s in study %s\n", subject, study)
		if err := self.enrollSubjectIntoStudy(subject, study); err != nil {
			return err
		}
	} else {
		fmt.Printf("Subject %s is already enrolled in study %s\n", subject, study)
	}

	// Used to generate a random number of files, each of which contain arbitrary data
	numFiles := getRandomInt(int(minFiles), int(maxFiles))
	fileSize := getRandomInt(100, 1000)

	// Create the files in the "subjects" part of the FUSE file tree. Any existing files are deleted and replaced by
	// the files given by the msg parameters,
	if err := self.createSubjectStudyFiles(subject, study, numFiles, fileSize); err != nil {
		return err
	}

	// Create the directories in the "studies" part of the file tree
	if err := self.addSubjectStudyFilesToStudy(subject, study, numFiles); err != nil {
		return err
	}

	return nil
}

// Stores the meta-data in a .json file
func (self *Ptfs) StoreMetaData(md *pb.Metadata, study string) error {
	// There exists exactly one JSON file for each study
	// Path where we store the JSON file for a particular study
	jsonPath := fmt.Sprintf("%s/%s/%s/%s/%s", self.mountDir, STUDIES_DIR, study, METADATA_DIR, METADATA_FILE)

	// Overwrite file if it exists
	file, err := os.OpenFile(jsonPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}

	defer file.Close()

	// File writer
	w := bufio.NewWriter(file)
	_, err = w.WriteString(string(md.GetContent()))
	if err != nil {
		return err
	}

	if err := w.Flush(); err != nil {
		return err
	}

	return nil
}

func (self *Ptfs) Studies() []string {
	studies := make([]string, 0)
	for study := range self.studies {
		studies = append(studies, study)
	}
	return studies
}

// Returns the file meta-data assoicated with the study
func (self *Ptfs) StudyMetaData(msg message.NodeMessage) ([]byte, error) {
	// Check if the study is known to the FUSE daemon
	if !self.studyExists(msg.Study) {
		return nil, errors.New("Study does not exist")
	} else {
		fmt.Printf("Study %s exists\n", msg.Study)
	}

	// TODO: file can be too large, so we need an interface for streaming!

	// TODO: do not return the subject permissions -- filter them away!
	jsonPath := fmt.Sprintf("%s/%s/%s/%s/%s", self.mountDir, STUDIES_DIR, msg.Study, METADATA_DIR, METADATA_FILE)
	return ioutil.ReadFile(jsonPath)
}

// Set the policy for all subjects enrolled in the study using the given model
func (self *Ptfs) SetStudyPolicy(study, fileName, model string) error {
	subjects, ok := self.studySubjects[study]
	if !ok {
		return errors.New("Study does not exist")
	}

	for _, s := range subjects {
		if err := self.SetSubjectPolicy(s, study, fileName, model); err != nil {
			panic(err)
		}
	}

	fmt.Printf("studySubjects: %s\n", self.studySubjects)
	fmt.Printf("subjectStudies: %s\n", self.subjectStudies)

	return nil
}
