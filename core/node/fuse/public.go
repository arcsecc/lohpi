package fuse

/** This file contains FUSE methods used by the system to populate the FUSE filesystem
 * and perform other administrative actions otherwise not possible to do from outside these
 * entry points. The invocations originate from the mux's HTTP server
 */

import (
	"fmt"

	"firestore/core/message"
)
func (self *Ptfs) FeedBulkData(bulk *message.BulkDataCreator) error {
	// Check if subject's data is already stored by FUSE
	if self.subjectExists(bulk.Subject) {
		fmt.Printf("Subject exists. Continuing...\n")
	} else {
		fmt.Printf("Subject does not exist. Adding a completely new set of node data\n")
		if err := self.createSubject(bulk.Subject); err != nil {
			return err
		}
	}

	// We need to check if the study exists. If it does not exist, create the
	// bulk.Study directory and its default sub-directories. The default directories
	// are not populated with the subject's files. 
	if !self.studyExists(bulk.Study) {
		self.createStudy(bulk.Study)
	}

	// To begin with, we take care of the "subjects" part of the FUSE file tree.
	// We assume that the subject exists (although ensure that it actually does!).
	// If the given bulk's study does not exist, create it and populate the "bulk.Study"
	// directory.
	if !self.subjectIsEnrolledInStudy(bulk.Subject, bulk.Study) {
		fmt.Printf("Enrolling %s in study %s\n", bulk.Subject, bulk.Study)
		self.enrollSubjectIntoStudy(bulk.Subject, bulk.Study)
	} else {
		fmt.Printf("Subject %s is already enrolled in study %s\n", bulk.Subject, bulk.Study)
	}

	//fmt.Printf("KAKASPASKPADOSK: %v\n", bulk.Attributes)

	// Create the files in the "subjects" part of the FUSE file tree
	if err := self.createSubjectStudyFiles(
		bulk.Subject, 
		bulk.Study, 
		int(bulk.NumFiles), 
		int(bulk.FileSize)); err != nil {
		return err
	}

	// Create the directories in the "studies" part of the file tree
	if err := self.addSubjectStudyFilesToStudy(bulk.Subject, bulk.Study, int(bulk.NumFiles), ); err != nil {
		return err
	}

	return nil
}

