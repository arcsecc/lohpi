package fuse

/** This file contains FUSE methods used by the system to populate the FUSE filesystem
 * and perform other administrative actions otherwise not possible to do from outside these
 * entry points. The invocations originate from the mux's HTTP server
 */

import (
	"fmt"

	"firestore/core/message"
)
func (self *Ptfs) FeedBulkData(msg *message.NodeMessage) error {
	// Check if subject's data is already stored by FUSE
	if self.subjectExists(msg.Subject) {
		fmt.Printf("Subject exists. Continuing...\n")
	} else {
		fmt.Printf("Subject does not exist. Adding a completely new set of node data\n")
		if err := self.createSubject(msg.Subject); err != nil {
			return err
		}
	}

	// We need to check if the study exists. If it does not exist, create the
	// msg.Study directory and its default sub-directories. The default directories
	// are not populated with the subject's files. 
	if !self.studyExists(msg.Study) {
		self.createStudy(msg.Study)
	}

	// To begin with, we take care of the "subjects" part of the FUSE file tree.
	// We assume that the subject exists (although ensure that it actually does!).
	// If the given msg's study does not exist, create it and populate the "msg.Study"
	// directory. What "enrollSubjectIntoStudy" is simply add msg.Study into the msg.Subject map
	// Later, we realize those changes on disk -- these are just in-memory states.
	if !self.subjectIsEnrolledInStudy(msg.Subject, msg.Study) {
		fmt.Printf("Enrolling %s in study %s\n", msg.Subject, msg.Study)
		self.enrollSubjectIntoStudy(msg.Subject, msg.Study)
	} else {
		fmt.Printf("Subject %s is already enrolled in study %s\n", msg.Subject, msg.Study)
	}

	// Create the files in the "subjects" part of the FUSE file tree. Any existing files are deleted and replaced by
	// the files given by the msg parameters, 
	if err := self.createSubjectStudyFiles(msg.Subject, msg.Study, int(msg.NumFiles), int(msg.FileSize)); err != nil {
		return err
	}

	// Create the directories in the "studies" part of the file tree
	if err := self.addSubjectStudyFilesToStudy(msg.Subject, msg.Study, int(msg.NumFiles)); err != nil {
		return err
	}

	// Create the policies that formalize the access control list parameters enforced by the FUSE.
	// The policies are assigned to "this subject" participating in "this study"
	if err := self.SetSubjectPolicy(msg.Subject, msg.Study, msg.Attributes); err != nil {
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