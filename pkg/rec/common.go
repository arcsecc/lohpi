package rec

// Common operations in REC are implemented here

import (
	pb "github.com/tomcat-bit/lohpi/protobuf" 
)

// Adds the given study to the internal maps. The embedded subjects are added as well
func (r *Rec) createObject(objectHeader *pb.ObjectHeader) {
	// Add study to internal memory map. Assign the subjects to that study.
	// Performs a complete overwrite of the data structures
	r.objectSubjectsLock.Lock()
	if r.objectSubjects[objectHeader.GetName()] == nil {
		r.objectSubjects[objectHeader.GetName()] = make([]string, 0)
		r.objectSubjects[objectHeader.GetName()] = append(r.objectSubjects[objectHeader.GetName()], objectHeader.GetMetadata().GetSubjects()...)
	} else {
		for _, sub := range objectHeader.GetMetadata().GetSubjects() {
			if !r.subjectInObject(sub, objectHeader.GetName()) {
				r.objectSubjects[objectHeader.GetName()] = append(r.objectSubjects[objectHeader.GetName()], sub)
			}
		}
	}
	r.objectSubjectsLock.Unlock()
}

func (r *Rec) enrollSubjects(objectHeader *pb.ObjectHeader) {
	r.subjectObjectsLock.Lock()
	defer r.subjectObjectsLock.Unlock()
	for _, o := range objectHeader.GetMetadata().GetSubjects() {
		if _, ok := r.subjectObjects[o]; !ok {
			r.subjectObjects[o] = make([]string, 0)
		}
		r.subjectObjects[o] = append(r.subjectObjects[o], objectHeader.GetName())
	}
}

func (r *Rec) setObjectPolicy(objectName string, p *pb.Policy) {
	r.objectPoliciesLock.Lock()
	defer r.objectPoliciesLock.Unlock()
	r.objectPolicies[objectName] = p
}

func (r *Rec) objectSubjectsMap() map[string][]string {
	r.objectSubjectsLock.RLock()
	defer r.objectSubjectsLock.RUnlock()
	return r.objectSubjects
}

func (r *Rec) objectPoliciesMap() map[string]*pb.Policy {
	r.objectPoliciesLock.RLock()
	defer r.objectPoliciesLock.RUnlock()
	return r.objectPolicies
}

func (r *Rec) subjectStudiesMap() map[string][]string {
	r.subjectObjectsLock.RLock()
	defer r.subjectObjectsLock.RUnlock()
	return r.subjectObjects
}

func (r *Rec) objectExists(study string) bool {
	m := r.objectPoliciesMap()
	_, exists := m[study]
	return exists
}

func (r *Rec) subjectInObject(subject, objectName string) bool {
	subjects := r.objectSubjectsMap()[objectName]
	for _, s := range subjects {
		if s == subject {
			return true
		}
	}
	return false
}