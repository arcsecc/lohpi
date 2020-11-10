package rec

// Common operations in REC are implemented here

import (
	"fmt"

	pb "github.com/tomcat-bit/lohpi/protobuf" 
)

// Adds the given study to the internal maps. The embedded subjects are added as well
func (r *Rec) insertObjectHeader(objectHeader *pb.ObjectHeader) {
	// Add study to internal memory map. Assign the subjects to that study.
	// Performs a complete overwrite of the data structures
	r.objectHeadersLock.Lock()
	defer r.objectHeadersLock.Unlock()
	r.objectHeadersMap[objectHeader.GetName()] = objectHeader
}

func (r *Rec) setObjectPolicy(objectId string, p *pb.Policy) error {
	// Fetch object header
	r.objectHeadersLock.RLock()
	h, exists := r.objectHeadersMap[objectId]
	if !exists {
		r.objectHeadersLock.RUnlock()
		return fmt.Errorf("Project with ID '%s' does not exist")
	}
	r.objectHeadersLock.RUnlock()
	
	h.Policy = p
	r.insertObjectHeader(h)
	return nil 
}

func (r *Rec) objectHeaders() map[string]*pb.ObjectHeader {
	r.objectHeadersLock.RLock()
	defer r.objectHeadersLock.RUnlock()
	return r.objectHeadersMap
}

func (r *Rec) objectHeaderExists(id string) bool {
	r.objectHeadersLock.RLock()
	defer r.objectHeadersLock.RUnlock()
	_, exists := r.objectHeadersMap[id]
	return exists
}