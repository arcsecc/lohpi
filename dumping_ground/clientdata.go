package main

import (
	"math/rand"
	"crypto/sha1"
	"errors"
)

var (
	errHashClientData 			= errors.New("Could not hash client's data")
)

/** Application data being passed around. When in transit across the network,
 * the timestamp is set. When it is received, the time spent in the network is computed.
*/
type Clientdata struct {
	Blob []byte
	BlobHash string
	NSecInTransit int64
	TransitStart int64
	InTransit bool
	EpochTimestamp int64
	// lock?
}

func NewApplicationClientData(pakcetSize int) (*Clientdata, error) {
	appData := make([]byte, pakcetSize)
	
	_, err := rand.Read(appData)
	if err != nil { 
		return nil, errCreateRandomClientData		
	}

	hash, err := ClientdataHash(appData)
	if err != nil {
		return nil, err
	}
	
	clientData := &Clientdata {
		Blob: appData,
		BlobHash: hash,
		InTransit: false,
	}

return clientData, nil
}

func (cd *Clientdata) GetClientdataBlob() []byte {
	return cd.Blob
}

func ClientdataHash(data []byte) (string, error) {
	hash := sha1.New()
	_, err := hash.Write([]byte(data))
	if err != nil {
		return "", errHashClientData
	}

	return string(hash.Sum(nil)[:]), nil
}

func (cd *Clientdata) DataHash() string {
	return cd.BlobHash
}

func (cd *Clientdata) SetInTransit(transit bool) {
	cd.InTransit = transit
}

func (cd *Clientdata) GetInTransit() bool {
	return cd.InTransit
}

func (cd *Clientdata) SetEpochTimestamp(now int64) {
	cd.EpochTimestamp = now
}

func (cd *Clientdata) GetEpochTimestamp() int64 {
	return cd.EpochTimestamp
}
