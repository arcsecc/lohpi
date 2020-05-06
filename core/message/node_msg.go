package message

/** This API is used for message passing between mux and storage nodes */
import (
	"encoding/json"
	"errors"
//	"bytes"
	//"firestore/core/file"
)

type MsgType string 

// Different message types to be used internally in the Lohpi network
const (
	MSG_TYPE_GET_NODE_INFO 			= "MSG_TYPE_GET_NODE_INFO"
	MSG_TYPE_LOAD_NODE 				= "MSG_TYPE_LOAD_NODE"
	MSG_TYPE_MUX_HANDSHAKE 			= "MSG_TYPE_MUX_HANDSHAKE"
	MSG_TYPE_GET_STUDY_LIST 		= "MSG_TYPE_GET_STUDY_MAP"
	MSG_TYPE_GET_DATA 				= "MSG_TYPE_GET_DATA"
	MSG_TYPE_GET_META_DATA 			= "MSG_TYPE_GET_META_DATA"
	MSG_TYPE_SET_MUX_IP 			= "MSG_TYPE_SET_MUX_IP"
	MSG_TYPE_OK 					= "MSG_TYPE_OK"
	MSG_TYPE_MONITORING_NODE		= "MSG_TYPE_MONITORING_NODE" 
	MSG_TYPE_CHANGE_SUBJECT_POLICY 	= "MSG_TYPE_CHANGE_SUBJECT_POLICY"
	MSG_TYPE_SET_STUDY_POLICY 		= "MSG_TYPE_SET_STUDY_POLICY"
	MSG_TYPE_ERROR 					= "MSG_TYPE_ERROR"
	MSG_TYPE_SET_SUBJECT_POLICY 	= "MSG_TYPE_SET_SUBJECT_POLICY"
	MSG_TYPE_SET_REC_POLICY			= "MSG_TYPE_SET_REC_POLICY"
)

// Required format of the bulk data creator 
const (
	Node 					= "node"
	Subjects 				= "subjects"
	Study 					= "study"
	Required_attributes 	= "required_attributes"
	Country 				= "country"
	Research_network 		= "research_network"
	Purpose 				= "purpose"
	Num_files 				= "num_files"
	File_size 				= "file_size"
)

var errInvalidMessageType = errors.New("Invalid message type. Expected map[string]interface{}")

// Type used to pass messages between the nodes. Always check the MsgType before
// processing it any further
type NodeMessage struct {
	MessageType			MsgType
	Node				string 		
	Subject				string 		
	Study				string 		
	Attributes			map[string][]string
	NumFiles			float64 	
	FileSize			float64 	
	MuxIP				string
	Populator			*NodePopulator
	Filename 			string
	Extras 				[]byte
}

func (nm *NodeMessage) Encode() ([]byte, error) {
	return json.Marshal(nm)
}