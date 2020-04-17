package message

/** This API is used for message passing between mux and storage nodes */
import (
	_"fmt"
	"errors"
//	"bytes"
	//"firestore/core/file"
)

type MsgType string 

// Different message types
const (
	MSG_TYPE_GET_NODE_INFO 		= "MSG_TYPE_GET_NODE_INFO"
	MSG_TYPE_LOAD_NODE 			= "MSG_TYPE_LOAD_NODE"
	MSG_TYPE_MUX_HANDSHAKE 		= "MSG_TYPE_MUX_HANDSHAKE"
	MSG_TYPE_GET_STUDY_LIST 	= "MSG_TYPE_GET_STUDY_MAP"
	MSG_TYPE_GET_DATA 			= "MSG_TYPE_GET_DATA"
	MSG_TYPE_SET_MUX_IP 		= "MSG_TYPE_SET_MUX_IP"
	MSG_TYPE_OK 				= "MSG_TYPE_OK"
	MSG_TYPE_MONITORING_NODE	= "MSG_TYPE_MONITORING_NODE" 
)

// Required format of the bulk data creator 
const (
	Node 					= "node"
	Subject 				= "subject"
	Study 					= "study"
	Required_attributes 	= "required_attributes"
	Country 				= "country"
	Research_network 		= "research_network"
	Purpose 				= "purpose"
	Num_files 				= "num_files"
	File_size 				= "file_size"
)

var errInvalidMessageType = errors.New("Invalid message type. Expected map[string]interface{}")

type BulkDataCreator struct {
	MessageType MsgType
	Node 	string 							`json:"node"`
	Subject string 							`json:"subject"`
	Study string 							`json:"study"`
	Attributes map[string]interface {}	 	`json:"allowed_attributes"`
	DefaultAccess bool  					`json:"default_access, omitempty"`
	NumFiles float64 						`json:"num_files"`
	FileSize float64 						`json:"file_size"`
}

// Type used to pass messages between the nodes. Always check the MsgType before
// processing it any further
type NodeMessage struct {
	MessageType			MsgType
	Node				string 					`json:"node"`
	Subject				string 					`json:"subject"`
	Study				string 					`json:"study"`
	Attributes			map[string]string	 	`json:"allowed_attributes"`
	NumFiles			float64 				`json:"num_files, omitempty"`
	FileSize			float64 				`json:"file_size"`
	MuxIP				string
}

