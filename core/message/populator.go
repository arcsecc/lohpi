// This file contains structs and utility methods used to successully create
// objects to be passed to the nodes when they are to be populated with data and meta-data.
package message

import (
	"fmt"
	"errors"
	"encoding/json"
	
	"github.com/buger/jsonparser"
)
// This object describes how we generate data at the nodes and assign meta-data to it -- 
// this struct contains all nescessary information to do so. We require at least some fields to be present.
// We allow arbitrary objects to be present as well. 
//
// Please refer to the lohpi/resources/metadata/studies/*.json files.
type NodePopulator struct {
	Node 				string		
	Subjects 			[]string 
	MinFiles, MaxFiles 	int
	MetaData			*MetaData	
}

// This type describes the meta-data assoicated with each study. Note that type MetaData is a subset of
// the type 'NodePopulator'. When we assign metadata to a study, we simply extract the variable of this type
// from a NodePopulator. 
type MetaData struct {
	Extras 					map[string][]byte
	Rec_attr				map[string][]byte
	Data_protection_attr 	map[string][]byte
	Meta_data_info			*MetaDataInfo
}

type MetaDataInfo struct {
	Policies 		map[string][]byte
	Extras 			map[string][]byte
	DataFields 		[]*DataField
}

type DataField struct {
	Name 			string					// mandatory
	FilePattern 	FilePattern				// mandatory
	Extras			map[string][]byte		// optional fields
}

type FilePattern struct {
	Directory 		string
	MultipleFiles 	string
	Extras 			map[string][]byte
}

func NewNodePopulator(input []byte) (*NodePopulator, error) {
	// Get the node
	node, err := jsonparser.GetString(input, "node")
	if err != nil {
		return nil, err
	}

	// Min files
	minFiles, err := jsonparser.GetInt(input, "min-files")
	if err != nil {
		return nil, err
	}

	// Max files
	maxFiles, err := jsonparser.GetInt(input, "max-files")
	if err != nil {
		return nil, err
	}

	// Get all subjects enrolled in this study
	subjects, err := getAllStringsInArray(input, "subjects")
	if err != nil {
		return nil, err
	}

	// All the meta-data
	metaData, err := NewMetaData(input)
	if err != nil {
		return nil, err
	}

	return &NodePopulator{
		Node: 		node,
		Subjects:	subjects,
		MinFiles:	int(minFiles),
		MaxFiles: 	int(maxFiles),
		MetaData:   metaData,
	}, nil
}

func (np *NodePopulator) Encode() ([]byte, error) {
	return json.Marshal(np)
}

func NewMetaData(jsonFile []byte) (*MetaData, error) {
	metaDataInfo, err := NewMetaDataInfo(jsonFile)
	if err != nil {
		return nil, err
	}

	arbitraryMetaDataObjects, err := getObjectsAndRemoveIgnoredObjects(jsonFile, []string{"rec-attributes", "data-protection-attributes", "meta-data-info"}, "meta-data")
	if err != nil {
		return nil, err
	}

	rec_attr, err := getObjectsAndRemoveIgnoredObjects(jsonFile, []string{}, "meta-data", "rec-attributes")
	if err != nil {
		return nil, err
	}

	dataProtectionAttr, err := getObjectsAndRemoveIgnoredObjects(jsonFile, []string{},  "meta-data", "data-protection-attributes")
	if err != nil {
		return nil, err
	}

	return &MetaData{
		Extras: 					arbitraryMetaDataObjects,
		Rec_attr:				rec_attr,
		Data_protection_attr: 	dataProtectionAttr,
		Meta_data_info:			metaDataInfo,
	}, nil
}

func NewMetaDataInfo(jsonFile []byte) (*MetaDataInfo, error) {
	// Get mandatory fields (not 'policy-attributes' and 'data-field')
	var err error
	arbitraryMetaDataInfoObjects, err := getObjectsAndRemoveIgnoredObjects(jsonFile, []string{"policy-attributes", "data-fields"}, "meta-data", "meta-data-info")
	if err != nil {
		return nil, err
	}
	policies, err := getObjectsAndRemoveIgnoredObjects(jsonFile, []string{}, "meta-data", "meta-data-info", "policy-attributes")
	if err != nil {
		return nil, err
	}

	dataFields, err := getDataFields(jsonFile)
	if err != nil {
		return nil, err
	}

	return &MetaDataInfo{
		Policies: 	policies,
		Extras:		arbitraryMetaDataInfoObjects,
		DataFields: dataFields,
	}, nil
}

// Returns a new 'data-field' element in the 'data-fields' array
func NewDataFieldElement(jsonFile []byte, index string, fp *FilePattern) (*DataField, error) {
	other := make(map[string][]byte)
	var name string 

	_ = jsonparser.ObjectEach(jsonFile, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		switch string(key) {
		case "file-pattern":
		case "name":
			if dataType == jsonparser.String {
				name = string(value)
			} else {
				fmt.Println("error")
			}

		default:
			mapKey := string(key)
			other[mapKey] = value
		}
		// TODO name as well
		return nil
	}, "meta-data", "meta-data-info", "data-fields", index)

	return &DataField {
		Name:			name,
		FilePattern: 	*fp,
		Extras: 			other,
	}, nil 
}

// Returns a new 'file-pattern' object
func NewFilePattern(ss []byte) (*FilePattern, error) {
	other := make(map[string][]byte)
	var directory string
	var multipleFiles string

	err := jsonparser.ObjectEach(ss, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		//fmt.Printf("Innter key: '%s'\n Inner Value: '%s'\n Type: %s\n", string(key), string(value), dataType)
		switch string(key) {
		case "directory":
			if dataType == jsonparser.String {
				directory = string(value)
			} else {
				fmt.Println("error")
			}
		case "multiple-files":
			if dataType == jsonparser.String {
				multipleFiles = string(value)
			} else {
				fmt.Println("error")
			}
		default:
			mapKey := string(key)
			other[mapKey] = value
		}
		return nil
	}, "file-pattern")
	
	// Check things here!
	if directory == "" || multipleFiles == "" {
		return nil, errors.New("Error parsing 'file-pattern' object")
	}

	return &FilePattern{
		Directory: 		directory,
		MultipleFiles: 	multipleFiles,
		Extras: 			other,
	}, err
}

// Returns all data fields in "meta-data-info"
func getDataFields(jsonFile []byte) ([]*DataField, error) {
	// The path into 'jsonFile'
	paths := []string{"meta-data", "meta-data-info", "data-fields"}
	dataFields := make([]*DataField, 0)
	
	// Get the objects in data fields 
	count := 0
	idx := "[0]"
	_, err := jsonparser.ArrayEach(jsonFile, func(key []byte, dataType jsonparser.ValueType, offset int, err error) {
		// Fetch the "file-pattern" object
		fp, innerError := NewFilePattern(key)
		if innerError != nil {
			panic(err)
		}

		// Fetch the "data-field" object
		df, innerError := NewDataFieldElement(jsonFile, idx, fp)
		if innerError != nil {
			panic(err)
		}

		count++
		idx = fmt.Sprintf("[%d]", count)
		
		// Append the data-field object to the list
		dataFields = append(dataFields, df)
	}, paths...)
	if err != nil {
		return nil, err
	}

	// TODO get errors from inside callback
	return dataFields, nil
}

// Returns a string array with the values in the path being strings
func getAllStringsInArray(input []byte, path string) ([]string, error) {
	result := make([]string, 0)
	_, err := jsonparser.ArrayEach(input, func(key []byte, dataType jsonparser.ValueType, offset int, err error) {
		if dataType != jsonparser.String {
			err = errors.New("Value is not string")
			return
		}
		result = append(result, string(key))
	}, path)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// Returns a map with arbitrary objects in the path. Ie. the returned map contains objects that are not mandatory 
// to the parsing procedures. The function filters keys in 'paths' which means that deeper complex objects are put in the map.
func getObjectsAndRemoveIgnoredObjects(input []byte, ignoredPaths []string, paths ...string) (map[string][]byte, error) {
	result := make(map[string][]byte)
	err := jsonparser.ObjectEach(input, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		k := string(key)
		result[k] = value
		return nil
	}, 	paths...)
	if err != nil {
		return nil, err
	}

	// Remove filtered entries
	for _, k := range ignoredPaths {
        delete(result, k)
    }

	return result, nil
}