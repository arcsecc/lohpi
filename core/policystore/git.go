package policystore

import (
	"strconv"
	"errors"
	"fmt"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Sets up the Git resources in an already-existing Git repository
func initializeGitRepository(path string) (*git.Repository, error) {
	if path == "" {
		return nil, errors.New("Git repository path needs to be set")
	}

	ok, err := exists(path)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	if !ok {
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			return nil, err
		}

		log.Infoln("Initializing a plain Git repository at", path)
		return git.PlainInit(path, false)
	}

	log.Infoln("Opening a plain Git repository at", path)
	return git.PlainOpen(path)
}

func (ps *PolicyStoreCore) gitStorePolicy(nodeIdentifier, datasetIdentifier string, policy *pb.Policy) error {
	if nodeIdentifier == "" {
		return errors.New("Node identifier must not be empty")
	}

	if datasetIdentifier == "" {
		return errors.New("Dataset identifier must not be empty")
	}

	if err := ps.gitWritePolicy(policy, nodeIdentifier); err != nil {
		log.Errorln(err.Error())
		return err
	}

	if err := ps.gitCommitPolicy(policy, nodeIdentifier); err != nil {
		log.Errorln(err.Error())
		return err
	}
	return nil
}

// Writes the given policy store
func (ps *PolicyStoreCore) gitWritePolicy(p *pb.Policy, nodeIdentifier string) error {
	if nodeIdentifier == "" {
		return errors.New("Node identifier must not be empty")
	}

	if p == nil {
		return errors.New("Policy object must not be nil")
	}

	// Check if directory exists for the given study and subject
	// Use "git-repo/study/subject/policy/policy.conf" paths
	ok, err := exists(ps.config.GitRepositoryPath)
	if err != nil {
		log.Fatalf(err.Error())
		return err
	}

	// Create dataset directory if it doesn't exist
	if !ok {
		if err := os.MkdirAll(ps.config.GitRepositoryPath, os.ModePerm); err != nil {
			log.Errorln(err.Error())
			return err
		}
	}

	// Change cwd to gir repo
	err = os.Chdir(ps.config.GitRepositoryPath)
	if err != nil {
		log.Fatalf(err.Error())
		return err
	}

	// Create new directory for the node. We  require the tree structure
	// to be in the form <storage node>/<dataset identifer>.
	if err := os.MkdirAll(nodeIdentifier, os.ModePerm); err != nil {
		log.Errorln(err.Error())
		return err
	}

	// Special case: replace '/' by '_'
	s := strings.ReplaceAll(p.GetDatasetIdentifier(), "/", "_")

	fPath := fmt.Sprintf("%s/%s", nodeIdentifier, s)

	// If the file exists, overwrite the contents of it. If it doesn not, create and write
	ok, err = exists(fPath)
	if err != nil {
		log.Fatalf(err.Error())
		return err
	}

	var f *os.File

	if ok {
		f, err = os.OpenFile(fPath, os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			log.Errorln(err.Error())
			panic(err)
			return err
		}

		_, err = f.WriteString(strconv.FormatBool(p.GetContent()))
		if err != nil {
			panic(err)
			return err
		}
	} else {
		f, err = os.OpenFile(fPath, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Errorln(err.Error())
			panic(err)
			return err
		}

		_, err = f.WriteString(strconv.FormatBool(p.GetContent()))
		if err != nil {
			panic(err)
			return err
		}
	}
	return err
}

func (ps *PolicyStoreCore) getFilePath(p *pb.Policy) string {
	if p == nil {
		log.Errorln("Policy object must not be nil")
		return ""
	}

	// Special case: replace '/' by '_'
	s := strings.ReplaceAll(p.GetDatasetIdentifier(), "/", "_")
	return filepath.Join(ps.config.GitRepositoryPath, s)
}

// Commit the policy model to the Git repository
func (ps *PolicyStoreCore) gitCommitPolicy(p *pb.Policy, nodeIdentifier string) error {
	if p == nil {
		return errors.New("Policy object must not be nil")
	}

	if nodeIdentifier == "" {
		return errors.New("Node identifier must not be empty")
	}

	// Change cwd to gir repo
	err := os.Chdir(ps.config.GitRepositoryPath)
	if err != nil {
		panic(err)
	}

	// Get the current worktree
	worktree, err := ps.repository.Worktree()
	if err != nil {
		panic(err)
	}

	s := strings.ReplaceAll(p.GetDatasetIdentifier(), "/", "_")
	fPath := fmt.Sprintf("%s/%s", nodeIdentifier, s)

	// Add the file to the staging area
	_, err = worktree.Add(fPath)
	if err != nil {
		return err
	}

	status, err := worktree.Status()
	if err != nil {
		return err
	}

	// Check status and abort commit if staging changes don't differ from HEAD.
	// TODO: might need to re-consider this one! What if we need to reorder commits?
	if status.File(fPath).Staging == git.Untracked {
		if err := worktree.Checkout(&git.CheckoutOptions{}); err != nil {
			return err
		}
		return nil
	}

	// Commit the file, not directory (although we can consider it at a later point)
	c, err := worktree.Commit(fPath, &git.CommitOptions{
		All: true,
		Author: &object.Signature{
			Name: p.GetIssuer(),
			//Email: "john@doe.org",
			When: time.Now(),
		},
		Committer: &object.Signature{
			Name: "Policy store",
			//Email: "john@doe.org",
			When: time.Now(),
		},
	})

	if err != nil {
		return err
	}

	obj, err := ps.repository.CommitObject(c)
	if err != nil {
		return err
	}

	//fmt.Println(obj)
	_ = obj
	return nil
}

// Returns true if the dataset policy is stored in Git, returns false otherwise.
func (ps *PolicyStoreCore) gitDatasetExists(nodeIdentifier, datasetId string) bool {
	if nodeIdentifier == "" {
		log.Errorln("Node identifier must not be empty")
		return false
	}

	if datasetId == "" {
		log.Errorln("Dataset identifier must not be empty")
		return false
	}

	ok, err := exists(ps.config.GitRepositoryPath)
	if err != nil {
		log.Errorln(err.Error())
		return false
	}

	if !ok {
		log.Errorln("Git directory does not exist at", ps.config.GitRepositoryPath)
		return false
	}

	// Change cwd to git repo
	err = os.Chdir(ps.config.GitRepositoryPath)
	if err != nil {
		log.Errorln(err.Error())
		return false
	}

	// Special case: replace '/' by '_'
	s := strings.ReplaceAll(datasetId, "/", "_")

	fPath := fmt.Sprintf("%s/%s", nodeIdentifier, s)

	// If the file exists, overwrite the contents of it. If it doesn not, create and write
	ok, err = exists(fPath)
	if err != nil {
		log.Errorln(err.Error())
		return false
	}

	return ok
}

// Returns the policy of the given dataset identifier
func (ps *PolicyStoreCore) gitGetDatasetPolicy(nodeIdentifier, datasetId string) (string, error) {
	if nodeIdentifier == "" {
		err := fmt.Errorf("Node identifier must not be empty")
		return "", err
	}

	if datasetId == "" {
		err := fmt.Errorf("Dataset identifier must not be empty")
		return "", err
	}

	ok, err := exists(ps.config.GitRepositoryPath)
	if err != nil {
		log.Errorln(err.Error())
		return "", err
	}

	if !ok {
		log.Errorln("Git directory does not exist at", ps.config.GitRepositoryPath)
		return "", err
	}

	// Change cwd to git repo
	err = os.Chdir(ps.config.GitRepositoryPath)
	if err != nil {
		log.Errorln(err.Error())
		return "", err
	}

	// Special case: replace '/' by '_'
	s := strings.ReplaceAll(datasetId, "/", "_")

	fPath := fmt.Sprintf("%s/%s", nodeIdentifier, s)

	// If the file exists, overwrite the contents of it. If it doesn not, create and write
	ok, err = exists(fPath)
	if err != nil {
		log.Errorln(err.Error())
		return "", err
	}

	str, err := ioutil.ReadFile(fPath)
	if err != nil {
		return "", err
	}

	return string(str), nil
}
