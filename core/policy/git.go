package policy

import (
	"io/ioutil"
	"fmt"
	"strings"
	"errors"
	"path/filepath"
	"os"
	log "github.com/sirupsen/logrus"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"time"
	pb "github.com/arcsecc/lohpi/protobuf"
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

func (ps *PolicyStore) gitStorePolicy(nodeIdentifier, datasetIdentifier string, policy *pb.Policy) error {
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
func (ps *PolicyStore) gitWritePolicy(p *pb.Policy, nodeIdentifier string) error {
	// Check if directory exists for the given study and subject
	// Use "git-repo/study/subject/policy/policy.conf" paths 
	ok, err := exists(ps.config.PolicyStoreGitRepository)
	if err != nil {
		log.Fatalf(err.Error())
		return err
	}

	// Create dataset directory if it doesn't exist
	if !ok {
		if err := os.MkdirAll(ps.config.PolicyStoreGitRepository, os.ModePerm); err != nil {
			log.Errorln(err.Error())
			return err
		}
	}

	// Change cwd to gir repo
	err = os.Chdir(ps.config.PolicyStoreGitRepository)
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
	s := strings.ReplaceAll(p.GetObjectIdentifier(), "/", "_")

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
			return err
		}

		_, err = f.WriteString(p.GetContent())
		if err != nil {
			return err
		}
	} else {
		f, err = os.OpenFile(fPath, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
	    	log.Errorln(err.Error())
			return err
		}

		_, err = f.WriteString(p.GetContent())
		if err != nil {
			return err
		}
	}
	return err
}

func (ps *PolicyStore) getFilePath(p *pb.Policy) string {
	// Special case: replace '/' by '_'
	s := strings.ReplaceAll(p.GetObjectIdentifier(), "/", "_")
	return filepath.Join(ps.config.PolicyStoreGitRepository, s)
}

// Commit the policy model to the Git repository
func (ps *PolicyStore) gitCommitPolicy(p *pb.Policy, nodeIdentifier string) error {
	// Change cwd to gir repo
	err := os.Chdir(ps.config.PolicyStoreGitRepository)
	if err != nil {
    	panic(err)
	}

	// Get the current worktree
	worktree, err := ps.repository.Worktree()
	if err != nil {
		panic(err)
	}

	s := strings.ReplaceAll(p.GetObjectIdentifier(), "/", "_")
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
			When:  time.Now(),
		},
		Committer: &object.Signature{
			Name: "Policy store",
			//Email: "john@doe.org",
			When:  time.Now(),
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

func (ps *PolicyStore) gitDatasetExists(nodeIdentifier, datasetId string) bool {
	ok, err := exists(ps.config.PolicyStoreGitRepository)
	if err != nil {
		log.Errorln(err.Error())
		return false
	}

	if !ok {
		log.Errorln("Git directory does not exist at", ps.config.PolicyStoreGitRepository)
		return false
	}

	// Change cwd to git repo
	err = os.Chdir(ps.config.PolicyStoreGitRepository)
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

func (ps *PolicyStore) gitGetDatasetPolicy(nodeIdentifier, datasetId string) (string, error) {
	ok, err := exists(ps.config.PolicyStoreGitRepository)
	if err != nil {
		log.Errorln(err.Error())
		return "", err
	}

	if !ok {
		log.Errorln("Git directory does not exist at", ps.config.PolicyStoreGitRepository)
		return "", err
	}

	// Change cwd to git repo
	err = os.Chdir(ps.config.PolicyStoreGitRepository)
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
