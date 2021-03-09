package policy

import (
	//"path"
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
		log.Fatalf(err.Error())
		return nil, err
	}

	if !ok {
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			return nil, err
		}

		log.Infof("Initializing a plain Git repository at '%s'\n", path)
		return git.PlainInit(path, false)
	} else {
		log.Infoln("Policies directory in Git repository already exists at", path)
	}

	log.Infof("Opening a plain Git repository at %s\n", path)
	return git.PlainOpen(path)
}

// Writes the given policy store 
func (ps *PolicyStore) writePolicy(p *pb.Policy) error {
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
    	panic(err)
	}

	// Special case: replace '/' by '_'
	s := strings.ReplaceAll(p.GetObjectIdentifier(), "/", "_")
	
	// If the file exists, overwrite the contents of it. If it doesn not, create and write
	ok, err = exists(s)
	if err != nil {
		log.Fatalf(err.Error())
		return err
	}

	var f *os.File

	if ok {
		f, err = os.OpenFile(s, os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
	    	log.Errorln(err.Error())
			return err
		}

		_, err = f.WriteString(p.GetContent())
	} else {
		f, err = os.OpenFile(s, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
	    	log.Errorln(err.Error())
			return err
		}

		_, err = f.WriteString(p.GetContent())
	}
	return err
}

func (ps *PolicyStore) getFilePath(p *pb.Policy) string {
	// Special case: replace '/' by '_'
	s := strings.ReplaceAll(p.GetObjectIdentifier(), "/", "_")
	return filepath.Join(ps.config.PolicyStoreGitRepository, s)
}

// Commit the policy model to the Git repository
func (ps *PolicyStore) commitPolicy(p *pb.Policy) error {
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

	fPath := strings.ReplaceAll(p.GetObjectIdentifier(), "/", "_")
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

func (ps *PolicyStore) remoteGitSync() error {
	return nil
}

