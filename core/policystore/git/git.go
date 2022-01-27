package git

import (
	"errors"
	"fmt"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/go-git/go-git/v5"
	_ "github.com/go-git/go-git/v5/plumbing/object"
	log "github.com/sirupsen/logrus"
	_ "io/ioutil"
	"os"
	_ "path/filepath"
	_ "strings"
	_ "time"
)

var (
	ErrNoDatasetIdentifier = errors.New("Dataset identifier cannot be an empty string")
	ErrNoPolicy            = errors.New("Policy cannot be nil")
	ErrNoNodeIdentifier    = errors.New("Node identifier cannot be an empty string")
)

type GitRepository struct {
	gitRepo     *git.Repository
	repoDirPath string
}

var logFields = log.Fields{
	"package":     "core/policy/git",
	"description": "git repository",
}

// Sets up the Git resources in an already-existing Git repository
func NewGitRepository(path string) (*GitRepository, error) {
	if path == "" {
		return nil, errors.New("Git repository path needs to be set")
	}

	var repo *git.Repository

	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			log.WithFields(logFields).Error(err.Error())
			return nil, err
		}

		log.WithFields(logFields).Infof("Initializing a plain Git repository at path %s", path)
		repo, err = git.PlainInit(path, false)
		if err != nil {
			log.WithFields(logFields).Error(err.Error())
			return nil, err
		}
	}

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		fileInfo, err := os.Stat(path)
		if err != nil {
			log.WithFields(logFields).Error(err.Error())
			return nil, err
		}
		if fileInfo.IsDir() {
			log.Infoln("Opening a plain Git repository at", path)
			repo, err = git.PlainOpen(path)
			if err != nil {
				log.WithFields(logFields).Error(err.Error())
				return nil, err
			}
		} else {
			err := fmt.Errorf("Path '%s' is not a directory")
			log.WithFields(logFields).Error(err.Error())
			return nil, err
		}
	}

	return &GitRepository{
		gitRepo:     repo,
		repoDirPath: path,
	}, nil
}

// NOTE: finish me
func (g *GitRepository) StorePolicy(nodeIdentifier, datasetIdentifier string, policy *pb.Policy) error {
	return nil
	if nodeIdentifier == "" {
		return ErrNoNodeIdentifier
	}

	if datasetIdentifier == "" {
		return ErrNoDatasetIdentifier
	}

	if policy == nil {
		return ErrNoPolicy
	}

	if err := g.writePolicy(policy, nodeIdentifier); err != nil {
		log.Errorln(err.Error())
		return err
	}

	if err := g.commitPolicy(policy, nodeIdentifier); err != nil {
		log.Errorln(err.Error())
		return err
	}
	return nil
}

// Writes the given policy store
func (g *GitRepository) writePolicy(p *pb.Policy, nodeIdentifier string) error {
	/*
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
		}*/
	//return err
	return nil
}

func (g *GitRepository) getFilePath(p *pb.Policy) string {
	/*	if p == nil {
			log.Errorln("Policy object must not be nil")
			return ""
		}

		// Special case: replace '/' by '_'
		s := strings.ReplaceAll(p.GetDatasetIdentifier(), "/", "_")
		return filepath.Join(ps.config.GitRepositoryPath, s)*/
	return ""
}

// Commit the policy model to the Git repository
func (g *GitRepository) commitPolicy(p *pb.Policy, nodeIdentifier string) error {
	/*
		if p == nil {
			return errors.New("Policy object must not be nil")
		}

		if nodeIdentifier == "" {
			return errors.New("Node identifier must not be empty")
		}

		dir, err := os.Getwd()
		if err != nil {
			return err
		}

		// Change cwd to gir repo
		if err := os.Chdir(ps.config.GitRepositoryPath); err != nil {
			panic(err)
		}

		// Change back to the original directory when cleaning up
		defer func() {
			if err := os.Chdir(dir); err != nil {
				panic(err)
			}
		}()

		// Get the current worktree
		worktree, err := ps.repository.Worktree()
		if err != nil {
			panic(err)
		}

		s := strings.ReplaceAll(p.GetDatasetIdentifier(), "/", "_")

		fPath := fmt.Sprintf("%s/%s", nodeIdentifier, s)
		// BUG: what if the name contains "_"?
		// Add the file to the staging area
		_, err = worktree.Add(fPath)
		if err != nil {
			panic(err)
			return err
		}

		status, err := worktree.Status()
		if err != nil {
			panic(err)
			return err
		}

		// Check status and abort commit if staging changes don't differ from HEAD.
		// TODO: might need to re-consider this one! What if we need to reorder commits?
		if status.File(fPath).Staging == git.Untracked {
			if err := worktree.Checkout(&git.CheckoutOptions{}); err != nil {
				panic(err)
				return err
			}
			return nil
		}

		// Commit the file, not directory (although we can consider it at a later point)
		c, err := worktree.Commit(fPath, &git.CommitOptions{
			All: true,
			Author: &object.Signature{
				Name: "Policy store",
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
			panic(err)
			return err
		}

		obj, err := ps.repository.CommitObject(c)
		if err != nil {
			panic(err)
			return err
		}

		//fmt.Println(obj)
		_ = obj*/
	return nil
}

// Returns true if the dataset policy is stored in Git, returns false otherwise.
func (g *GitRepository) gitDatasetExists(nodeIdentifier, datasetId string) bool {
	/*if nodeIdentifier == "" {
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

	return ok*/
	return false
}

// Returns the policy of the given dataset identifier
func (g *GitRepository) gitGetDatasetPolicy(nodeIdentifier, datasetId string) (string, error) {
	/*if nodeIdentifier == "" {
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
	*
		return string(str), nil*/
	return "", nil
}

func directoryExists(path string) bool {
	if info, err := os.Stat(path); err == nil && info.IsDir() {
		return true
	}
	return false
}

func fileExists(path string) bool {
	if info, err := os.Stat(path); err == nil && !info.IsDir() {
		return true
	}
	return false
}
