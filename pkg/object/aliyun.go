package object

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/K265/aliyundrive-go/pkg/aliyun/drive"
	"github.com/google/uuid"
)

type AliyunStorage struct {
	DefaultObjectStorage
	fs          drive.Fs
	workdir     string
	tempdirID   string
	nodeIDCache sync.Map
	getLock     chan struct{}
	putLock     chan struct{}
}

func (s *AliyunStorage) getNode(path string, createDir bool) (string, error) {
	if v, ok := s.nodeIDCache.Load(path); ok {
		return v.(string), nil
	}
	node, err := s.fs.GetByPath(context.Background(), path, drive.AnyKind)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && createDir {
			nodeID, err := s.fs.CreateFolderRecursively(context.Background(), path)
			if err != nil {
				return "", err
			}
			s.nodeIDCache.Store(path, nodeID)
			return nodeID, nil
		}
		return "", err
	}
	s.nodeIDCache.Store(path, node.NodeId)
	return node.NodeId, nil
}

func (s *AliyunStorage) path(key string) string {
	return filepath.Join(s.workdir, key)
}

func (s *AliyunStorage) Get(key string, offset int64, length int64) (io.ReadCloser, error) {
	s.getLock <- struct{}{}
	defer func() {
		<-s.getLock
	}()
	path := s.path(key)
	log.Println("Get", path)
	nodeID, err := s.getNode(path, false)
	if err != nil {
		return nil, err
	}
	header := map[string]string{}
	if offset > 0 && length == 0 {
		header["Range"] = fmt.Sprintf("bytes=%d", offset)
	}
	if length > 0 {
		header["Range"] = fmt.Sprintf("bytes=%d-%d", offset, offset+length)
	}
	r, err := s.fs.Open(context.Background(), nodeID, header)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (s *AliyunStorage) Put(key string, in io.Reader) error {
	s.putLock <- struct{}{}
	defer func() {
		<-s.putLock
	}()

	path := s.path(key)
	log.Println("Put", path)
	dir, filename := filepath.Split(path)
	dirNodeID, err := s.getNode(dir, true)
	if err != nil {
		return fmt.Errorf("get node: %w", err)
	}
	nodeID, err := s.fs.CreateFile(context.Background(), drive.Node{ParentId: s.tempdirID, Name: uuid.NewString()}, in)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	_, err = s.fs.Move(context.Background(), nodeID, dirNodeID, filename)
	if err != nil {
		err = s.delete(key)
		if err != nil {
			return fmt.Errorf("delete temp file: %w", err)
		}
		_, err = s.fs.Move(context.Background(), nodeID, dirNodeID, filename)
		if err != nil {
			return fmt.Errorf("move temp file: %w", err)
		}
	}
	s.nodeIDCache.Store(path, nodeID)
	return nil
}

func (s *AliyunStorage) delete(key string) error {
	path := s.path(key)
	nodeID, err := s.getNode(path, false)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	s.nodeIDCache.Delete(path)
	return s.fs.Remove(context.Background(), nodeID)
}

func (s *AliyunStorage) Delete(key string) error {
	log.Println("Delete", s.path(key))
	return s.delete(key)
}

func (s *AliyunStorage) String() string {
	return fmt.Sprintf("aliyun://%s/", s.workdir)
}

func newAliyun(endpoint, accessKey, secretKey, token string) (ObjectStorage, error) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	tokenData, err := os.ReadFile("refresh_token")
	if err == nil {
		secretKey = string(tokenData)
	}
	config := &drive.Config{
		RefreshToken: secretKey,
		IsAlbum:      false,
		DeviceId:     accessKey,
		HttpClient:   &http.Client{},
		OnRefreshToken: func(refreshToken string) {
			os.WriteFile("refresh_token", []byte(refreshToken), 0600)
		},
	}
	ctx := context.Background()
	fs, err := drive.NewFs(ctx, config)
	if err != nil {
		return nil, err
	}
	s := AliyunStorage{fs: fs}
	_, err = s.getNode(endpoint, true)
	if err != nil {
		return nil, err
	}
	s.workdir = endpoint
	s.getLock = make(chan struct{}, 2)
	s.putLock = make(chan struct{}, 2)

	// clean temp dir
	tempDir := filepath.Join(s.workdir, ".temp")
	tmp, err := s.getNode(tempDir, false)
	if err == nil {
		s.nodeIDCache.Delete(tempDir)
		err = s.fs.Remove(context.Background(), tmp)
		if err != nil {
			return nil, err
		}
	}
	tmp, err = s.getNode(tempDir, true)
	if err != nil {
		return nil, err
	}
	s.tempdirID = tmp
	return &s, nil
}

func init() {
	Register("aliyun", newAliyun)
}
