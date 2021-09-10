package protolib

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// ProtoRegistry is an in-memory store for protobuf file descriptor sets
type ProtoRegistry struct {
	// TODO: This is a single global registry where descriptors can be added but not updated. We'll
	// need to update this to support updating protos.
	Registry *protoregistry.Files
	dir      string
}

// NewProtoRegistry initializes a new file descriptor store. "dir" is an optional directory
// to load file descriptor sets from.
func NewProtoRegistry(dir string) *ProtoRegistry {
	return &ProtoRegistry{dir: dir, Registry: &protoregistry.Files{}}
}

// Start the ProtoRegistry, loading descriptors from disk if configured.
func (s *ProtoRegistry) Start() error {
	if s.dir != "" {
		return LoadFromDirectory(s.Registry, s.dir)
	}
	return nil
}

// Stop the ProtoRegistry
func (s *ProtoRegistry) Stop() error {
	return nil
}

// FindDescriptorByName looks up a descriptor by the full name.
//
// This returns (nil, NotFound) if not found.
func (s *ProtoRegistry) FindDescriptorByName(name protoreflect.FullName) (protoreflect.Descriptor, error) {
	return s.Registry.FindDescriptorByName(name)
}

// LoadFromDirectory recursively walks the directory, looking for protobuf file descriptor sets with the ".bin"
// extension and loading them into memory. A file descriptor set may contain many descriptors. If a descriptor with
// the same full name is seen more than once, the first one wins.
func LoadFromDirectory(r *protoregistry.Files, dir string) error {
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			if strings.HasPrefix(d.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".bin") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		fds := &descriptorpb.FileDescriptorSet{}
		if err := proto.Unmarshal(data, fds); err != nil {
			return err
		}
		files, err := protodesc.NewFiles(fds)
		if err != nil {
			return err
		}
		files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
			if ierr := r.RegisterFile(fd); ierr != nil {
				msg := ierr.Error()
				// If a proto file with the same filename and/or package path is registered more than once, the
				// first instance is used. This can easily happen if file descriptor sets contain the transitive
				// closure of imports.
				if strings.Contains(msg, "already registered") || strings.Contains(msg, "conflict") {
					return true
				}
				err = ierr
				return false
			}
			return true
		})
		return err
	})
	return errors.WithStack(err)
}
