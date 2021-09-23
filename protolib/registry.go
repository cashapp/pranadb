package protolib

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/meta"
	"github.com/squareup/pranadb/notifier"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/notifications"
	"github.com/squareup/pranadb/table"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

type Resolver protodesc.Resolver

// EmptyRegistry is a resolver that always returns protoregistry.NotFound for all lookups.
var EmptyRegistry = emptyRegistry{}

type emptyRegistry struct{}

func (e emptyRegistry) FindFileByPath(s string) (protoreflect.FileDescriptor, error) {
	return nil, protoregistry.NotFound
}

func (e emptyRegistry) FindDescriptorByName(name protoreflect.FullName) (protoreflect.Descriptor, error) {
	return nil, protoregistry.NotFound
}

var _ Resolver = &emptyRegistry{}

const ProtobufTableName = "protos"

// ProtobufTableInfo is a static definition of the table schema for the table schema table.
var ProtobufTableInfo = &common.MetaTableInfo{TableInfo: &common.TableInfo{
	ID:             common.ProtobufTableID,
	SchemaName:     meta.SystemSchemaName,
	Name:           ProtobufTableName,
	PrimaryKeyCols: []int{0},
	ColumnNames:    []string{"path", "fd"},
	ColumnTypes: []common.ColumnType{
		common.VarcharColumnType,
		common.VarcharColumnType,
	},
}}

var descriptorRowsFactory = common.NewRowsFactory(ProtobufTableInfo.ColumnTypes)

// ProtoRegistry contains all protobuf file descriptors registered with Prana. It first attempts to look up
// file descriptors registered in the cluster storage. If not found, then tries to look up in the directory
// backed registry.
type ProtoRegistry struct {
	mu struct {
		sync.RWMutex
		registry *protoregistry.Files
	}
	loadDir     string
	dirResolver *diskBackedRegistry
	meta        *meta.Controller
	cluster     cluster.Cluster
	queryExec   common.SimpleQueryExec
	notify      func(message notifier.Notification) error
}

// NewProtoRegistry initializes a new file descriptor store. "loadDir" is an optional directory
// to load file descriptor sets from.
func NewProtoRegistry(metaController *meta.Controller, clus cluster.Cluster, queryExec common.SimpleQueryExec, loadDir string) *ProtoRegistry {
	pr := &ProtoRegistry{
		meta:      metaController,
		cluster:   clus,
		queryExec: queryExec,
		loadDir:   loadDir,
	}
	return pr
}

// Start the ProtoRegistry, loading descriptors from disk if configured.
func (s *ProtoRegistry) Start() error {
	schema := s.meta.GetOrCreateSchema("sys")
	schema.PutTable(ProtobufTableInfo.Name, ProtobufTableInfo)

	if s.loadDir != "" {
		r, err := NewDirBackedRegistry(s.loadDir)
		if err != nil {
			return err
		}
		s.dirResolver = r.(*diskBackedRegistry) // nolint: forcetypeassert
	}

	return s.reloadProtobufsFromTable()
}

func (s *ProtoRegistry) reloadProtobufsFromTable() error {
	descByPath, err := s.loadFromTable()
	if err != nil {
		return err
	}
	r, err := buildRegistry(descByPath)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.mu.registry = r
	s.mu.Unlock()

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
	s.mu.RLock()
	defer s.mu.RUnlock()
	fd, err := s.mu.registry.FindDescriptorByName(name)
	if err == protoregistry.NotFound {
		return s.dirResolver.FindDescriptorByName(name)
	}
	return fd, err
}

func (s *ProtoRegistry) FindFileByPath(path string) (protoreflect.FileDescriptor, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	fd, err := s.mu.registry.FindFileByPath(path)
	if err == protoregistry.NotFound {
		return s.dirResolver.FindFileByPath(path)
	}
	return fd, err
}

func (s *ProtoRegistry) RegisterFiles(descriptors *descriptorpb.FileDescriptorSet) error {
	// Ensure the descriptor set is valid and contains all transitive dependencies.
	_, err := protodesc.NewFiles(descriptors)
	if err != nil {
		return err
	}

	wb := cluster.NewWriteBatch(cluster.SystemSchemaShardID, false)
	for _, fd := range descriptors.File {
		if err := table.Upsert(ProtobufTableInfo.TableInfo, encodeDescriptorToRow(fd), wb); err != nil {
			return err
		}
	}
	if err := s.cluster.WriteBatch(wb); err != nil {
		return errors.WithStack(err)
	}

	if err := s.notify(&notifications.ReloadProtobuf{}); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (s *ProtoRegistry) SetNotifier(notify func(message notifier.Notification) error) {
	s.notify = notify
}

func (s *ProtoRegistry) HandleNotification(n notifier.Notification) error {
	return s.reloadProtobufsFromTable()
}

func encodeDescriptorToRow(fd *descriptorpb.FileDescriptorProto) *common.Row {
	rows := descriptorRowsFactory.NewRows(1)
	rows.AppendStringToColumn(0, fd.GetName())
	bin, err := proto.Marshal(fd)
	if err != nil {
		panic(err)
	}
	rows.AppendStringToColumn(1, string(bin))
	row := rows.GetRow(0)
	return &row
}

func (s *ProtoRegistry) loadFromTable() (map[string]*descriptorpb.FileDescriptorProto, error) {
	rows, err := s.queryExec.ExecuteQuery(meta.SystemSchemaName,
		"select path, fd from "+ProtobufTableName)
	if err != nil {
		return nil, err
	}

	protos := make(map[string]*descriptorpb.FileDescriptorProto, rows.RowCount())
	for i := 0; i < rows.RowCount(); i++ {
		row := rows.GetRow(i)
		rawFd := row.GetString(1)
		fd := &descriptorpb.FileDescriptorProto{}
		if err := proto.Unmarshal([]byte(rawFd), fd); err != nil {
			return nil, err
		}
		protos[fd.GetName()] = fd
	}
	return protos, nil
}

type diskBackedRegistry struct {
	*protoregistry.Files
}

// NewDirBackedRegistry recursively walks the directory, looking for protobuf file descriptor sets with the ".bin"
// extension and loading them into memory. A file descriptor set may contain many descriptors. If a descriptor with
// the same full name is seen more than once, the last one wins.
func NewDirBackedRegistry(dir string) (Resolver, error) {
	descByPath := make(map[string]*descriptorpb.FileDescriptorProto)
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
		for _, fd := range fds.File {
			descByPath[fd.GetName()] = fd
		}
		return err
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	r, err := buildRegistry(descByPath)
	if err != nil {
		return nil, err
	}
	return &diskBackedRegistry{Files: r}, nil
}

func buildRegistry(protos map[string]*descriptorpb.FileDescriptorProto) (*protoregistry.Files, error) {
	fds := &descriptorpb.FileDescriptorSet{
		File: make([]*descriptorpb.FileDescriptorProto, 0, len(protos)),
	}
	for _, fd := range protos {
		fds.File = append(fds.File, fd)
	}
	return protodesc.NewFiles(fds)
}
