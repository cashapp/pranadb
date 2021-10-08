package commands

import (
	"context"
	"fmt"
	"os"

	"github.com/squareup/pranadb/client"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/service"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

type UploadProtoCommand struct {
	FilePath string `arg:"" type:"existingfile" help:"The protobuf file descriptor set to upload"`
}

func (cmd *UploadProtoCommand) Run(cl *client.Client) error {
	data, err := os.ReadFile(cmd.FilePath)
	if err != nil {
		return errors.WithStack(err)
	}
	fd := &descriptorpb.FileDescriptorSet{}
	if err := proto.Unmarshal(data, fd); err != nil {
		return errors.WithStack(err)
	}
	fmt.Println("Registering the following protoufs:")
	for _, f := range fd.File {
		fmt.Printf("\t%s\n", f.GetName())
	}
	fmt.Println()
	if err := cl.RegisterProtobufs(context.Background(), &service.RegisterProtobufsRequest{Descriptors: fd}); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (cmd *UploadProtoCommand) Help() string {
	return `
Prana requires a file descriptor set containing the full transitive closure of
imports for the protobuf. To create one, use protoc, e.g.

	protoc --include_imports --descriptor_set_out=<path_to_descriptor_file> PROTO_FILES
`
}
