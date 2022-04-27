PROTOS_DIR = "../../../protos"

all: simple_v2.fds.txt simple_v1.fds.txt simple_v3.fds.txt
	make -C $(PROTOS_DIR)

.PHONY: simple_v2.fds.txt
simple_v2.fds.txt:
	echo "# descriptor for squareup/cash/pranadb/testproto/v1/simple.proto" > $@
	protoc --decode=google.protobuf.FileDescriptorSet noop.proto < $(PROTOS_DIR)/descriptors/squareup/cash/pranadb/testproto/v1/simple.bin >> $@

.PHONY: simple_v1.fds.txt
simple_v1.fds.txt:
	echo "# descriptor for squareup/cash/pranadb/testproto/v1/simple.proto" > $@
	echo "# with field \"val2\" removed for testing." >> $@
	tac simple_v2.fds.txt | sed '/val2/,/}/d' | tac > $@

.PHONY: simple_v3.fds.txt
simple_v3.fds.txt:
	echo "# descriptor for squareup/cash/pranadb/testproto/v1/simple.proto" > $@
	echo "# with field \"val1\" and \"val2\" swapped." >> $@
	tac simple_v2.fds.txt | sed -e 's/val1/tmp/' -e 's/val2/val1/' -e 's/tmp/val2/' | tac > $@
