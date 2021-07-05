.PHONY: all
all: protos

.PHONY: protos
protos:
	make -C protos

.PHONY: clean
clean:
	make -C protos clean
