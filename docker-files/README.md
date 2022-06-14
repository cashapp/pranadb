### Build docker image.

**PranaDB**
```bash
cd $PROJECT_DIR

docker build -f docker/Dockerfile -t pranadb:latest . 	
```

**Prana CLI**
```bash
cd $PROJECT_DIR

docker build -f docker/cli.Dockerfile -t pranadb-cli:latest .
```
### Run locally.

```bash
cd $PROJECT_DIR

docker run -it --rm -v $PROJECT_DIR/cfg:/etc/ pranadb --config etc/example.conf --node-id 0
```
