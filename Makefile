mac:
	GOOS=linux GOARCH=amd64 \
	CC=x86_64-unknown-linux-gnu-gcc \
	CXX=x86_64-unknown-linux-gnu-g++ \
	CGO_LDFLAGS="-L/usr/local/x86_64-linux/lib -lzmq" \
	CGO_ENABLED=1 \
	go build

linux:
	GOOS=linux GOARCH=amd64 \
	CGO_ENABLED=1 \
	go build

run_regtest:
	CGO_ENABLED=1 go run app.go -test=2 -config=./config_regtest.toml