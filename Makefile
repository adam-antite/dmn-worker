BINARY_NAME=worker

build:
	go mod tidy
	go build -o ./out/${BINARY_NAME}

clean:
	go clean
	rm ${BINARY_NAME}