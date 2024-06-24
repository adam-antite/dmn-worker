CONTAINER_NAME=shader-worker

build:
	go build -o out/

run:
	docker build -t $(CONTAINER_NAME) .
	docker run --rm --env-file .env --name $(CONTAINER_NAME) $(CONTAINER_NAME)

runlocal:
	go run ./src/*.go
