CONTAINER_NAME=shader-worker

build:
	go build -o out/

run:
	docker build -t $(CONTAINER_NAME) .
	docker run --env-file .env --name $(CONTAINER_NAME) $(CONTAINER_NAME)
	docker rm $(CONTAINER_NAME)