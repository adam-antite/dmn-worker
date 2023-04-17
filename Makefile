build:
	go build -o out/

docker:
	docker build -t shader-worker .
	docker run --env-file .env --name shader-worker shader-worker
	docker rm shader-worker