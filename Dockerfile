FROM golang:1.19-alpine as base

WORKDIR /app

COPY go.mod ./
COPY go.sum ./

RUN go mod download
RUN go mod verify

COPY *.go ./

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /worker

FROM gcr.io/distroless/static-debian11

COPY --from=base /worker .

CMD [ "./worker container=true" ]