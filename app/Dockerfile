FROM golang:1.22 AS builder

ENV GOPROXY=https://proxy.golang.org,direct
ENV GOSUMDB=sum.golang.org

WORKDIR /build

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY cmd/main.go .
COPY types ./types/

RUN go build -v -o ./main ./main.go

###########START NEW IMAGE###################
FROM ubuntu:jammy AS production

LABEL Author="George Leonard (georgelza@gmail.com)"

WORKDIR /app

COPY --from=builder /build/main .

CMD ["/app/main", "avro"]
