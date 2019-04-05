FROM golang:latest as build-env

RUN mkdir /app
WORKDIR /app
COPY go.mod .
COPY go.sum .

# Get dependencies - will also be cached if we won't change mod/sum
RUN go mod download
# COPY the source code as the last step
COPY . .
# Build the binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -installsuffix cgo -o /go/bin/app

FROM alpine:3.4
RUN apk add --update --no-cache ca-certificates git
COPY --from=build-env /go/bin/app /go/bin/app
ADD ./cloud/default.json /models/default.json
ADD ./cloud/azure.json /models/azure.json
ENTRYPOINT ["/go/bin/app"]
