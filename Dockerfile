FROM golang:latest
EXPOSE 9001

EXPOSE 8080

RUN  mkdir -p /go/src \
  && mkdir -p /go/bin \
  && mkdir -p /go/pkg
ENV GOPATH=/go
ENV PATH=$GOPATH/bin:$PATH   

RUN mkdir -p $GOPATH/src/app 
RUN mkdir -p /models
ADD ./cloud/default.json /models/default.json
ADD ./cloud/azure.json /models/azure.json
ADD . $GOPATH/src/app
ADD ./cloud/ /go/src/app/vendor/github.com/kubecost/cost-model/cloud/
ADD ./costmodel/ /go/src/app/vendor/github.com/kubecost/cost-model/costmodel/

WORKDIR $GOPATH/src/app 
RUN go build -o myapp . 
CMD ["/go/src/app/myapp"]
