VERSION?=v0.0.3-SNAPSHOT
REGISTRY?=gcr.io
PROJECT_ID?=kubecost1
APPNAME?=kube-metrics

release: clean build push clean

build:
	docker build -f "Dockerfile.metrics" -t ${REGISTRY}/${PROJECT_ID}/${APPNAME}:${VERSION} .

push:
	docker push ${REGISTRY}/${PROJECT_ID}/${APPNAME}:${VERSION}

clean:
	docker rm -f ${REGISTRY}/${PROJECT_ID}/${APPNAME}:${VERSION} 2> /dev/null || true

.PHONY: release clean build push
