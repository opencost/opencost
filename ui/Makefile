BASE_URL := "/model"

.PHONY: build
build:
	echo "building with base url ${BASE_URL}"
	BASE_URL=${BASE_URL} npx parcel build src/index.html

.PHONY: serve
serve:
	echo "serving with base url ${BASE_URL}"
	BASE_URL=${BASE_URL} npx parcel serve src/index.html

.PHONY: clean
clean:
	rm -rf dist/*