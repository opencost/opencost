BASE_URL := "/model"

.PHONY: build
build:
	BASE_URL=${BASE_URL} npx parcel build src/index.html

.PHONY: serve
serve:
	BASE_URL=${BASE_URL} npx parcel serve src/index.html

.PHONY: clean
clean:
	rm -rf dist/*