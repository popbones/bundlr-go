PACKAGES=`go list ./...`

test:
	go test -v -race -coverprofile=coverage.txt -covermode=atomic -cover ${PACKAGES}

.PHONEY: test
