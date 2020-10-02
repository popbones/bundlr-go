PACKAGES=`go list ./... | grep -v examples`

test:
	go test -v -race -coverprofile=coverage.txt -covermode=atomic -cover ${PACKAGES}

.PHONEY: test
