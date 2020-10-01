PACKAGES=`go list ./...`

test:
	go test -v -cover ${PACKAGES}

.PHONEY: test
