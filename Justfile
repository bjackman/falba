test:
    go test ./...

format:
    go fmt

alias fmt := format

vet:
    go vet

alias lint := vet