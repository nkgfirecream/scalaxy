all: gen

dep:
	go get -v -a github.com/golang/protobuf/protoc-gen-go

gen:
	protoc --go_out=plugins=grpc:. proto.proto