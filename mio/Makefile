
all: mio.pb.go

%.pb.go: %.proto
	protoc --go_out=. $^
