FROM golang:latest

WORKDIR bgpfinder
COPY . .
RUN go mod download
EXPOSE 8080
RUN cd ./cmd/bgpfinder-server/ && go build -o ./bgpfinder-server
RUN chmod 777 ./cmd/bgpfinder-server/bgpfinder-server
ENTRYPOINT [ "/bin/bash", "-l", "-c" ]
CMD ["./cmd/bgpfinder-server/bgpfinder-server"]