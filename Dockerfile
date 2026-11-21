FROM golang:1.27 as test

COPY . /src
WORKDIR /src

RUN go test -v .

FROM golang:1.27 as build

COPY . /src
COPY --from=test /go/pkg/mod /go/pkg/mod
WORKDIR /src
RUN  CGO_ENABLED=0 GOOS=linux go build -a -o kvs

FROM scratch

COPY --from=build /src/kvs .
COPY --from=build /src/*.pem .

EXPOSE 8082

CMD [ "/kvs" ]
