module github.com/arcsecc/lohpi

go 1.15

require (
	github.com/Azure/azure-pipeline-go v0.2.3
	github.com/Azure/azure-storage-blob-go v0.13.0
	github.com/fsnotify/fsnotify v1.5.0 // indirect
	github.com/go-git/go-git/v5 v5.2.0
	github.com/go-redis/redis v6.15.9+incompatible
	github.com/go-redis/redis/v8 v8.11.3 // indirect
	github.com/go-stack/stack v1.8.1 // indirect
	github.com/golang/protobuf v1.5.2
	github.com/gorilla/mux v1.8.0
	github.com/inconshreveable/log15 v0.0.0-20201112154412-8562bdadbbac
	github.com/jackc/pgx/v4 v4.13.0
	github.com/jinzhu/configor v1.2.1
	github.com/joonnna/ifrit v0.0.0-20210824133306-d5b1be2e1223
	github.com/joonnna/workerpool v0.0.0-20180531065140-2c82629f6727 // indirect
	github.com/lestrrat-go/jwx v1.1.4
	github.com/lib/pq v1.10.2
	github.com/mattn/go-colorable v0.1.8 // indirect
	github.com/mattn/go-isatty v0.0.13 // indirect
	github.com/pkg/errors v0.9.1
	github.com/rs/cors v1.8.0
	github.com/sirupsen/logrus v1.7.0
	github.com/spf13/cast v1.4.1 // indirect
	github.com/spf13/viper v1.8.1
	github.com/stretchr/testify v1.7.0
	github.com/tomcat-bit/fifoqueue v0.0.0-20210526093133-19bed9acef0b
	golang.org/x/net v0.0.0-20210813160813-60bc85c4be6d // indirect
	golang.org/x/sys v0.0.0-20210823070655-63515b42dcdf // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/genproto v0.0.0-20210821163610-241b8fcbd6c8 // indirect
	google.golang.org/grpc v1.40.0
	google.golang.org/protobuf v1.27.1
)

replace github.com/joonnna/ifrit v0.0.0-20210714101851-94fdee5dc570 => /home/thomas/go/src/github.com/tomcat-bit/ifrit
