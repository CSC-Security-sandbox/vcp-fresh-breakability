module github.com/vcp-vsa-control-Plane/vsa-control-plane

go 1.24.1

require (
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.51.0
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace v1.27.0
	github.com/go-chi/chi/v5 v5.2.1
	github.com/go-faster/errors v0.7.1
	github.com/go-faster/jx v1.1.0
	github.com/go-openapi/errors v0.22.1
	github.com/go-openapi/runtime v0.28.0
	github.com/go-openapi/strfmt v0.23.0
	github.com/go-openapi/swag v0.23.1
	github.com/go-openapi/validate v0.24.0
	github.com/golang-jwt/jwt/v4 v4.5.2
	github.com/golang-migrate/migrate/v4 v4.18.2
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.7.4
	github.com/lib/pq v1.10.9
	github.com/mattn/go-sqlite3 v1.14.22
	github.com/ogen-go/ogen v1.10.1
	github.com/stretchr/testify v1.10.0
	go.opentelemetry.io/otel v1.35.0
	go.opentelemetry.io/otel/metric v1.35.0
	go.opentelemetry.io/otel/sdk v1.35.0
	go.opentelemetry.io/otel/sdk/metric v1.35.0
	go.opentelemetry.io/otel/trace v1.35.0
	go.temporal.io/api v1.44.1
	go.temporal.io/sdk v1.33.0
	go.temporal.io/sdk/contrib/opentelemetry v0.6.0
	go.uber.org/multierr v1.11.0
	golang.org/x/exp v0.0.0-20250408133849-7e4ce0ab07d0
	golang.org/x/net v0.39.0
	golang.org/x/oauth2 v0.29.0
	golang.org/x/sync v0.13.0
	google.golang.org/api v0.228.0
	gopkg.in/yaml.v2 v2.4.0
	gorm.io/driver/postgres v1.5.11
	gorm.io/driver/sqlite v1.5.7
	gorm.io/gorm v1.25.12
	netapp.com/vsa/lifecycle-manager v0.0.0-00010101000000-000000000000
)

require (
	cloud.google.com/go/auth v0.15.0 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/compute v1.36.0 // indirect
	cloud.google.com/go/compute/metadata v0.6.0 // indirect
	cloud.google.com/go/monitoring v1.24.1 // indirect
	cloud.google.com/go/trace v1.11.5 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.51.0 // indirect
	github.com/asaskevich/govalidator v0.0.0-20230301143203-a9d515a09cc2 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dlclark/regexp2 v1.11.5 // indirect
	github.com/facebookgo/clock v0.0.0-20150410010913-600d898af40a // indirect
	github.com/fatih/color v1.18.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/go-faster/yaml v0.4.6 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-openapi/analysis v0.23.0 // indirect
	github.com/go-openapi/jsonpointer v0.21.1 // indirect
	github.com/go-openapi/jsonreference v0.21.0 // indirect
	github.com/go-openapi/loads v0.22.0 // indirect
	github.com/go-openapi/spec v0.21.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/mock v1.7.0-rc.1 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.6 // indirect
	github.com/googleapis/gax-go/v2 v2.14.1 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.26.3 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/mailru/easyjson v0.9.0 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/nexus-rpc/sdk-go v0.3.0 // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pborman/uuid v1.2.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/robfig/cron v1.2.0 // indirect
	github.com/segmentio/asm v1.2.0 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	go.mongodb.org/mongo-driver v1.17.3 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.60.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.60.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	golang.org/x/crypto v0.37.0 // indirect
	golang.org/x/sys v0.32.0 // indirect
	golang.org/x/text v0.24.0 // indirect
	golang.org/x/time v0.11.0 // indirect
	google.golang.org/genproto v0.0.0-20250407143221-ac9807e6c755 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250407143221-ac9807e6c755 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250407143221-ac9807e6c755 // indirect
	google.golang.org/grpc v1.71.1 // indirect
	google.golang.org/protobuf v1.36.6 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace netapp.com/vsa/lifecycle-manager => github.com/VCP-VSA-control-Plane/temp-vsa-pkg v1.0.2
