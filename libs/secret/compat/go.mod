module github.com/tracehubmmp/golang-basics/libs/secret/compat

go 1.27.0

replace github.com/tracehubmmp/golang-basics/libs/secret => ../

require (
	github.com/caarlos0/env/v11 v11.4.1
	github.com/go-viper/mapstructure/v2 v2.5.0
	github.com/stretchr/testify v1.12.1
	github.com/tracehubmmp/golang-basics/libs/secret v0.0.0-00010101000000-000000000000
	go.yaml.in/yaml/v3 v3.0.5
)
