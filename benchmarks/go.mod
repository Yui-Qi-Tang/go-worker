module example.com/go-worker-benchmarks

go 1.27.3

require (
	github.com/alitto/pond/v2 v2.7.3
	github.com/panjf2000/ants/v2 v2.12.1
	go.uber.org/zap v1.28.0
	yuki-tang.github.com v0.0.0
)

require (
	github.com/google/uuid v1.6.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
)

replace yuki-tang.github.com => ..
