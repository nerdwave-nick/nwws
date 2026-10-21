module github.com/nerdwave-nick/nwws/benchmarks/compare

go 1.27.0

replace github.com/nerdwave-nick/nwws => ../..

require (
	github.com/coder/websocket v1.8.15
	github.com/gobwas/ws v1.4.0
	github.com/gorilla/websocket v1.5.3
	github.com/nerdwave-nick/nwws v0.1.0
)

require (
	github.com/gobwas/httphead v0.1.0 // indirect
	github.com/gobwas/pool v0.2.1 // indirect
	golang.org/x/sys v0.6.0 // indirect
)
