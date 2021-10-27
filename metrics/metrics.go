package metrics

type Counter interface {
	Inc()
}

type Factory interface {
	CreateCounter(name string, description string) (Counter, error)

	Start() error

	Stop() error
}
