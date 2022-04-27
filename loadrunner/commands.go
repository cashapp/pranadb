package loadrunner

// Command represents something that can be run by the load runner
type Command interface {
	Run() error
}

// CommandFactory creates Command instances given some config
// Each command type has its own CommandFactory instance
type CommandFactory interface {

	// Name returns the name of the command
	Name() string

	// CreateCommand creates a Command instance
	CreateCommand(loadRunnerNodeID int, commandConfig string) Command
}
