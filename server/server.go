package server

type Server struct {
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Start() error {
	println("Hello world")
	return nil
}
