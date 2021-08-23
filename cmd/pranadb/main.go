package main

import "os"

func main() {
	r := &runner{}
	r.run(os.Args[1:], true)
	select {} // prevent main exiting
}
