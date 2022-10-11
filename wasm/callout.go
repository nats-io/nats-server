package main

//export callout
func callout(echo bool) bool {
	return !echo
}

//export add
func add(x, y uint32) uint32 {
	return x + y
}
func main() {
}
