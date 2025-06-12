package main

import (
	"fmt"

	"github.com/bjackman/falba/internal/falba"
)

func main() {
	iv := falba.IntValue{Value: 1}
	fmt.Printf("hello world %d\n", iv.IntValue())
}
