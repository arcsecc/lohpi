package main;
import (
	"fmt"
	"github.com/joonnna/ifrit/cauth"
)

func main() {
	ca, err := cauth.NewCa()
	if err != nil {
		panic(err);
	}

	ca.Start();
}