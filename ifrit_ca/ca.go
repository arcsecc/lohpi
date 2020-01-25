package main;
import (
	"fmt"
	"github.com/joonnna/ifrit/cauth"
	//"ifrit/cauth"
	
)

func main() {
	fmt.Printf("Starting CA...\n");

	ca, err := cauth.NewCa()
	if err != nil {
		panic(err);
	}

 	ca.Start();
}
