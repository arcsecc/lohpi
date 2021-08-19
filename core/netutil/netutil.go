package netutil

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	log "github.com/inconshreveable/log15"
)

var (
	errFoundNoPort = errors.New("Couldnt find any available port")
	errNoAddr      = errors.New("Failed to find non-loopback address")
)

func GetOpenPort() int {
	attempts := 0
	for {
		l, err := net.Listen("tcp4", ":0")
		if err == nil {
			addr := l.Addr().String()
			l.Close()
			tmp := strings.Split(addr, ":")
			port, _ := strconv.Atoi(tmp[len(tmp)-1])
			return port
		} else {
			fmt.Println(err)
		}
		attempts++
		if attempts > 100 {
			return 0
		}
	}

	return 0
}

func ListenOnPort(port int) (net.Listener, error) {
	return net.Listen("tcp4", fmt.Sprintf(":%d", port))
}

func GetListener() (net.Listener, error) {
	var l net.Listener
	var err error

	attempts := 0

	h, _ := os.Hostname()

	addr, err := net.LookupHost(h)
	if err != nil {
		return nil, err
	}

	for {
		l, err = net.Listen("tcp4", fmt.Sprintf("%s:", addr[0]))
		if err == nil {
			return l, nil
		} else {
			log.Error(err.Error())
		}
		attempts++

		if attempts > 100 {
			return l, errFoundNoPort
		}
	}

	return l, errFoundNoPort
}

func LocalIP() (string, error) {
	host, _ := os.Hostname()

	addrs, err := net.LookupIP(host)
	if err != nil {
		return "", err
	}

	if len(addrs) == 0 {
		return "", errNoAddr
	}

	return addrs[0].String(), nil
}
