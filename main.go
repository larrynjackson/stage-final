package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"time"

	"lnj.com/unix/sockets/list"
	"lnj.com/unix/sockets/list/priorityqueue"
	"lnj.com/unix/sockets/message-handlers"
)

const (
	protocol          = "unix"
	sockAddrIn string = "/temp/stage-final-socket"
)

type InputMessage struct {
	Id          int64  `json:"id"`
	ShortCode   string `json:"shortcode"`
	Destination string `json:"destination"`
	Tag         string `json:"tag"`
}

type Config struct {
	ConnIn    net.Conn
	Queue     list.DListNode
	ReadCount int
	ShutDown  bool
	AppMutex  sync.Mutex
}

func main() {
	var app Config
	var readCount int = 0

	app.ReadCount = readCount

	queue := priorityqueue.CreatePQueue()
	app.Queue = queue
	os.RemoveAll(sockAddrIn)
	if _, err := os.Stat(sockAddrIn); err == nil {
		if err := os.RemoveAll(sockAddrIn); err != nil {
			log.Fatal(err)
		}
	}

	listener, err := net.Listen(protocol, sockAddrIn)

	if err != nil {
		log.Fatal(err)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)

	go func() {
		<-quit

		//app.AppMutex.Lock()
		app.ShutDown = true
		//app.AppMutex.Unlock()

		fmt.Println("ctrl-c pressed!")
		close(quit)
	}()

	go app.handleQueuedMessages()

	fmt.Println("> Server launched")
	for {
		connIn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		app.ConnIn = connIn
		go app.readInput()
	}
}

func (app *Config) handleQueuedMessages() {

	var state string = "start"
	var ok bool
	var msg *message.Transport

	for {

		switch state {
		case "start":
			if !app.Queue.IsEmpty() {
				//fmt.Println("QC:", app.Queue.GetNodeCount())
				app.AppMutex.Lock()
				ok, msg = app.Queue.Take()
				app.AppMutex.Unlock()
				if ok {
					// do some small work
					time.Sleep(time.Millisecond * 5)
					s := string(msg.Data)
					fmt.Println("s:", s)
				}
			} else {
				if app.ShutDown {
					fmt.Println("sleep 1 for shutdown race")
					time.Sleep(time.Second * 1)
					fmt.Println("write shutdown")
					os.Exit(0)
				}
				fmt.Println("RC=", app.ReadCount)
				time.Sleep(time.Second * 5)
			}
		}
	}
}

func (app *Config) readInput() {
	defer app.ConnIn.Close()

	m := &message.Transport{}
	err := m.Read(app.ConnIn)
	if err != nil {
		log.Println(err)
		return
	}
	app.ReadCount += 1
	app.AppMutex.Lock()
	app.Queue.PriorityPut(m, 0)
	app.AppMutex.Unlock()

	if app.ShutDown {
		fmt.Println("read shutdown signal")
		mb := &message.Transport{}
		msg := "hold-messages"
		mb.Length = len(msg)
		mb.Data = []byte(msg)
		err = mb.Write(app.ConnIn)
		if err != nil {
			log.Println(err)
		}
		app.ConnIn.Close()
	} else {
		mb := &message.Transport{}
		msg := "ok"
		mb.Length = len(msg)
		mb.Data = []byte(msg)
		err = mb.Write(app.ConnIn)
		if err != nil {
			log.Println(err)
			return
		}
	}

}
