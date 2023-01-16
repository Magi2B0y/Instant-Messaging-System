package main

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

type Server struct {
	IP   string
	Port int

	OnlineMap map[string]*User
	mapLock   sync.RWMutex

	Message chan string
}

func NewServer(ip string, port int) *Server {
	server := &Server{
		IP:        ip,
		Port:      port,
		OnlineMap: make(map[string]*User),
		Message:   make(chan string),
	}
	return server
}

func (this *Server) ListenMessager() {
	for {
		msg := <-this.Message
		//fmt.Println(msg)
		this.mapLock.Lock()
		for _, cli := range this.OnlineMap {
			cli.C <- msg
		}
		this.mapLock.Unlock()
	}
}

func (this *Server) BroadCast(user *User, msg string) {
	sendMsg := "[" + user.Addr + "]" + user.Name + ":" + msg
	this.Message <- sendMsg
}

func (this *Server) Handler(conn net.Conn) {
	user := NewUser(conn, this)

	user.Online()

	isLive := make(chan bool)

	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := conn.Read(buf)
			if n == 0 {
				user.Offline()
				return
			}

			if err != nil && err != io.EOF {
				fmt.Println("Conn Read err:", err)
				return
			}

			msg := string(buf[:n-1])

			user.DoMessage(msg)

			isLive <- true
		}
	}()

	for {
		select {
		case <-isLive:
		case <-time.After(time.Second * 600):
			user.SendMsg("You've been kicked out")
			close(user.C)
			conn.Close()
			return

		}

	}
}

func (this *Server) Start() {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", this.IP, this.Port))
	if err != nil {
		fmt.Println("net.Listen err:", err)
		return
	}
	defer listener.Close()

	go this.ListenMessager()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("listener accept err:", err)
			continue
		}
		go this.Handler(conn)
	}

}
