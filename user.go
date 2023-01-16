package main

import (
	"net"
	"strings"
)

type User struct {
	Name   string
	Addr   string
	C      chan string
	conn   net.Conn
	server *Server
}

func NewUser(conn net.Conn, server *Server) *User {
	userAddr := conn.RemoteAddr().String()
	user := &User{
		Name:   userAddr,
		Addr:   userAddr,
		C:      make(chan string),
		conn:   conn,
		server: server,
	}
	go user.ListenMessage()

	return user
}

func (this *User) Online() {
	this.server.mapLock.Lock()
	this.server.OnlineMap[this.Name] = this
	this.server.mapLock.Unlock()

	this.server.BroadCast(this, "Player online")
}

func (this *User) Offline() {
	this.server.mapLock.Lock()
	delete(this.server.OnlineMap, this.Name)
	this.server.mapLock.Unlock()

	this.server.BroadCast(this, "Player offline")
}

func (this *User) SendMsg(msg string) {
	this.conn.Write([]byte(msg))
}

func (this *User) DoMessage(msg string) {
	if msg == "who" {
		this.server.mapLock.Lock()
		for _, user := range this.server.OnlineMap {
			onlineMsg := "[" + user.Addr + "]" + user.Name + ":" + "is onlining...\n"
			this.SendMsg(onlineMsg)
		}
		this.server.mapLock.Unlock()
	} else if len(msg) > 7 && msg[:7] == "rename|" {
		newName := strings.Split(msg, "|")[1]

		_, ok := this.server.OnlineMap[newName]
		if ok {
			this.SendMsg("The current user name is already in use\n")
		} else {
			this.server.mapLock.Lock()
			delete(this.server.OnlineMap, this.Name)
			this.server.OnlineMap[newName] = this
			this.server.mapLock.Unlock()

			this.Name = newName
			this.SendMsg("You have updated user name: " + this.Name + "\n")
		}

	} else if len(msg) > 4 && msg[:3] == "to|" {
		remoteName := strings.Split(msg, "|")[1]
		if remoteName == "" {
			this.SendMsg("Message format error! Please use \"to|UserName|Message\"\n")
			return
		}

		remoteUser, ok := this.server.OnlineMap[remoteName]
		if !ok {
			this.SendMsg("User doesn't exist\n")
			return
		}

		content := strings.Split(msg, "|")[2]
		if content == "" {
			this.SendMsg("no content, please resend\n")
			return
		}
		remoteUser.SendMsg(this.Name + " talk to you: " + content)

	} else {
		this.server.BroadCast(this, msg)
	}

}

func (this *User) ListenMessage() {
	for {
		msg := <-this.C
		//fmt.Print(msg)
		this.conn.Write([]byte(msg + "\n"))
	}
}
