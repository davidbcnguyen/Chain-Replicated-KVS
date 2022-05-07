package chainedkv

import (
	"errors"
	"log"
	"net"
	"net/rpc"
	"reflect"
	"strings"
	"sync"
	"time"

	fchecker "cs.ubc.ca/cpsc416/a3/fcheck"
	"github.com/DistributedClocks/tracing"
)

// Actions to be recorded by coord (as part of ctrace, ktrace, and strace):

type CoordStart struct {
}

type ServerFail struct {
	ServerId uint8
}

type ServerFailHandledRecvd struct {
	FailedServerId   uint8
	AdjacentServerId uint8
}

type NewChain struct {
	Chain []uint8
}

type AllServersJoined struct {
}

type HeadReqRecvd struct {
	ClientId string
}

type HeadRes struct {
	ClientId string
	ServerId uint8
}

type TailReqRecvd struct {
	ClientId string
}

type TailRes struct {
	ClientId string
	ServerId uint8
}

type ServerJoiningRecvd struct {
	ServerId uint8
}

type ServerJoinedRecvd struct {
	ServerId uint8
}

type CoordConfig struct {
	ClientAPIListenAddr string
	ServerAPIListenAddr string
	LostMsgsThresh      uint8
	NumServers          uint8
	TracingServerAddr   string
	Secret              []byte
	TracingIdentity     string
}

type Coord struct {
	// Coord state may go here
	Shared *SharedState
	Trace  *tracing.Trace
}

type ServerInfo struct {
	ServerId           int
	ServerStatus       string
	CoordToServerAddr  string
	ClientToServerAddr string
	ServerToServerAddr string
	HbAddr             string
	Trace              *tracing.Trace
}

// Not sure if is the best idea to include own address in arg
type ServerJoiningArgs struct {
	ServerId           int
	CoordToServerAddr  string
	ClientToServerAddr string
	ServerToServerAddr string
	HbAddr             string
	Token              tracing.TracingToken
}

type HeadReqArgs struct {
	ClientId   string
	Token      tracing.TracingToken
	ClientAddr string
}

type TailReqArgs struct {
	ClientId   string
	Token      tracing.TracingToken
	ClientAddr string
}

type ServerAckArgs struct {
	ServerId   uint8                // For server method use
	ServerPrev string               //
	ServerNext string               //
	Token      tracing.TracingToken // For server method use
}

type CoordState struct {
	shared *SharedState
}

type SharedState struct {
	Servers      map[int]*ServerInfo
	ServerInfoCh chan *ServerInfo
	NumServers   int
	Tracer       *tracing.Tracer
	HeadClients  map[string]bool
	TailClients  map[string]bool
	HeadMutex    *sync.Mutex
	TailMutex    *sync.Mutex
}

type GetServerInfoReply struct {
	ServerId           int
	ClientToServerAddr string
	Token              tracing.TracingToken
}

type NotifyServerFailureArgs struct {
	FailedServerId uint8
	NewServerAddr  string
	NewServerId    uint8
	Token          tracing.TracingToken
}

const (
	SERVER_REQUESTING = "REQUESTING"
	SERVER_ALIVE      = "ALIVE"
	SERVER_DEAD       = "DEAD"
)

func NewCoord() *Coord {
	return &Coord{
		Shared: nil,
		Trace:  nil,
	}
}

func (c *Coord) Start(clientAPIListenAddr string, serverAPIListenAddr string, lostMsgsThresh uint8, numServers uint8, ctrace *tracing.Tracer) error {
	c.Trace = ctrace.CreateTrace()
	c.Shared = &SharedState{
		NumServers:   int(numServers),
		Servers:      make(map[int]*ServerInfo, numServers),
		ServerInfoCh: make(chan *ServerInfo, numServers),
		HeadClients:  make(map[string]bool),
		TailClients:  make(map[string]bool),
		HeadMutex:    &sync.Mutex{},
		TailMutex:    &sync.Mutex{},
		Tracer:       ctrace,
	}

	c.Trace.RecordAction(CoordStart{})

	listener, err := net.Listen("tcp", serverAPIListenAddr)
	if err != nil {
		return errors.New("cannot resolve serverAPIListenAddr")
	}

	clientListener, err := net.Listen("tcp", clientAPIListenAddr)
	if err != nil {
		return errors.New("cannot resolve clientAPIListenAddr")
	}

	cs := &CoordState{
		shared: c.Shared,
	}
	server := rpc.NewServer()
	server.Register(cs)
	for i := 0; i < int(numServers); i++ {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go server.ServeConn(conn)
	}

	serversFinishedJoining := make(chan int, 1)
	c.handleJoiningServer(serversFinishedJoining)

	fcArg := fchecker.StartStruct{
		AckLocalIPAckLocalPort:     zeroAddressPort(serverAPIListenAddr),
		EpochNonce:                 0,
		HBeatLocalIPHBeatLocalPort: zeroAddressPort(serverAPIListenAddr),
		HBeatServerInfo:            c.convertToFCheckerFormat(),
		LostMsgThresh:              lostMsgsThresh,
	}
	notifyCh, _, err := fchecker.Start(fcArg)
	if err != nil {
		log.Fatal(err)
	}
	go c.handleFailures(notifyCh)

	// TODO might need more fine tuning
	// May actually be okay
	<-serversFinishedJoining
	server.Accept(clientListener)

	return nil
}

// ============================================RPC API CALLS===============================================
func (cs *CoordState) ConnectToCoord(args *ServerJoiningArgs, reply *tracing.TracingToken) error {
	t := cs.shared.Tracer.ReceiveToken(args.Token)
	t.RecordAction(ServerJoiningRecvd{uint8(args.ServerId)})
	cs.shared.ServerInfoCh <- &ServerInfo{
		ServerId:           args.ServerId,
		ServerStatus:       SERVER_REQUESTING,
		CoordToServerAddr:  args.CoordToServerAddr,
		ClientToServerAddr: args.ClientToServerAddr,
		ServerToServerAddr: args.ServerToServerAddr,
		HbAddr:             args.HbAddr,
		Trace:              t,
	}
	*reply = nil
	return nil
}

// May not be necessary
func (cs *CoordState) ConfirmServerJoined(args *ServerJoiningArgs, reply *tracing.TracingToken) error {
	t := cs.shared.Tracer.ReceiveToken(args.Token)
	t.RecordAction(ServerJoinedRecvd{uint8(args.ServerId)})
	*reply = t.GenerateToken()
	return nil
}

func (cs *CoordState) GetHeadServer(args *HeadReqArgs, reply *GetServerInfoReply) error {
	cs.shared.HeadMutex.Lock()
	defer cs.shared.HeadMutex.Unlock()
	t := cs.shared.Tracer.ReceiveToken(args.Token)
	t.RecordAction(HeadReqRecvd{args.ClientId})

	servers := cs.shared.Servers
	N := cs.shared.NumServers

	cs.shared.HeadClients[args.ClientAddr] = true

	for serverId := 1; serverId <= N; serverId++ {
		if servers[serverId].ServerStatus == SERVER_ALIVE {
			si := servers[serverId]
			t.RecordAction(HeadRes{args.ClientId, uint8(serverId)})
			*reply = GetServerInfoReply{
				ServerId:           si.ServerId,
				ClientToServerAddr: si.ClientToServerAddr,
				Token:              t.GenerateToken(),
			}
			return nil
		}
	}
	return nil
}

func (cs *CoordState) GetTailServer(args *TailReqArgs, reply *GetServerInfoReply) error {
	cs.shared.TailMutex.Lock()
	defer cs.shared.TailMutex.Unlock()
	t := cs.shared.Tracer.ReceiveToken(args.Token)
	t.RecordAction(TailReqRecvd{args.ClientId})

	servers := cs.shared.Servers
	N := cs.shared.NumServers

	cs.shared.TailClients[args.ClientAddr] = true

	for serverId := N; serverId >= 1; serverId-- {
		if servers[serverId].ServerStatus == SERVER_ALIVE {
			si := servers[serverId]
			t.RecordAction(TailRes{args.ClientId, uint8(serverId)})
			*reply = GetServerInfoReply{
				ServerId:           si.ServerId,
				ClientToServerAddr: si.ClientToServerAddr,
				Token:              t.GenerateToken(),
			}
			return nil
		}
	}
	return nil
}

// ========================================================================================================

func zeroAddressPort(serverAddr string) string {
	splitIP := strings.Split(serverAddr, ":")
	newIP := splitIP[0] + ":0"
	return newIP
}

func (c *Coord) handleFailures(notifyCh <-chan fchecker.FailureDetected) {
	for fd := range notifyCh {
		c.Trace.RecordAction(ServerFail{uint8(fd.ServerId)})
		go c.notifyServerFailure(fd.ServerId)
		// c.Trace.RecordAction(NewChain{c.getServerChain()})
	}
}

func (c *Coord) convertToFCheckerFormat() []fchecker.ServerInfo {
	servers := c.Shared.Servers

	arr := make([]fchecker.ServerInfo, len(servers))
	for k, v := range servers {
		arr[k-1] = fchecker.ServerInfo{Addr: v.HbAddr, ServerId: v.ServerId}
	}
	return arr
}

func (c *Coord) handleJoiningServer(serverFinishedJoining chan int) {
	for i := 0; i < c.Shared.NumServers; i++ {
		serverInfo := <-c.Shared.ServerInfoCh
		c.Shared.Servers[serverInfo.ServerId] = serverInfo
	}
	c.formalizeServerChain()
	// c.Trace.RecordAction(NewChain{c.getServerChain()})
	c.Trace.RecordAction(AllServersJoined{})
	close(serverFinishedJoining)
}

func (c *Coord) formalizeServerChain() {
	servers := c.Shared.Servers
	N := c.Shared.NumServers

	for serverId := 1; serverId <= N; serverId++ {
		serverInfo := servers[serverId]
		cli, err := rpc.Dial("tcp", serverInfo.CoordToServerAddr)
		if err != nil {
			log.Fatal(err)
		}

		serverPrev := ""
		serverNext := ""

		if serverId != 1 {
			serverPrev = servers[serverId-1].ServerToServerAddr
		}
		if serverId != N {
			serverNext = servers[serverId+1].ServerToServerAddr
		}

		args := &ServerAckArgs{
			ServerPrev: serverPrev,
			ServerNext: serverNext,
			Token:      serverInfo.Trace.GenerateToken(),
		}
		var reply tracing.TracingToken

		err = cli.Call("CoordToServer.ProcessServerJoin", args, &reply)
		if err != nil {
			log.Fatal(err)
		}
		serverInfo.ServerStatus = SERVER_ALIVE
		t := c.Shared.Tracer.ReceiveToken(reply)
		t.RecordAction(ServerJoinedRecvd{uint8(serverId)})
		// Not sure if every time
		c.Trace.RecordAction(NewChain{c.getServerChain()})
		cli.Close()
	}
}

func callServerFailedKvslib(clients map[string]bool, procedureName string) {
	for cliAddr, subscribed := range clients {
		if subscribed {
			cli, err := rpc.Dial("tcp", cliAddr)
			if err != nil {
				continue
			}
			arg := true
			var reply bool
			cli.Call(procedureName, &arg, &reply)
			cli.Close()
		}
	}

}

// TODO handle concurrent server failure
func (c *Coord) notifyServerFailure(serverId int) {
	servers := c.Shared.Servers

	serverFailed := servers[serverId]
	serverFailed.ServerStatus = SERVER_DEAD

	if c.isHeadServer(serverId) {
		c.Shared.HeadMutex.Lock()
		defer c.Shared.HeadMutex.Unlock()

		go callServerFailedKvslib(c.Shared.HeadClients, "NotifyFailure.HeadFailed")
	}

	if c.isTailServer(serverId) {
		c.Shared.TailMutex.Lock()
		defer c.Shared.TailMutex.Unlock()

		go callServerFailedKvslib(c.Shared.TailClients, "NotifyFailure.TailFailed")
	}

	prevServerLinked := false
	nextServerLinked := false

	oldServerChain := c.getServerChain()
	for {
		prevServerId := c.getPrevServer(serverId)
		nextServerId := c.getNextServer(serverId)
		if prevServerId != -1 && !prevServerLinked {
			if err := c.makeRPCServerFailureCall(prevServerId, nextServerId, serverId, "CoordToServer.NextServerFailHandler"); err != nil {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			prevServerLinked = true
		}

		if nextServerId != -1 && !nextServerLinked {
			if err := c.makeRPCServerFailureCall(nextServerId, prevServerId, serverId, "CoordToServer.PrevServerFailHandler"); err != nil {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			nextServerLinked = true
		}
		break
	}

	// QueSTIONABLE
	if reflect.DeepEqual(oldServerChain, c.getServerChain()) {
		c.Trace.RecordAction(NewChain{c.getServerChain()})
	}

}

func (c *Coord) makeRPCServerFailureCall(notifiedServer, serverToUpdate, failedServer int, procedureName string) error {
	servers := c.Shared.Servers
	serverInfo := servers[notifiedServer]
	cli, err := rpc.Dial("tcp", serverInfo.CoordToServerAddr)
	if err != nil {
		return err
	}

	newServerAddr := ""
	newServerId := 0
	if serverToUpdate != -1 {
		newServerAddr = servers[serverToUpdate].ServerToServerAddr
		newServerId = serverToUpdate
	}
	args := NotifyServerFailureArgs{
		FailedServerId: uint8(failedServer),
		NewServerAddr:  newServerAddr,
		NewServerId:    uint8(newServerId),
		Token:          c.Trace.GenerateToken(),
	}
	var reply tracing.TracingToken

	err = cli.Call(procedureName, args, &reply)
	if err != nil {
		return err
	}
	c.Trace = c.Shared.Tracer.ReceiveToken(reply)

	c.Trace.RecordAction(ServerFailHandledRecvd{uint8(failedServer), uint8(notifiedServer)})
	cli.Close()
	return nil
}

func (c *Coord) getServerChain() []uint8 {
	chain := make([]uint8, 0)

	for i := 1; i <= c.Shared.NumServers; i++ {
		s := c.Shared.Servers[i]
		if s.ServerStatus == SERVER_ALIVE {
			chain = append(chain, uint8(s.ServerId))
		}
	}
	return chain
}

func (c *Coord) getPrevServer(serverId int) int {
	for i := serverId - 1; i >= 1; i-- {
		if c.Shared.Servers[i].ServerStatus == SERVER_ALIVE {
			return i
		}
	}
	return -1
}

func (c *Coord) getNextServer(serverId int) int {
	for i := serverId + 1; i <= c.Shared.NumServers; i++ {
		if c.Shared.Servers[i].ServerStatus == SERVER_ALIVE {
			return i
		}
	}
	return -1
}

func (c *Coord) isHeadServer(serverId int) bool {
	return c.getPrevServer(serverId) == -1
}

func (c *Coord) isTailServer(serverId int) bool {
	return c.getNextServer(serverId) == -1
}
