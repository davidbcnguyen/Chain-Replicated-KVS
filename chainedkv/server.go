package chainedkv

import (
	"net"
	"net/rpc"
	"sync"
	"time"

	fchecker "cs.ubc.ca/cpsc416/a3/fcheck"
	"github.com/DistributedClocks/tracing"
)

// =================================================================
// Trace Actions: Server startup
// =================================================================
type ServerStart struct {
	ServerId uint8
}

// =================================================================
// Trace Actions: Server joining
// =================================================================

type ServerJoining struct {
	ServerId uint8
}

type NextServerJoining struct {
	NextServerId uint8
}

type NewJoinedSuccessor struct {
	NextServerId uint8
}

type ServerJoined struct {
	ServerId uint8
}

// =================================================================
// Trace Actions: Server failure handling
// =================================================================

type ServerFailRecvd struct {
	FailedServerId uint8
}

type NewFailoverSuccessor struct {
	NewNextServerId uint8
}

type NewFailoverPredecessor struct {
	NewPrevServerId uint8
}

type ServerFailHandled struct {
	FailedServerId uint8
}

// =================================================================
// Trace Actions: Put call handling
// =================================================================

type PutRecvd struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
}

type PutOrdered struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
}

type PutFwd struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
}

type PutFwdRecvd struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
}

type PutResult struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
}

// =================================================================
// Trace Actions: Get call handling
// =================================================================

type GetRecvd struct {
	ClientId string
	OpId     uint32
	Key      string
}

type GetOrdered struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
}

type GetResult struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
}

// =================================================================
// Server and Server Config
// =================================================================

type Server struct {
	KVState           map[string]Value             //
	ClientOpIdHistory map[string]uint32            //
	ClientHistory     map[string]map[uint32]uint64 //
	GId               uint64                       //
	WaitForPut        bool                         //
	Config            ServerConfig                 //
	PrevServerIPPort  string                       // if "", assume there is no previous server (this is a head)
	NextServerIPPort  string                       // if "", assume there is no next server (this is a tail)
	Tracer            *tracing.Tracer              //
	Trace             *tracing.Trace               //
	Mutex             *sync.Mutex                  //
}

func NewServer() *Server {
	return &Server{
		KVState:           make(map[string]Value),
		ClientOpIdHistory: make(map[string]uint32),
		ClientHistory:     make(map[string]map[uint32]uint64),
		GId:               0,
		WaitForPut:        false,
		Mutex:             &sync.Mutex{},
	}
}

// yeah, maybe don't worry about this.
type ServerConfig struct {
	ServerId          uint8
	CoordAddr         string
	ServerAddr        string
	ServerServerAddr  string
	ServerListenAddr  string
	ClientListenAddr  string
	TracingServerAddr string
	Secret            []byte
	TracingIdentity   string
}

// =================================================================
// Server RPC states
// =================================================================

// RPC struct whose methods are for Coord to call
type CoordToServer struct {
	ServerState *Server
}

// RPC struct whose methods are for other Servers to call
type ServerToServer struct {
	ServerState *Server
}

// RPC struct whose methods are for Clients to call
type ClientToServer struct {
	ServerState *Server
}

// =================================================================
// RPC arg and reply structs
// =================================================================

// type OpAndId struct {
// 	OpId uint32
// 	Op   string
// }

type GetPutArgs struct {
	ClientId   string
	Key        string
	Value      string
	OpId       uint32
	GId        uint64
	SrcIPPort  string
	Token      tracing.TracingToken
	WaitingFor uint32
}

type GetPutReply struct {
	Key   string
	Value string
	OpId  uint32
	GId   uint64
	Token tracing.TracingToken
}

type Value struct {
	Value string
	GId   uint64
}

// =================================================================
// Start
// =================================================================

// TODO
const PutIncrements = 1000

func (s *Server) Start(serverId uint8, coordAddr string, serverAddr string, serverServerAddr string, serverListenAddr string, clientListenAddr string, strace *tracing.Tracer) error {
	// Make new trace and trace ServerStart

	serverTrace := strace.CreateTrace()
	serverTrace.RecordAction(ServerStart{
		ServerId: serverId,
	})

	// set Server fields that couldn't be set by NewServer()
	s.Config = ServerConfig{
		ServerId:         serverId,
		CoordAddr:        coordAddr,
		ServerAddr:       serverAddr,
		ServerListenAddr: serverListenAddr,
		ClientListenAddr: clientListenAddr,
	}
	s.Tracer = strace
	s.Trace = serverTrace

	// Start fcheck
	_, hbListenAddr, err := fchecker.Start(fchecker.StartStruct{
		AckLocalIPAckLocalPort: serverAddr,
	})
	if err != nil {
		return err
	}

	// =============================================================
	// Start goroutine listeners
	// =============================================================

	// Coord listener goroutine
	coordToServer := CoordToServer{
		ServerState: s,
	}

	coordToServerAddr := ""
	for {
		coordToServerAddr, err = createListenerRoutine(zeroAddressPort(serverAddr), &coordToServer)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}

	// Other server listener goroutine
	serverToServer := ServerToServer{
		ServerState: s,
	}

	for {
		_, err = createListenerRoutine(serverListenAddr, &serverToServer)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}

	// kvslib listener goroutine
	clientToServer := ClientToServer{
		ServerState: s,
	}

	for {
		_, err = createListenerRoutine(clientListenAddr, &clientToServer)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}

	// =============================================================
	// Send message to Coord to join the server chain
	// =============================================================

	var coordRPC *rpc.Client
	for {
		coordRPC, err = connectRPCHelper(serverAddr, coordAddr)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}

	// Trace: ServerJoining
	// (we want to trace before GenerateToken is called)
	serverTrace.RecordAction(ServerJoining{
		ServerId: serverId,
	})

	serverJoiningArgs := ServerJoiningArgs{
		ServerId:           int(serverId),
		CoordToServerAddr:  coordToServerAddr,
		ClientToServerAddr: clientListenAddr,
		ServerToServerAddr: serverListenAddr,
		HbAddr:             hbListenAddr,
		Token:              serverTrace.GenerateToken(),
	}

	var recvedToken tracing.TracingToken // to be honest, this doesn't really do anything.

	// server join RPC call
	err = coordRPC.Call("CoordState.ConnectToCoord", &serverJoiningArgs, &recvedToken)
	coordRPC.Close()
	if err != nil {
		return err
	}

	// After the RPC call, ProcessServerJoin will be called, so the rest of the join stuff happens there.

	// =============================================================
	// Keep polling to see if this server is the head server
	// Whenever it is, just continue sending hb Puts
	// =============================================================

	for {
		time.Sleep(3 * time.Second)

		// If there is no previous server, and there is indeed a next server
		if s.PrevServerIPPort == "" && s.NextServerIPPort != "" {
			s.Mutex.Lock()

			rpc, err := connectRPCHelper(zeroAddressPort(serverServerAddr), s.NextServerIPPort)
			if err != nil {
				s.Mutex.Unlock()
				continue
			}

			s.GId += PutIncrements

			args := s.GId
			var reply bool

			err = rpc.Call("ServerToServer.PutHandlerPutHeartbeat", &args, &reply)
			rpc.Close()
			if err != nil {
				s.Mutex.Unlock()
				continue
			}

			s.Mutex.Unlock()
		}
	}
}

// =================================================================
// RPC methods: Coord to Server
// =================================================================

// "Tail is server [number]"
func (s *CoordToServer) ProcessServerJoin(args *ServerAckArgs, reply *tracing.TracingToken) error {
	// Set previous server to previous tail
	// (if it's blank, that means this server is the head. just ack the coord at that point)
	if args.ServerPrev == "" {
		// update the trace based on calling coord's token
		s.ServerState.Trace = s.ServerState.Tracer.ReceiveToken(args.Token)
	} else {
		s.ServerState.PrevServerIPPort = args.ServerPrev

		// Call previous tail server to update its next server field
		prevServerRPC, err := connectRPCHelper(s.ServerState.Config.ServerServerAddr, args.ServerPrev)
		if err != nil {
			return err
		}

		acceptNewTailArgs := ServerAckArgs{
			ServerId:   s.ServerState.Config.ServerId,
			ServerNext: s.ServerState.Config.ServerListenAddr, // called method will interpret this correctly
			Token:      args.Token,                            // this token was sent by Coord
		}

		var recvedToken tracing.TracingToken

		err = prevServerRPC.Call("ServerToServer.AcceptNewTail", &acceptNewTailArgs, &recvedToken)
		prevServerRPC.Close()
		if err != nil {
			return err
		}

		// update the trace based on this new token
		s.ServerState.Trace = s.ServerState.Tracer.ReceiveToken(recvedToken)
	}

	// Trace: ServerJoined
	s.ServerState.Trace.RecordAction(ServerJoined{
		ServerId: s.ServerState.Config.ServerId,
	})

	// RPC call back to coordinator telling them it was a success
	*reply = s.ServerState.Trace.GenerateToken()

	return nil
}

// If there is S1 - S2 - S3 and S2 fails, S3 would have this called.
func (s *CoordToServer) PrevServerFailHandler(args *NotifyServerFailureArgs, reply *tracing.TracingToken) error {
	s.ServerState.Mutex.Lock()
	defer s.ServerState.Mutex.Unlock()

	// Trace: ServerFailRecvd
	trace := s.ServerState.Tracer.ReceiveToken(args.Token)
	trace.RecordAction(ServerFailRecvd{
		FailedServerId: args.FailedServerId,
	})

	// If this server becomes the new head,
	// increment by the maximum possible GId increase in the previous head
	if args.NewServerAddr == "" {
		s.ServerState.GId += (257 * PutIncrements)
	}

	// Change prev server field
	s.ServerState.PrevServerIPPort = args.NewServerAddr

	// Trace: NewFailoverPredecessor
	if args.NewServerId != 0 {
		trace.RecordAction(NewFailoverPredecessor{
			NewPrevServerId: args.NewServerId,
		})
	}

	// Trace: ServerFailHandled
	trace.RecordAction(ServerFailHandled{
		FailedServerId: args.FailedServerId,
	})

	*reply = trace.GenerateToken()

	return nil
}

// If there is S1 - S2 - S3 and S2 fails, S1 would have this called.
func (s *CoordToServer) NextServerFailHandler(args *NotifyServerFailureArgs, reply *tracing.TracingToken) error {
	s.ServerState.Mutex.Lock()
	defer s.ServerState.Mutex.Unlock()

	// Trace: ServerFailRecvd
	trace := s.ServerState.Tracer.ReceiveToken(args.Token)
	trace.RecordAction(ServerFailRecvd{
		FailedServerId: args.FailedServerId,
	})

	// if this server becomes a tail server,
	// set a flag that makes this server wait for a put/put heartbeat
	// for GId order consistency
	// (don't do this if it's the head though)
	if args.NewServerAddr == "" && s.ServerState.PrevServerIPPort != "" {
		s.ServerState.WaitForPut = true
	}

	// Change next server field
	s.ServerState.NextServerIPPort = args.NewServerAddr

	// Trace: NewFailoverSuccessor
	if args.NewServerId != 0 {
		trace.RecordAction(NewFailoverSuccessor{
			NewNextServerId: args.NewServerId,
		})
	}

	// Trace: ServerFailHandled
	trace.RecordAction(ServerFailHandled{
		FailedServerId: args.FailedServerId,
	})

	*reply = trace.GenerateToken()

	return nil
}

// =================================================================
// RPC methods: Server to Server
// =================================================================

// "I'm now tail" -newly added server
func (s *ServerToServer) AcceptNewTail(args *ServerAckArgs, reply *tracing.TracingToken) error {
	s.ServerState.Mutex.Lock()
	defer s.ServerState.Mutex.Unlock()

	// Trace: NextServerJoining
	serverTrace := s.ServerState.Tracer.ReceiveToken(args.Token)
	serverTrace.RecordAction(NextServerJoining{
		NextServerId: args.ServerId,
	})

	// Update the next server field
	s.ServerState.NextServerIPPort = args.ServerNext

	// Trace: NewJoinedSuccessor
	serverTrace.RecordAction(NewJoinedSuccessor{
		NextServerId: args.ServerId,
	})

	// Reply should be a new token
	*reply = serverTrace.GenerateToken()

	return nil
}

func (s *ServerToServer) PutHandlerNotHead(args *GetPutArgs, reply *bool) error {
	s.ServerState.Mutex.Lock()
	defer s.ServerState.Mutex.Unlock()

	latestProcessedOpId := s.ServerState.ClientOpIdHistory[args.ClientId]
	if s.ServerState.NextServerIPPort == "" && (latestProcessedOpId < args.WaitingFor || s.ServerState.ClientHistory[args.ClientId][latestProcessedOpId] > args.GId) {
		*reply = false
		return nil
	}

	oldGId := uint64(0)
	if cVal, cOk := s.ServerState.ClientHistory[args.ClientId]; cOk {
		if val, ok := cVal[args.OpId]; ok && val >= args.GId {
			*reply = true
			return nil
		} else {
			oldGId = cVal[args.OpId]
			cVal[args.OpId] = args.GId
		}
	} else {
		s.ServerState.ClientHistory[args.ClientId] = make(map[uint32]uint64)
		oldGId = s.ServerState.ClientHistory[args.ClientId][args.OpId]
		s.ServerState.ClientHistory[args.ClientId][args.OpId] = args.GId
	}

	// Trace PutFwdRecvd
	trace := s.ServerState.Tracer.ReceiveToken(args.Token)

	// Update current GId to
	if args.GId > s.ServerState.GId {
		s.ServerState.GId = args.GId
	}

	trace.RecordAction(PutFwdRecvd{
		ClientId: args.ClientId,
		OpId:     args.OpId,
		GId:      args.GId,
		Key:      args.Key,
		Value:    args.Value,
	})

	// Put value
	v, ok := s.ServerState.KVState[args.Key]

	if !ok || v.GId < s.ServerState.GId {
		s.ServerState.KVState[args.Key] = Value{
			Value: args.Value,
			GId:   s.ServerState.GId,
		}
	}

	s.ServerState.WaitForPut = false

	// Send message to next node
	s.ServerState.Mutex.Unlock()
	*reply = callPutNextServerOrClient(s.ServerState, args, trace)
	s.ServerState.Mutex.Lock()

	// if s.ServerState.NextServerIPPort == "" && args.OpId > s.ServerState.ClientOpIdHistory[args.ClientId] {
	// 	s.ServerState.ClientOpIdHistory[args.ClientId] = args.OpId
	// }

	if !*reply {
		if oldGId == 0 {
			delete(s.ServerState.ClientHistory[args.ClientId], args.OpId)
		} else {
			s.ServerState.ClientHistory[args.ClientId][args.OpId] = oldGId
		}
	}
	return nil
}

// For GId incrementing purposes when doing Puts
func (s *ServerToServer) PutHandlerPutHeartbeat(args *uint64, reply *bool) error {
	s.ServerState.Mutex.Lock()

	// do not trace anything
	// don't even Put anything

	// update local GId
	if *args > s.ServerState.GId {
		s.ServerState.GId = *args
	}

	s.ServerState.WaitForPut = false

	s.ServerState.Mutex.Unlock()

	// if tail server don't do anything
	// if not tail server, update local GId and call this method to the next server in the chain
	if s.ServerState.NextServerIPPort != "" {
		var result bool
		// for loop because the next server might crash
		for callRPC(zeroAddressPort(s.ServerState.Config.ServerServerAddr), s.ServerState.NextServerIPPort, "ServerToServer.PutHandlerPutHeartbeat", args, &result) != nil {
			// if this server the method is being called on becomes the new tail, just break out
			if s.ServerState.NextServerIPPort == "" {
				return nil
			}
		}
	}

	return nil
}

// =================================================================
// RPC methods: Client to Server
// =================================================================

// Note: the reply parameter will never be used
func (s *ClientToServer) PutHandler(args *GetPutArgs, reply *GetPutReply) error {
	s.ServerState.Mutex.Lock()
	defer s.ServerState.Mutex.Unlock()

	if cVal, cOk := s.ServerState.ClientHistory[args.ClientId]; cOk {
		if val, ok := cVal[args.OpId]; ok && val >= args.GId {
			return nil
		} else {
			cVal[args.OpId] = args.GId
		}
	} else {
		s.ServerState.ClientHistory[args.ClientId] = make(map[uint32]uint64)
		s.ServerState.ClientHistory[args.ClientId][args.OpId] = args.GId
	}

	if s.ServerState.NextServerIPPort == "" {
		for s.ServerState.ClientOpIdHistory[args.ClientId] < args.WaitingFor {
			s.ServerState.Mutex.Unlock()
			time.Sleep(time.Second)
			s.ServerState.Mutex.Lock()
		}

	}

	// Trace: PutRecvd
	trace := s.ServerState.Tracer.ReceiveToken(args.Token)
	trace.RecordAction(PutRecvd{
		ClientId: args.ClientId,
		OpId:     args.OpId,
		Key:      args.Key,
		Value:    args.Value,
	})

	for {

		// Increment GId
		s.ServerState.GId += PutIncrements

		args.GId = s.ServerState.GId

		// Trace: PutOrdered
		trace.RecordAction(PutOrdered{
			ClientId: args.ClientId,
			OpId:     args.OpId,
			GId:      s.ServerState.GId,
			Key:      args.Key,
			Value:    args.Value,
		})

		// Put value
		v, ok := s.ServerState.KVState[args.Key]

		if !ok || v.GId < s.ServerState.GId {
			s.ServerState.KVState[args.Key] = Value{
				Value: args.Value,
				GId:   s.ServerState.GId,
			}
		}

		s.ServerState.Mutex.Unlock()
		if callPutNextServerOrClient(s.ServerState, args, trace) {
			s.ServerState.Mutex.Lock()
			break
		}
		time.Sleep(time.Second)
		s.ServerState.Mutex.Lock()
	}

	// if s.ServerState.NextServerIPPort == "" && args.OpId > uint32(s.ServerState.ClientOpIdHistory[args.ClientId]) {
	// 	s.ServerState.ClientOpIdHistory[args.ClientId] = args.OpId
	// }

	*reply = GetPutReply{
		Key:   args.Key,
		Value: args.Value,
		OpId:  args.OpId,
		GId:   args.GId,
		Token: nil,
	}

	return nil
}

func (s *ClientToServer) GetHandler(args *GetPutArgs, reply *GetPutReply) error {
	s.ServerState.Mutex.Lock()
	defer s.ServerState.Mutex.Unlock()

	latestProcessedOpId := s.ServerState.ClientOpIdHistory[args.ClientId]
	for s.ServerState.ClientOpIdHistory[args.ClientId] < args.WaitingFor || s.ServerState.ClientHistory[args.ClientId][latestProcessedOpId] > s.ServerState.GId {
		s.ServerState.Mutex.Unlock()
		time.Sleep(time.Second)
		s.ServerState.Mutex.Lock()
	}

	// If the GId is a multiple of 1000 (or whatever PutIncrements is), block it
	// (or if we need to wait for a put heartbeat to maintain consistency in GId order)
	for ((s.ServerState.GId+1)%PutIncrements == 0 || s.ServerState.WaitForPut) && s.ServerState.PrevServerIPPort != "" {
		s.ServerState.Mutex.Unlock()
		time.Sleep(time.Second)
		s.ServerState.Mutex.Lock()
	}

	if cVal, cOk := s.ServerState.ClientHistory[args.ClientId]; cOk {
		cVal[args.OpId] = args.GId
	} else {
		s.ServerState.ClientHistory[args.ClientId] = make(map[uint32]uint64)
		s.ServerState.ClientHistory[args.ClientId][args.OpId] = args.GId
	}

	// if args.OpId > s.ServerState.ClientOpIdHistory[args.ClientId] {
	// 	s.ServerState.ClientOpIdHistory[args.ClientId] = args.OpId
	// }

	// Trace: GetRecvd
	trace := s.ServerState.Tracer.ReceiveToken(args.Token)
	trace.RecordAction(GetRecvd{
		ClientId: args.ClientId,
		OpId:     args.OpId,
		Key:      args.Key,
	})

	// Trace: GetOrdered
	// Also increment the GId for this node
	s.ServerState.GId++

	trace.RecordAction(GetOrdered{
		ClientId: args.ClientId,
		OpId:     args.OpId,
		GId:      s.ServerState.GId,
		Key:      args.Key,
	})

	// Get value
	value, ok := s.ServerState.KVState[args.Key]
	val := value.Value
	if !ok {
		val = ""
	}

	// Trace: GetResult
	trace.RecordAction(GetResult{
		ClientId: args.ClientId,
		OpId:     args.OpId,
		GId:      s.ServerState.GId,
		Key:      args.Key,
		Value:    val,
	})

	// Reply fields
	*reply = GetPutReply{
		Key:   args.Key,
		Value: val,
		OpId:  args.OpId,
		GId:   s.ServerState.GId,
		Token: trace.GenerateToken(),
	}

	return nil
}

func (s *ClientToServer) UpdateLatestOpId(args *GetPutArgs, reply *bool) error {
	s.ServerState.Mutex.Lock()
	if s.ServerState.ClientOpIdHistory[args.ClientId] < args.OpId {
		s.ServerState.ClientOpIdHistory[args.ClientId] = args.OpId
		if cVal, cOk := s.ServerState.ClientHistory[args.ClientId]; cOk {
			cVal[args.OpId] = args.GId
		} else {
			s.ServerState.ClientHistory[args.ClientId] = make(map[uint32]uint64)
			s.ServerState.ClientHistory[args.ClientId][args.OpId] = args.GId
		}
	}
	s.ServerState.Mutex.Unlock()
	return nil
}

// =================================================================
// Helpers
// =================================================================

// used by Start to kickstart RPCs
func createListenerRoutine(address string, s interface{}) (string, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return "", err
	}

	server := rpc.NewServer()
	server.Register(s)
	go server.Accept(listener)

	return listener.Addr().String(), nil
}

func connectRPCHelper(localIPPort string, remoteIPPort string) (*rpc.Client, error) {
	localAddr, err := net.ResolveTCPAddr("tcp", localIPPort)
	if err != nil {
		return nil, err
	}
	remoteAddr, err := net.ResolveTCPAddr("tcp", remoteIPPort)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTCP("tcp", localAddr, remoteAddr)
	if err != nil {
		return nil, err
	}
	RPC := rpc.NewClient(conn)
	return RPC, nil
}

// For use by the Put handler to decide which destination to call
func callPutNextServerOrClient(server *Server, args *GetPutArgs, trace *tracing.Trace) bool {
	var result bool = true

	// If the server is a tail/the only server, reply to the client.
	// Otherwise, relay to the next server.
	if server.NextServerIPPort == "" {

		latestProcessedOpId := server.ClientOpIdHistory[args.ClientId]
		if latestProcessedOpId < args.WaitingFor || server.ClientHistory[args.ClientId][latestProcessedOpId] > args.GId {
			return false
		}

		// Trace: PutResult
		trace.RecordAction(PutResult{
			ClientId: args.ClientId,
			OpId:     args.OpId,
			GId:      args.GId,
			Key:      args.Key,
			Value:    args.Value,
		})

		// Reply fields
		replyArgs := GetPutReply{
			Key:   args.Key,
			Value: args.Value,
			OpId:  args.OpId,
			GId:   args.GId,
			Token: trace.GenerateToken(),
		}

		callRPC(zeroAddressPort(server.Config.ClientListenAddr), args.SrcIPPort, "NotifyPut.PutSuccess", &replyArgs, nil)

	} else {
		// Trace: PutFwd
		trace.RecordAction(PutFwd{
			ClientId: args.ClientId,
			OpId:     args.OpId,
			GId:      args.GId,
			Key:      args.Key,
			Value:    args.Value,
		})

		// Next server message fields
		argsForNextServer := GetPutArgs{
			ClientId:   args.ClientId,
			Key:        args.Key,
			Value:      args.Value,
			OpId:       args.OpId,
			GId:        args.GId,
			SrcIPPort:  args.SrcIPPort,
			Token:      trace.GenerateToken(),
			WaitingFor: args.WaitingFor,
		}

		for callRPC(zeroAddressPort(server.Config.ServerServerAddr), server.NextServerIPPort, "ServerToServer.PutHandlerNotHead", &argsForNextServer, &result) != nil {
			// If the next server updates into this one being the new tail,
			// just recurse into the if case instead and send result to client (and break here)
			if server.NextServerIPPort == "" {
				return false
			}
		}

	}

	return result
}

// General purpose RPC client setup and caller
func callRPC(localAddr string, remoteAddr string, methodName string, args interface{}, reply interface{}) error {
	nextRPC, err := connectRPCHelper(localAddr, remoteAddr)
	if err != nil {
		return err
	}

	err = nextRPC.Call(methodName, args, reply)
	nextRPC.Close()
	if err != nil {
		return err
	}

	return nil
}
