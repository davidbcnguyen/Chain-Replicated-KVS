package kvslib

import (
	"errors"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"

	"cs.ubc.ca/cpsc416/a3/chainedkv"
	"github.com/DistributedClocks/tracing"
)

// Actions to be recorded by kvslib (as part of ktrace, put trace, get trace):

type KvslibStart struct {
	ClientId string
}

type KvslibStop struct {
	ClientId string
}

type Put struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
}

type PutResultRecvd struct {
	OpId uint32
	GId  uint64
	Key  string
}

type Get struct {
	ClientId string
	OpId     uint32
	Key      string
}

type GetResultRecvd struct {
	OpId  uint32
	GId   uint64
	Key   string
	Value string
}

type HeadReq struct {
	ClientId string
}

type HeadResRecvd struct {
	ClientId string
	ServerId uint8
}

type TailReq struct {
	ClientId string
}

type TailResRecvd struct {
	ClientId string
	ServerId uint8
}

type NotifyFailure struct {
	kvslib *KVS
}

type NotifyPut struct {
	kvslib *KVS
}

// NotifyChannel is used for notifying the client about a mining result.
type NotifyChannel chan ResultStruct

type ResultStruct struct {
	OpId   uint32
	GId    uint64
	Result string
}

type ServerHandlerStruct struct {
	args  chainedkv.GetPutArgs
	reply chainedkv.GetPutReply
	done  chan *rpc.Call
}

type QueuedOp struct {
	trace      *tracing.Trace
	clientId   string
	opId       uint32
	op         string
	key        string
	value      string
	waitingFor uint32
}

type KVS struct {
	notifyCh NotifyChannel
	// Add more KVS instance state here.
	chCapacity int
	clientId   string
	mutex      *sync.Mutex

	coordListenIPPort string
	tailListenIPPort  string

	localCoordIPPort      string
	localHeadServerIPPort string
	localTailServerIPPort string

	tracer   *tracing.Tracer
	kvsTrace *tracing.Trace

	stopSignaled chan bool
	opid         uint32
	coord        *rpc.Client
	head         *rpc.Client
	tail         *rpc.Client

	inFlight            []*QueuedOp
	latestProcessedOpId uint32
	latestProcessedGId  uint64
}

func NewKVS() *KVS {
	return &KVS{
		notifyCh: nil,

		chCapacity: 0,
		clientId:   "",
		mutex:      &sync.Mutex{},

		localHeadServerIPPort: "",
		localTailServerIPPort: "",

		tracer:   nil,
		kvsTrace: nil,

		stopSignaled: make(chan bool, 1),
		opid:         0,
		coord:        nil,
		head:         nil,
		tail:         nil,

		inFlight:            nil,
		latestProcessedOpId: 0,
		latestProcessedGId:  0,
	}
}

// Start Starts the instance of KVS to use for connecting to the system with the given coord's IP:port.
// The returned notify-channel channel must have capacity ChCapacity and must be used by kvslib to deliver
// all get/put output notifications. ChCapacity determines the concurrency
// factor at the client: the client will never have more than ChCapacity number of operations outstanding (pending concurrently) at any one time.
// If there is an issue with connecting to the coord, this should return an appropriate err value, otherwise err should be set to nil.
func (d *KVS) Start(localTracer *tracing.Tracer, clientId string, coordIPPort string, localCoordIPPort string, localHeadServerIPPort string, localTailServerIPPort string, chCapacity int) (NotifyChannel, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	kvsTrace := localTracer.CreateTrace()
	kvsTrace.RecordAction(KvslibStart{
		ClientId: clientId,
	})

	// Sets up RPC server to handle pings that indicate failure of the head/tail server
	nf := &NotifyFailure{
		kvslib: d,
	}
	coordListenIPPort, err := setUpRPCListener(d, ":0", nf)
	if err != nil {
		return nil, err
	}

	// Sets up RPC server to handle put success from tail server
	np := &NotifyPut{
		kvslib: d,
	}
	tailListenIPPort, err := setUpRPCListener(d, ":0", np)
	if err != nil {
		return nil, err
	}

	coordRPC := connectToRPC(localCoordIPPort, coordIPPort)

	headReply, err := getHeadServerHelper(localTracer, kvsTrace, clientId, coordRPC, coordListenIPPort)
	if err != nil {
		return nil, err
	}

	tailReply, err := getTailServerHelper(localTracer, kvsTrace, clientId, coordRPC, coordListenIPPort)
	if err != nil {
		return nil, err
	}

	headRPC := connectToRPC(localHeadServerIPPort, headReply.ClientToServerAddr)

	tailRPC := connectToRPC(localTailServerIPPort, tailReply.ClientToServerAddr)

	go hbToTail(d)

	// Sets KVS fields
	d.notifyCh = make(NotifyChannel, chCapacity)

	d.chCapacity = chCapacity
	d.clientId = clientId

	d.coordListenIPPort = coordListenIPPort
	d.tailListenIPPort = tailListenIPPort

	d.localCoordIPPort = localCoordIPPort
	d.localHeadServerIPPort = localHeadServerIPPort
	d.localTailServerIPPort = localTailServerIPPort

	d.tracer = localTracer
	d.kvsTrace = kvsTrace

	d.coord = coordRPC
	d.head = headRPC
	d.tail = tailRPC

	d.inFlight = make([]*QueuedOp, 0, chCapacity)

	return d.notifyCh, nil
}

// Get non-blocking request from the client to make a get call for a given key.
// In case there is an underlying issue (for example, servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The returned value must be delivered asynchronously to the client via the notify-channel channel returned in the Start call.
// The value opId is used to identify this request and associate the returned value with this request.
func (d *KVS) Get(tracer *tracing.Tracer, clientId string, key string) (uint32, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if len(d.inFlight) == d.chCapacity {
		return 0, errors.New("Too many queued operations")
	}

	d.opid++

	waitingFor := uint32(0)
	for _, op := range d.inFlight {
		if op.opId > waitingFor {
			waitingFor = op.opId
		}
	}
	op := &QueuedOp{
		trace:      tracer.CreateTrace(),
		clientId:   clientId,
		opId:       d.opid,
		op:         "get",
		key:        key,
		value:      "",
		waitingFor: waitingFor,
	}

	d.inFlight = append(d.inFlight, op)

	sendOutGet(d, op, true)

	return d.opid, nil
}

// Put non-blocking request from the client to update the value associated with a key.
// In case there is an underlying issue (for example, the servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The value opId is used to identify this request and associate the returned value with this request.
// The returned value must be delivered asynchronously via the notify-channel channel returned in the Start call.
func (d *KVS) Put(tracer *tracing.Tracer, clientId string, key string, value string) (uint32, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if len(d.inFlight) == d.chCapacity {
		return 0, errors.New("Too many queued operations")
	}

	d.opid++

	waitingFor := uint32(0)
	for _, op := range d.inFlight {
		if op.opId > waitingFor {
			waitingFor = op.opId
		}
	}
	op := &QueuedOp{
		trace:      tracer.CreateTrace(),
		clientId:   clientId,
		opId:       d.opid,
		op:         "put",
		key:        key,
		value:      value,
		waitingFor: waitingFor,
	}
	d.inFlight = append(d.inFlight, op)

	sendOutPut(d, op, true)

	return d.opid, nil
}

// Stop Stops the KVS instance from communicating with the KVS and from delivering any results via the notify-channel.
// This call always succeeds.
func (d *KVS) Stop() {
	close(d.stopSignaled)

	d.head.Close()
	d.tail.Close()
	d.coord.Close()
	d.kvsTrace.RecordAction(KvslibStop{
		ClientId: d.clientId,
	})
}

// ============================================RPC API CALLS===============================================

func (nf *NotifyFailure) HeadFailed(args *bool, reply *bool) error {
	nf.kvslib.mutex.Lock()
	defer nf.kvslib.mutex.Unlock()

	// Update head server RPC
	headReply, err := getHeadServerHelper(nf.kvslib.tracer, nf.kvslib.kvsTrace, nf.kvslib.clientId, nf.kvslib.coord, nf.kvslib.coordListenIPPort)
	if err != nil {
		return err
	}
	nf.kvslib.head.Close()
	headRPC := connectToRPC(nf.kvslib.localHeadServerIPPort, headReply.ClientToServerAddr)
	nf.kvslib.head = headRPC

	// Recovery
	for _, op := range nf.kvslib.inFlight {
		if op.op == "put" {
			sendOutPut(nf.kvslib, op, false)
		}
	}

	return nil
}

func (nf *NotifyFailure) TailFailed(args *bool, reply *bool) error {
	nf.kvslib.mutex.Lock()
	defer nf.kvslib.mutex.Unlock()

	// Update tail server RPC
	tailReply, err := getTailServerHelper(nf.kvslib.tracer, nf.kvslib.kvsTrace, nf.kvslib.clientId, nf.kvslib.coord, nf.kvslib.coordListenIPPort)
	if err != nil {
		return err
	}
	nf.kvslib.tail.Close()
	tailRPC := connectToRPC(nf.kvslib.localTailServerIPPort, tailReply.ClientToServerAddr)
	nf.kvslib.tail = tailRPC

	if len(nf.kvslib.inFlight) > 0 {
		args := &chainedkv.GetPutArgs{
			ClientId: nf.kvslib.clientId,
			OpId:     nf.kvslib.latestProcessedOpId,
			GId:      nf.kvslib.latestProcessedGId,
		}
		nf.kvslib.tail.Call("ClientToServer.UpdateLatestOpId", args, nil)
	}

	// Recovery
	for _, op := range nf.kvslib.inFlight {
		if op.op == "get" {
			sendOutGet(nf.kvslib, op, false)
		}
	}

	return nil
}

func (np *NotifyPut) PutSuccess(args *chainedkv.GetPutReply, reply *bool) error {
	np.kvslib.mutex.Lock()
	defer np.kvslib.mutex.Unlock()

	// To handle possible duplicated calls
	isWaitingFor := false
	for _, op := range np.kvslib.inFlight {
		if args.OpId == op.opId {
			isWaitingFor = true
			break
		}
	}
	if !isWaitingFor {
		return nil
	}

	puttrace := np.kvslib.tracer.ReceiveToken(args.Token)
	puttrace.RecordAction(PutResultRecvd{
		OpId: args.OpId,
		GId:  args.GId,
		Key:  args.Key,
	})
	np.kvslib.notifyCh <- ResultStruct{
		OpId:   args.OpId,
		GId:    args.GId,
		Result: args.Value,
	}
	deleteOpId(&np.kvslib.inFlight, args.OpId)

	if np.kvslib.latestProcessedOpId < args.OpId {
		np.kvslib.latestProcessedOpId = args.OpId
		np.kvslib.latestProcessedGId = args.GId
	}

	return nil
}

// ========================================================================================================

func setUpRPCListener(d *KVS, localAddr string, kind interface{}) (string, error) {
	listener, err := net.Listen("tcp", localAddr)
	if err != nil {
		return "", err
	}
	server := rpc.NewServer()
	server.Register(kind)

	go func() {
		cons := make([]net.Conn, 0)
		for {
			select {
			case <-d.stopSignaled:
				for _, conn := range cons {
					conn.Close()
				}
				return
			default:
				conn, err := listener.Accept()
				if err != nil {
					log.Fatal(err)
				}
				cons = append(cons, conn)
				go server.ServeConn(conn)

			}
		}
	}()
	return listener.Addr().String(), nil
}

func connectToRPC(localIPPort string, remoteIPPort string) *rpc.Client {
	var rpcClient *rpc.Client
	var err error
	for {
		rpcClient, err = connectToRPCHelper(localIPPort, remoteIPPort)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	return rpcClient
}

func connectToRPCHelper(localIPPort string, remoteIPPort string) (*rpc.Client, error) {
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
	client := rpc.NewClient(conn)
	return client, nil
}

func getHeadServerHelper(tracer *tracing.Tracer, kvsTrace *tracing.Trace, clientId string, coordRPC *rpc.Client, clientAddr string) (chainedkv.GetServerInfoReply, error) {
	// Gets head server
	kvsTrace.RecordAction(HeadReq{
		ClientId: clientId,
	})
	headArgs := chainedkv.HeadReqArgs{
		ClientId:   clientId,
		Token:      kvsTrace.GenerateToken(),
		ClientAddr: clientAddr,
	}
	var headReply chainedkv.GetServerInfoReply
	err := coordRPC.Call("CoordState.GetHeadServer", &headArgs, &headReply)
	*kvsTrace = *tracer.ReceiveToken(headReply.Token)
	kvsTrace.RecordAction(HeadResRecvd{
		ClientId: clientId,
		ServerId: uint8(headReply.ServerId),
	})
	return headReply, err
}

func getTailServerHelper(tracer *tracing.Tracer, kvsTrace *tracing.Trace, clientId string, coordRPC *rpc.Client, clientAddr string) (chainedkv.GetServerInfoReply, error) {
	kvsTrace.RecordAction(TailReq{
		ClientId: clientId,
	})
	tailArgs := chainedkv.TailReqArgs{
		ClientId:   clientId,
		Token:      kvsTrace.GenerateToken(),
		ClientAddr: clientAddr,
	}
	var tailReply chainedkv.GetServerInfoReply
	err := coordRPC.Call("CoordState.GetTailServer", &tailArgs, &tailReply)
	*kvsTrace = *tracer.ReceiveToken(tailReply.Token)
	kvsTrace.RecordAction(TailResRecvd{
		ClientId: clientId,
		ServerId: uint8(tailReply.ServerId),
	})
	return tailReply, err
}

func sendOutGet(d *KVS, op *QueuedOp, realOp bool) {
	if realOp {
		op.trace.RecordAction(Get{
			ClientId: op.clientId,
			OpId:     op.opId,
			Key:      op.key,
		})
	}

	handlerStruct := ServerHandlerStruct{}
	handlerStruct.args = chainedkv.GetPutArgs{
		ClientId:   op.clientId,
		Key:        op.key,
		Value:      op.key,
		OpId:       op.opId,
		SrcIPPort:  d.tailListenIPPort,
		Token:      op.trace.GenerateToken(),
		WaitingFor: op.waitingFor,
	}
	getCall := d.tail.Go("ClientToServer.GetHandler", &handlerStruct.args, &handlerStruct.reply, nil)
	handlerStruct.done = getCall.Done

	go handleAsyncGet(d, &handlerStruct.args, &handlerStruct.reply, handlerStruct.done)
}

func sendOutPut(d *KVS, op *QueuedOp, realOp bool) {
	if realOp {
		op.trace.RecordAction(Put{
			ClientId: op.clientId,
			OpId:     op.opId,
			Key:      op.key,
			Value:    op.value,
		})
	}

	handlerStruct := ServerHandlerStruct{}
	handlerStruct.args = chainedkv.GetPutArgs{
		ClientId:   op.clientId,
		Key:        op.key,
		Value:      op.value,
		OpId:       op.opId,
		SrcIPPort:  d.tailListenIPPort,
		Token:      op.trace.GenerateToken(),
		WaitingFor: op.waitingFor,
	}
	putCall := d.head.Go("ClientToServer.PutHandler", &handlerStruct.args, &handlerStruct.reply, nil)
	handlerStruct.done = putCall.Done
}

func handleAsyncGet(d *KVS, args *chainedkv.GetPutArgs, reply *chainedkv.GetPutReply, done chan *rpc.Call) {
	for {
		select {
		case <-d.stopSignaled:
			return
		case call := <-done:
			if call.Error != nil {
				return
			}
			d.mutex.Lock()
			trace := d.tracer.ReceiveToken(reply.Token)
			trace.RecordAction(GetResultRecvd{
				OpId:  reply.OpId,
				GId:   reply.GId,
				Key:   reply.Key,
				Value: reply.Value,
			})
			d.notifyCh <- ResultStruct{
				OpId:   reply.OpId,
				GId:    reply.GId,
				Result: reply.Value,
			}
			deleteOpId(&d.inFlight, args.OpId)
			if d.latestProcessedOpId < args.OpId {
				d.latestProcessedOpId = args.OpId
				d.latestProcessedGId = args.GId
			}
			d.mutex.Unlock()
		}
	}
}

func deleteOpId(ops *[]*QueuedOp, opId uint32) {
	indexOfOpId := 0
	for i, op := range *ops {
		if opId == op.opId {
			indexOfOpId = i
		}
	}
	*ops = append((*ops)[:indexOfOpId], (*ops)[indexOfOpId+1:]...)
}

func hbToTail(d *KVS) {
	for {
		args := &chainedkv.GetPutArgs{
			ClientId: d.clientId,
			OpId:     d.latestProcessedOpId,
			GId:      d.latestProcessedGId,
		}
		d.tail.Call("ClientToServer.UpdateLatestOpId", args, nil)
		time.Sleep(time.Second)
	}
}
