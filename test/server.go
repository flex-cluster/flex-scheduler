package main

import (
	"context"
	"fmt"
	"github.com/flex-cluster/flex-scheduler/internal/controller"
	"github.com/flex-cluster/flex-scheduler/test/common"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/google/uuid"
	"io"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/json"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"log"
	"net"
	"net/http"
	"strings"

	_ "net/http/pprof"
)

type scheTask struct {
	time    time.Time
	scheReq common.ScheduleReq
}

type NodeItem struct {
	time time.Time
	node *v1.Node
}

type Server struct {
	server    *http.Server
	addr      string
	conns     sync.Map //map[string]*net.Conn
	scheQueue sync.Map //map[string]scheTask
	nodeMap   sync.Map //map[string]NodeItem
	stopCh    chan struct{}
}

func NewServer(addr string) *Server {
	return &Server{
		addr:   addr,
		stopCh: make(chan struct{}),
	}
}

func setNodeNotReady(n *v1.Node) {
	for i, condition := range n.Status.Conditions {
		if condition.Type == "Ready" {
			n.Status.Conditions[i].Status = "False"
			n.Status.Conditions[i].Reason = "NodeNotReady"
			n.Status.Conditions[i].Message = "Heartbeat timeout."
		}
	}
}

func IsNodeReady(n *v1.Node) bool {
	for i, condition := range n.Status.Conditions {
		if condition.Type == "Ready" {
			if n.Status.Conditions[i].Status == "True" {
				return true
			}
		}
	}
	return false
}

func GetNodeFromInterface(data interface{}, node *v1.Node) {
	switch data.(type) {
	case map[string]interface{}:
		// 如果是 map[string]interface{} 类型，说明是从 JSON 中解码得到的
		// 需要再次进行反序列化为 v1.Node 类型
		nodeData, err := json.Marshal(data)
		if err != nil {
			log.Fatalf("Error marshaling content: %v", err)
		}

		err = json.Unmarshal(nodeData, node)
		if err != nil {
			log.Fatalf("Error unmarshaling content to v1.Node: %v", err)
		}

	default:
		log.Fatal("Content is not a map[string]interface{} type")
	}
}

func (s *Server) Start() {
	http.HandleFunc("/schedule", s.handleSchedule)
	http.HandleFunc("/ws", s.handleWebSocket)

	go s.PeriodicCheck()
	// 启动 HTTP 服务器
	log.Printf("HTTP server started on http://%s/schedule\n", s.addr)
	log.Printf("WebSocket server started on ws://%s/ws\n", s.addr)
	server := &http.Server{
		Addr:    s.addr, // 设置服务器地址
		Handler: nil,    // 默认使用 http.DefaultServeMux（即 http.HandleFunc 注册的路由）
	}
	s.server = server
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Println("Server failed:", err)
	}
}

func (s *Server) updateNodeList(newNode *v1.Node) {
	n := NodeItem{
		time: time.Now(),
		node: newNode,
	}
	s.nodeMap.Store(newNode.Name, n)
	fmt.Println("updateNodeList, nodeList: ")
	s.nodeMap.Range(func(key, value any) bool {
		fmt.Println("k: ", key, "v: ", value.(NodeItem))
		return true
	})
}

func (s *Server) PeriodicCheck() {
	for {
		select {
		case <-s.stopCh:
			return
		default:
			// check scheQueue
			s.scheQueue.Range(func(key, value interface{}) bool {
				task := value.(scheTask)
				delta := time.Now().Unix() - task.time.Unix()
				if delta > int64(common.HeartBeatTimeInterval.Seconds()) { // 1 minute
					s.writeFile(task.scheReq, "failed")
					s.scheQueue.Delete(key)
				}
				return true
			})

			// check node status
			s.nodeMap.Range(func(key, value interface{}) bool {
				ni := value.(NodeItem)
				delta := time.Now().Unix() - ni.time.Unix()
				if delta > int64((common.HeartBeatTimeInterval * 2).Seconds()) {
					setNodeNotReady(ni.node)
					s.nodeMap.Store(key, ni)
				}
				return true
			})
			time.Sleep(60 * time.Second)
		}
	}
}

func (s *Server) handleSchedule(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		fmt.Errorf("Read request body failed.", err)
		return
	}
	var req common.ScheduleReq
	err = json.Unmarshal(body, &req)
	if err != nil {
		fmt.Errorf("Unmarshal request body failed.", err)
		return
	}
	var nodeList v1.NodeList
	s.nodeMap.Range(func(key, value any) bool {
		ni := value.(NodeItem)
		if IsNodeReady(ni.node) {
			nodeList.Items = append(nodeList.Items, *(ni.node))
		}
		return true
	})
	fmt.Println("Received ScheduleReq: ", req, "Node list: ", nodeList)
	uid := uuid.New().String()
	stask := scheTask{
		time:    time.Now(),
		scheReq: req,
	}
	s.scheQueue.Store(uid, stask)
	nodes := controller.SelectNodeBasedOnResources(&nodeList, req.ComRequired, req.DeviceRequired)
	fmt.Println("SelectNodeBasedOnResources: ", nodes)
	m := common.Message{}
	m.MessageType = common.ScheduleConfirmationReqType
	m.MessageID = uid
	m.Content = req.ComRequired

	for _, n := range nodes {
		//nodeIP := getNodeIP(&n)
		nodeName := n.Name
		fmt.Println("handleSchedule get node: ", nodeName)
		s.sendMsg(nodeName, m)
	}
}

func (s *Server) HandleWSMessage(conn *net.Conn, m common.Message) {
	//fmt.Println("HandleWSMessage: ", m.MessageType)
	switch m.MessageType {
	case common.NodeMessageType:
		var node v1.Node
		GetNodeFromInterface(m.Content, &node)
		s.updateNodeList(&node)
		s.conns.Store(node.Name, conn)
		break
	case common.ScheduleConfirmationResType:
		var res common.ScheduleConfirmationRes
		common.GetMsgFromContent(m.Content, &res)
		//res, ok := m.Content.(common.ScheduleConfirmationRes)
		//if !ok {
		//	fmt.Errorf("Error unmarshaling ScheduleConfirmationRes data: %s", m.Content)
		//	return
		//}
		fmt.Println("Received ScheduleConfirmationRes")

		if res.Confirmed {
			v, ok := s.scheQueue.Load(m.MessageID)
			if ok {
				scheTask := v.(scheTask)
				s.scheQueue.Delete(m.MessageID)
				nodeName := res.NodeName
				//clientIP := getClientIP(conn)
				latency := time.Now().UnixMicro() - scheTask.time.UnixMicro()
				fmt.Println("Successful shceduled to ", nodeName)
				s.writeFile(scheTask.scheReq, nodeName, latency)
				ackMsg := common.Message{
					MessageType: common.ScheduleConfirmationAckType,
					MessageID:   m.MessageID,
					Content:     common.ScheduleConfirmationAck{Ack: true},
				}
				s.sendMsg(nodeName, ackMsg)
			} else {
				nodeName := res.NodeName
				ackMsg := common.Message{
					MessageType: common.ScheduleConfirmationAckType,
					MessageID:   m.MessageID,
					Content:     common.ScheduleConfirmationAck{Ack: false},
				}
				s.sendMsg(nodeName, ackMsg)
			}
		}
		break
	default:
		fmt.Errorf("Received unkown message")
	}
}

func (s *Server) sendMsg(node string, m common.Message) {
	v, ok := s.conns.Load(node)
	if !ok {
		fmt.Printf("load conn for node %s failed", node)
		return
	}
	conn := v.(*net.Conn)
	bm, err := json.Marshal(m)
	if err != nil {
		fmt.Errorf("Marshal Message err :%v", err)
		return
	}
	err = wsutil.WriteServerText(*conn, bm)
	if err != nil {
		fmt.Errorf("Write server text err :%v", err)
		return
	}
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// 升级 HTTP 请求为 WebSocket 连接
	conn, _, _, err := ws.UpgradeHTTP(r, w)
	if err != nil {
		log.Println("Failed to upgrade connection:", err)
		return
	}

	// 监听客户端发来的消息
	for {
		// 读取客户端消息
		data, op, err := wsutil.ReadClientData(conn)
		if err != nil {
			if err.Error() == "EOF" || err.Error() == "connection reset by peer" {
				log.Println("Error reading message:", err)
				break
			}
			continue
		}

		if op == ws.OpText {
			// 打印收到的消息
			//fmt.Printf("Received: %s\n", data)
			var m common.Message
			err = json.Unmarshal(data, &m)
			if err != nil {
				log.Println("Error unmarshal message:", err)
				continue
			}
			go s.HandleWSMessage(&conn, m)
		}
	}
	conn.Close()
}

func (s *Server) writeFile(values ...interface{}) {
	var strValues []string
	for _, value := range values {
		strValues = append(strValues, fmt.Sprint(value)) // 使用 fmt.Sprint 将任何类型转换为字符串
	}
	// 将所有字符串连接成一个单一的字符串，并使用空格分隔
	finalString := strings.Join(strValues, " ") + "\n" // 添加换行符

	file, err := os.OpenFile("example.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close() // 记得关闭文件

	// 写入数据到文件
	_, err = file.WriteString(finalString)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return
	}

}

func main() {
	addr := ":8066"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	//go func() {
	//	// pprof 服务器，将暴露在 6060 端口
	//	if err := http.ListenAndServe(":6060", nil); err != nil {
	//		panic(err)
	//	}
	//}()
	s := NewServer(addr)
	go s.Start()

	sig := <-sigs
	fmt.Println("Received signal: ", sig)
	close(s.stopCh)

	fmt.Println("Shutting down server...")
	// 设置超时时间，等待处理中的请求
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.server.Shutdown(ctx); err != nil {
		fmt.Println("Server forced to shutdown:", err)
	}
	fmt.Println("Server exited")
}
