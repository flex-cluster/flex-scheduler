package main

import (
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
	"time"

	"log"
	"net"
	"net/http"
	"strings"
)

type scheTask struct {
	time    time.Time
	scheReq common.ScheduleReq
}

type Server struct {
	addr      string
	nodeList  v1.NodeList
	conns     map[string]*net.Conn
	scheQueue map[string]scheTask
}

func NewServer(addr string) *Server {
	return &Server{
		addr:      addr,
		nodeList:  v1.NodeList{},
		conns:     make(map[string]*net.Conn),
		scheQueue: make(map[string]scheTask),
	}
}

func getNodeIP(node *v1.Node) string {
	// 遍历节点的地址列表
	for _, address := range node.Status.Addresses {
		// 查找类型为 InternalIP 的地址
		if address.Type == v1.NodeInternalIP {
			return address.Address
		}
	}
	// 如果没有找到 InternalIP，则返回空字符串
	return ""
}

func getClientIP(conn net.Conn) string {
	clientAddr := conn.RemoteAddr().String()
	strAddr := strings.Split(clientAddr, ":")
	clientIP := strAddr[0]
	return clientIP
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
	// 启动 HTTP 服务器
	log.Printf("HTTP server started on http://%s/schedule\n", s.addr)
	log.Printf("WebSocket server started on ws://%s/ws\n", s.addr)
	err := http.ListenAndServe(s.addr, nil)
	if err != nil {
		log.Fatal("Error starting server: ", err)
	}
}

func (s *Server) updateNodeList(newNode v1.Node) {
	// 遍历 NodeList，查找与 newNode 相同 Name 的节点并更新
	found := false
	//fmt.Println("updateNodeList: ", s.nodeList)
	for i, node := range s.nodeList.Items {
		if node.Name == newNode.Name {
			// 更新 Capacity（假设我们只更新 Capacity）
			s.nodeList.Items[i].Status.Capacity = newNode.Status.Capacity
			s.nodeList.Items[i].Status.Allocatable = newNode.Status.Allocatable
			found = true
			break // 一旦找到就退出循环
		}
	}
	if !found {
		s.nodeList.Items = append(s.nodeList.Items, newNode)
	}
	//fmt.Println("nodeList: ", s.nodeList)
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
	fmt.Println("Received ScheduleReq: ", req, "Node list: ", s.nodeList)
	uid := uuid.New().String()
	stask := scheTask{
		time:    time.Now(),
		scheReq: req,
	}
	s.scheQueue[uid] = stask
	nodes := controller.SelectNodeBasedOnResources(&(s.nodeList), req.ComRequired, req.DeviceRequired)
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
	fmt.Println("HandleWSMessage: ", m.MessageType)
	switch m.MessageType {
	case common.NodeMessageType:
		var node v1.Node
		//err := json.Unmarshal(m.Content, &node)
		//fmt.Println("Received NodeMessage: ")
		//fmt.Printf("Type of m.Content: %T\n", m.Content)
		GetNodeFromInterface(m.Content, &node)
		//fmt.Println("Received NodeMessage: ", node)
		s.updateNodeList(node)
		s.conns[node.Name] = conn
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
		scheReq, ok := s.scheQueue[m.MessageID]
		if res.Confirmed && ok {
			delete(s.scheQueue, m.MessageID)
			nodeName := res.NodeName
			//clientIP := getClientIP(conn)
			latency := time.Now().UnixMicro() - scheReq.time.UnixMicro()
			s.writeFile(scheReq.scheReq, nodeName, latency)
			ackMsg := common.Message{
				MessageType: common.ScheduleConfirmationAckType,
				MessageID:   m.MessageID,
				Content:     common.ScheduleConfirmationAck{ScheduleReq: scheReq.scheReq, Ack: true},
			}
			s.sendMsg(nodeName, ackMsg)
		}
		break
	default:
		fmt.Errorf("Received unkown message")
	}
}

func (s *Server) sendMsg(node string, m common.Message) {
	conn := s.conns[node]
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
	defer conn.Close()
	//clientIP := getClientIP(conn)
	//s.conns[clientIP] = &conn

	// 向客户端发送欢迎消息
	//err = wsutil.WriteServerText(conn, []byte("Welcome to the WebSocket server!"))
	//if err != nil {
	//	log.Println("Error writing message to client:", err)
	//	return
	//}

	// 监听客户端发来的消息
	for {
		// 读取客户端消息
		data, op, err := wsutil.ReadClientData(conn)
		if err != nil {
			if err.Error() != "EOF" {
				log.Println("Error reading message:", err)
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
			s.HandleWSMessage(&conn, m)
		}

	}
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
	s := NewServer(addr)
	s.Start()
}
