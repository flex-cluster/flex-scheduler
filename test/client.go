package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/flex-cluster/flex-scheduler/test/common"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Client struct {
	url       string
	conn      net.Conn
	connected bool
}

type Node struct {
	clients []*Client
	node    *v1.Node
	stopCh  chan struct{}
}

func NewClient(url string) *Client {
	c := &Client{
		url: url,
	}
	return c
}

func (c *Client) sendMsg(m common.Message) {
	bmsg, err := json.Marshal(m)
	if err != nil {
		log.Fatalf("Failed to unmarshal message %s, : %v", m.MessageType, err)
	}
	err = wsutil.WriteClientText(c.conn, bmsg)
	if err != nil {
		log.Fatalf("Failed to send message %s, : %v", m.MessageType, err)
	}
}

func (n *Node) Start(urlList []string) {
	for _, u := range urlList {
		n.Join(u)
	}
	n.PeriodicTask()
	fmt.Println("All clients have finished.")
}

func (n *Node) StartClient(c *Client) {
	conn, _, _, err := ws.Dial(context.Background(), c.url)
	if err != nil {
		fmt.Printf("Failed to connect to WebSocket server: %v\n", err)
		return
	}
	c.conn = conn
	c.connected = true
	n.RunClient(c)
}

func (n *Node) PeriodicTask() {
	for {
		select {
		case <-n.stopCh:
			fmt.Println("PeriodicTask Exiting")
			return
		default:
			time.Sleep(common.HeartBeatTimeInterval)
			for _, c := range n.clients {
				if c.connected { //report
					n.doReport(c)
				} else { // retry connect
					go n.StartClient(c)
				}
			}
		}
	}
}

func (n *Node) doReport(c *Client) {
	message := common.Message{
		MessageType: common.NodeMessageType,
		Content:     n.node,
	}
	c.sendMsg(message)
}

func (n *Node) handleSche(c *Client, m common.Message) {
	cpuAllocatable := n.node.Status.Allocatable[v1.ResourceCPU]
	memoryAllocatable := n.node.Status.Allocatable[v1.ResourceMemory]
	fmt.Println("cpuAllocatable: ", cpuAllocatable)
	fmt.Println("memoryAllocatable: ", memoryAllocatable)

	var resRequired common.ScheduleConfirmationReq
	//err := json.Unmarshal(m.Content.([]byte), &resRequired)
	common.GetMsgFromContent(m.Content, &resRequired)

	if cpuAllocatable.Value()*1000 > resRequired.ComRequired[string(v1.ResourceCPU)] && memoryAllocatable.Value() > resRequired.ComRequired[string(v1.ResourceMemory)] {
		message := common.Message{
			MessageType: common.ScheduleConfirmationResType,
			MessageID:   m.MessageID,
			Content:     common.ScheduleConfirmationRes{NodeName: n.node.Name, Confirmed: true},
		}
		fmt.Println("send ScheduleConfirmationRes")
		c.sendMsg(message)
	}
}

func (n *Node) Allocate(s common.ScheduleReq) {
	cpuAllocatable := n.node.Status.Allocatable[v1.ResourceCPU]
	memoryAllocatable := n.node.Status.Allocatable[v1.ResourceMemory]
	nowCPU := cpuAllocatable.Value()*1000 - s.ComRequired[string(v1.ResourceCPU)]
	nowMem := memoryAllocatable.Value()/1024/1024 - s.ComRequired[string(v1.ResourceMemory)]
	n.node.Status.Allocatable[v1.ResourceCPU] = resource.MustParse(fmt.Sprintf("%dm", nowCPU))
	n.node.Status.Allocatable[v1.ResourceMemory] = resource.MustParse(fmt.Sprintf("%dMi", nowMem))

	for d, c := range s.DeviceRequired {
		nowD := 0
		allocatableD := n.node.Status.Allocatable[v1.ResourceName(d)]
		if allocatableD.Value() > c {
			nowD = int(allocatableD.Value() - c)
		}
		n.node.Status.Allocatable[v1.ResourceName(d)] = resource.MustParse(strconv.Itoa(nowD))
	}
}

func (n *Node) RunClient(c *Client) {
	n.doReport(c)
	// 从服务器接收消息
	for {
		data, op, err := wsutil.ReadServerData(c.conn)
		if err != nil {
			c.connected = false
			//fmt.Printf("Failed to read message: %v", err)
			return
		}
		// 打印接收到的消息
		//fmt.Println("Received message:", string(data))
		if op == ws.OpText {
			var m common.Message
			err = json.Unmarshal(data, &m)
			if err != nil {
				fmt.Printf("Failed to unmarshal message: %v", err)
				return
			}
			go n.HandleMSG(c, m)
		}
	}
	c.conn.Close()
	c.connected = false
}

func (n *Node) HandleMSG(c *Client, m common.Message) {
	switch m.MessageType {
	case common.ScheduleConfirmationReqType:
		fmt.Println("Received ScheduleConfirmationReq")
		n.handleSche(c, m)
		break
	case common.ScheduleConfirmationAckType:
		var resAck common.ScheduleConfirmationAck
		common.GetMsgFromContent(m.Content, &resAck)
		//err := json.Unmarshal(m.Content.([]byte), &resAck)
		fmt.Println("Received ScheduleConfirmationAck")
		if resAck.Ack {
			n.Allocate(resAck.ScheduleReq)
			n.doReport(c)
		}
	}
}

func (n *Node) Join(url string) {
	c := NewClient(url)
	n.clients = append(n.clients, c)
	cn := n.node.Annotations["clusterNum"]
	intc, err := strconv.Atoi(cn)
	if err != nil {
		intc = 1
	}
	n.node.Annotations["clusterNum"] = strconv.Itoa(intc + 1)

	go n.StartClient(c)
}

func parseFlag(nodeName, cpu, memory *string, deviceMap map[string]int, urlList *[]string) {
	flag.StringVar(nodeName, "nodeName", "", "Name of the node")
	flag.StringVar(cpu, "cpu", "", "Number of CPU cores (e.g., 16)")
	flag.StringVar(memory, "memory", "", "Amount of memory (e.g., 32Gi)")

	deviceList := flag.String("device", "", "Device map in key=value,key=value format")
	urls := flag.String("urls", "", "Urls in list format")

	// 解析命令行参数
	flag.Parse()

	// 检查参数是否提供
	if *nodeName == "" || *cpu == "" || *memory == "" || *deviceList == "" || *urls == "" {
		fmt.Println("Usage: go run main.go -nodeName=<name> -cpu=<cores> -memory=<size> -device=<key=value,key=value> -urls=<url1,url2>")
		return
	}

	fmt.Println("parseFlag: ", *nodeName, *cpu, *memory, *deviceList, *urls)
	if *deviceList != "" {
		pairs := strings.Split(*deviceList, ",")
		for _, pair := range pairs {
			kv := strings.Split(pair, "=")
			if len(kv) != 2 {
				fmt.Printf("Invalid device format: %s\n", pair)
				return
			}
			key := kv[0]
			var value int
			_, err := fmt.Sscanf(kv[1], "%d", &value)
			if err != nil {
				fmt.Printf("Invalid device value for key '%s': %s\n", key, kv[1])
				return
			}
			deviceMap[key] = value
		}
	}
	if *urls != "" {
		us := strings.Split(*urls, ",")
		for _, u := range us {
			*urlList = append(*urlList, u)
		}
	}
}

func newNode(nodeName, cpu, memory string, deviceMap map[string]int) *v1.Node {
	node := v1.Node{}
	node.Annotations = make(map[string]string)
	node.Annotations["clusterNum"] = strconv.Itoa(1)
	node.Name = nodeName
	node.Spec.PodCIDR = ""
	node.Status.Conditions = []v1.NodeCondition{v1.NodeCondition{
		Type:   "Ready",
		Status: "True"},
	}
	node.Status.Capacity = v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse(cpu), // 16 CPU cores
		v1.ResourceMemory: resource.MustParse(memory),
	}
	node.Status.Allocatable = v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse(cpu),
		v1.ResourceMemory: resource.MustParse(memory),
	}
	for k, v := range deviceMap {
		node.Status.Capacity[v1.ResourceName(k)] = resource.MustParse(strconv.Itoa(v))
		node.Status.Allocatable[v1.ResourceName(k)] = resource.MustParse(strconv.Itoa(v))
	}
	return &node
}

func main() {
	var nodeName, cpu, memory string
	var urlList []string
	deviceMap := make(map[string]int)
	parseFlag(&nodeName, &cpu, &memory, deviceMap, &urlList)
	fmt.Println(nodeName, cpu, memory, deviceMap, urlList)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// init node resource
	node := newNode(nodeName, cpu, memory, deviceMap)
	n := Node{
		clients: []*Client{},
		node:    node,
		stopCh:  make(chan struct{}),
	}

	go n.Start(urlList)
	sig := <-sigs
	fmt.Println("Received signal: ", sig)
	close(n.stopCh)

	for _, c := range n.clients {
		if c.connected {
			c.conn.Close()
		}
	}
}
