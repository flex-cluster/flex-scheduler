package common

import (
	"encoding/json"
	v1 "k8s.io/api/core/v1"
	"log"
)

type MTP string

const (
	NodeMessageType             MTP = "node"
	ScheduleReqType             MTP = "scheduleRequest"
	ScheduleConfirmationReqType MTP = "scheduleConfirmationRequest"
	ScheduleConfirmationResType MTP = "scheduleConfirmationResponse"
	ScheduleConfirmationAckType MTP = "scheduleConfirmationAck"
)

type Message struct {
	MessageType MTP
	MessageID   string
	Content     interface{}
}

type NodeMessage struct {
	v1.Node
}

type ScheduleReq struct {
	ComRequired    map[string]int64 // cpu: m core; memory: Mi
	DeviceRequired map[string]int64
}

type ScheduleConfirmationReq struct {
	ScheduleReq
}

type ScheduleConfirmationRes struct {
	NodeName  string
	Confirmed bool
}

type ScheduleConfirmationAck struct {
	Ack bool
}

func GetMsgFromContent(data interface{}, m interface{}) {
	switch data.(type) {
	case map[string]interface{}:
		// 如果是 map[string]interface{} 类型，说明是从 JSON 中解码得到的
		// 需要再次进行反序列化为 v1.Node 类型
		nodeData, err := json.Marshal(data)
		if err != nil {
			log.Fatalf("Error marshaling content: %v", err)
		}

		err = json.Unmarshal(nodeData, m)
		if err != nil {
			log.Fatalf("Error unmarshaling content to v1.Node: %v", err)
		}

	default:
		log.Fatal("Content is not a map[string]interface{} type")
	}
}
