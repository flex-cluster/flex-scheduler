package controller

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	v1 "k8s.io/api/core/v1"
)

// Schedule 负责将 Pod 调度到合适的节点上
func (s *FlexSchedulerReconciler) Schedule(ctx context.Context, pod *v1.Pod, nodes *v1.NodeList) (*v1.Pod, error) {
	// 收集资源需求（计算和设备资源）
	cpuQ := pod.Spec.Containers[0].Resources.Requests["cpu"]
	memQ := pod.Spec.Containers[0].Resources.Requests["memory"]
	pcpuQ := &cpuQ
	requiredCPU, _ := pcpuQ.AsInt64()
	requiredMemory, _ := (&memQ).AsInt64()
	requiredDevices := getRequiredDevices(pod)

	// 根据资源需求选择合适的节点
	requiredCom := map[string]int64{
		"cpu":    requiredCPU,
		"memory": requiredMemory,
	}
	selectedNode := SelectNodeBasedOnResources(nodes, requiredCom, requiredDevices)
	if len(selectedNode) == 0 {
		return nil, fmt.Errorf("failed to select a node")
	}

	// 返回调度的结果，表示 Pod 被调度到选中的节点
	// TODO ask each node
	pod.Spec.NodeName = selectedNode[0].Name
	return pod, nil
}

// getRequiredDevices 提取 Pod 的设备资源需求（如 GPU、TPU 等）
func getRequiredDevices(pod *v1.Pod) map[string]int64 {
	devices := make(map[string]int64)
	for _, container := range pod.Spec.Containers {
		for device, count := range container.Resources.Requests {
			// 假设设备资源通过特定的标签区分
			if device == "device/accelerator" {
				devices["accelerator"] = count.Value()
			}
		}
	}
	return devices
}

// selectNodeBasedOnResources 根据资源需求选择合适的节点
func SelectNodeBasedOnResources(nodes *v1.NodeList, requiredCom map[string]int64, requiredDevices map[string]int64) []v1.Node {
	type node struct {
		n        v1.Node
		priority float32
	}

	var nodeList []node
	for _, n := range nodes.Items {
		// 假设每个节点的信息是可以通过一个函数获取的，这里简化为模拟值
		nodeResources := getNodeResources(n)
		nodeLoad := getNodeLoad(n)
		clusterNum := getNodeClusterNum(n)

		fmt.Println("nodeResources: ", nodeResources)
		if matchNodeResources(nodeResources, requiredCom) == 1 {
			match := matchDeviceResources(nodeResources, requiredDevices)
			nodePriority := calculateNodePriorityWithCluster(match, clusterNum)
			nodePriority = calculateNodePriorityWithLoad(nodePriority, nodeLoad)

			nodeList = append(nodeList, node{n, nodePriority})
		}
	}

	sort.Slice(nodeList, func(i, j int) bool {
		return nodeList[i].priority > nodeList[j].priority // 降序排序
	})

	N := 3
	var selectedNode []v1.Node
	for i := 0; i < N && i < len(nodeList); i++ {
		fmt.Printf("Node: %s, Priority: %.2f\n", nodeList[i].n.Name, nodeList[i].priority)
		selectedNode = append(selectedNode, nodeList[i].n)
	}
	return selectedNode
}

// getNodeResources 获取节点的资源信息（如 CPU、内存、设备等）
func getNodeResources(node v1.Node) map[string]int64 {
	// 在实际情况中，这里应该查询节点的资源（例如通过 K8s API 获取），这里返回一个模拟数据
	// 获取 CPU 和内存信息
	//cpuCapacity := node.Status.Capacity[v1.ResourceCPU]
	//memoryCapacity := node.Status.Capacity[v1.ResourceMemory]

	// 打印节点的资源信息
	//fmt.Printf("Node: %s\n", node.Name)
	//fmt.Printf("CPU Capacity: %s\n", cpuCapacity.String())
	//fmt.Printf("Memory Capacity: %s\n", memoryCapacity.String())

	// 可分配资源
	cpuAllocatable := node.Status.Allocatable[v1.ResourceCPU]
	memoryAllocatable := node.Status.Allocatable[v1.ResourceMemory]
	deviceAllocatable := node.Status.Allocatable["device"]

	return map[string]int64{
		"cpu":    cpuAllocatable.Value() * 1000,
		"memory": (memoryAllocatable.Value() / 1024 / 1024), // Mi
		"device": deviceAllocatable.Value(),
	}
}

// getNodeLoad 获取节点的负载信息
func getNodeLoad(node v1.Node) int {
	cpuCapacity := node.Status.Capacity[v1.ResourceCPU]
	//memoryCapacity := node.Status.Capacity[v1.ResourceMemory]

	// 可分配资源
	cpuAllocatable := node.Status.Allocatable[v1.ResourceCPU]
	memoryAllocatable := node.Status.Allocatable[v1.ResourceMemory]

	// 打印节点的资源信息
	fmt.Printf("Node: %s\n", node.Name)
	fmt.Printf("CPU Capacity: %s\n", cpuAllocatable.String())
	fmt.Printf("Memory Capacity: %s\n", memoryAllocatable.String())

	result := float32(cpuAllocatable.Value()) / float32(cpuCapacity.Value()) * 100
	// 返回转化后的整数值，保留两位小数
	// 使用 int(result) 截断小数部分来获取整数
	intResult := int(result)
	return intResult
}

// getNodeLoad 获取节点的负载信息
func getNodeClusterNum(node v1.Node) int {
	clusterNum := node.Annotations["clusterNumber"]
	intRes, err := strconv.Atoi(clusterNum)
	if err != nil {
		return 1
	}
	return intRes
}

// matchDeviceResources 判断节点是否满足设备资源的需求
func matchNodeResources(nodeResources map[string]int64, requiredCom map[string]int64) int {
	for res, count := range requiredCom {
		if nodeResources[res] < count {
			return 0
		}
	}
	return 1
}

// matchDeviceResources 判断节点是否满足设备资源的需求
func matchDeviceResources(nodeResources map[string]int64, requiredDevices map[string]int64) float32 {
	match := float32(1.0)
	for device, count := range requiredDevices {
		if nodeResources[device] < count {
			if nodeResources[device] == 0 {
				match = match * 0.01
			} else {
				match = match * float32(nodeResources[device]) / float32(count)
			}
		}
	}
	return match
}

func calculateNodePriorityWithCluster(match float32, clusterNum int) float32 {
	// node属于多个集群
	match = match * (float32(1) / float32(clusterNum))
	return match
}

func calculateNodePriorityWithLoad(match float32, nodeLoad int) float32 {
	return match * float32(nodeLoad)
}
