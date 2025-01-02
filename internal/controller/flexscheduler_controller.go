/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	schedulingv1alpha1 "github.com/flex-cluster/flex-scheduler/api/v1alpha1"
)

// FlexSchedulerReconciler reconciles a FlexScheduler object
type FlexSchedulerReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=scheduling.flexcluster.io,resources=flexschedulers,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=scheduling.flexcluster.io,resources=flexschedulers/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=scheduling.flexcluster.io,resources=flexschedulers/finalizers,verbs=update
//+kubebuilder:rbac:groups=fdse.meixiezichuan,resources=pods,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=fdse.meixiezichuan,resources=pods/status,verbs=get
//+kubebuilder:rbac:groups=fdse.meixiezichuan,resources=nodes,verbs=get;list;watch
//+kubebuilder:rbac:groups=fdse.meixiezichuan,resources=nodes/status,verbs=get

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the FlexScheduler object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.15.0/pkg/reconcile
func (r *FlexSchedulerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = log.FromContext(ctx)

	// TODO(user): your logic here
	var sched schedulingv1alpha1.FlexScheduler
	err := r.Get(ctx, req.NamespacedName, &sched)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Log.Info("CR FlexScheduler not exist")
			return ctrl.Result{}, nil
		}
		log.Log.Error(err, "error on getting cr flexscheduler")
		return ctrl.Result{}, err
	}

	// get all nodes
	nodes := &v1.NodeList{}
	nopts := []client.ListOption{
		client.MatchingFields{"status.conditions[kubernetes.io/Ready].status": "True"}, // 筛选出 Ready 状态的节点
	}
	listErr := r.List(ctx, nodes, nopts...)
	if listErr != nil {
		return ctrl.Result{}, listErr
	}

	// check all pod
	podList := &v1.PodList{}
	opts := []client.ListOption{
		client.MatchingFields{"status.phase": "Pending"},
	}
	listErr = r.List(ctx, podList, opts...)
	if listErr != nil {
		return ctrl.Result{}, listErr
	}

	// send scheduling request to remote server

	var errList []error
	for i, n := 0, len(podList.Items); i < n; i++ {
		pod := podList.Items[i]
		_, er2 := r.Schedule(ctx, &pod, nodes)
		if er2 != nil {
			errList = append(errList, er2)
		} else {
			// delete pending pod
			_ = r.Delete(ctx, &pod)
		}
	}
	if len(errList) > 0 {
		return ctrl.Result{}, errList[0]
	}
	return ctrl.Result{}, nil

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *FlexSchedulerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&schedulingv1alpha1.FlexScheduler{}).
		Complete(r)
}
