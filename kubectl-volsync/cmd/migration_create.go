/*
Copyright © 2021 The VolSync authors

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/
package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	kerrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
)

type migrationCreate struct {
	cobra.Command
}

// migrationCreateCmd represents the create command
var migrationCreateCmd = &cobra.Command{
	Use:   "create",
	Short: i18n.T("Create a new migration destination"),
	Long: templates.LongDesc(i18n.T(`
	This command creates and prepares new migration destination to receive data.

	It creates the named PersistentVolumeClaim if it does not already exist,
	and it sets up an associated ReplicationDestination that will be configured
	to accept incoming transfers via rsync over ssh.
	`)),
	RunE: func(cmd *cobra.Command, args []string) error {
		r := &migrationCreate{
			Command: *cmd,
		}
		return r.Run()
	},
	Args: validateMigrationCreate,
}

func init() {
	migrationCmd.AddCommand(migrationCreateCmd)

	migrationCreateCmd.Flags().String("accessmodes", "",
		"accessMode of the PVC to create. viz: ReadWriteOnce, ReadOnlyMany, ReadWriteMany, ReadWriteOncePod")
	migrationCreateCmd.Flags().String("capacity", "", "size of the PVC to create in Gi ex: 10Gi")
	migrationCreateCmd.Flags().String("pvcname", "", "name of the PVC to create or use: [context/]namespace/name")
	cobra.CheckErr(migrationCreateCmd.MarkFlagRequired("pvcname"))
	migrationCreateCmd.Flags().String("storageclass", "", "StorageClass name for the PVC")
	migrationCreateCmd.Flags().String("servicetype", "",
		"Service Type or ingress methods for a service. viz: ClusterIP, NodePort, LoadBalancer")
	cobra.CheckErr(migrationCreateCmd.MarkFlagRequired("servicetype"))
}

func validateMigrationCreate(cmd *cobra.Command, args []string) error {
	// If specified, the PVC's capacity must parse to a valid resource.Quantity
	capacity, err := cmd.Flags().GetString("capacity")
	if err != nil {
		return err
	}
	if len(capacity) > 0 {
		if _, err := resource.ParseQuantity(capacity); err != nil {
			return fmt.Errorf("capacity must be a valid resource.Quantity: %w", err)
		}
	}
	// The PVC name must be specified, and it needs to be in the right format
	pvcname, err := cmd.Flags().GetString("pvcname")
	if err != nil {
		return err
	}
	if _, err := ParseXClusterName(pvcname); err != nil {
		return err
	}
	return nil
}

func (cmd *migrationCreate) Run() error {
	// build struct migrationRelationship from cmd line args
	m, err := newmigrationRelationship(&cmd.Command)
	if err != nil {
		return err
	}

	if err = m.Save(); err != nil {
		return fmt.Errorf("unable to save relationship configuration: %w", err)
	}

	// build struct migrationRelationshipDestination from cmd line args
	mrd, err := newMigrationRelationshipDestination(&cmd.Command)
	if err != nil {
		return nil
	}

	// create namespace if does not exist
	err = createNamespace(cmd.Context(), mrd)
	if err != nil {
		return nil
	}

	// create destination PVC if does not exist
	if mrd.PVC == nil {
		mrd.PVC, err = createDestinationPVC(cmd.Context(), mrd)
		if err != nil {
			return nil
		}
	}

	// create replication destination
	err = CreateDestination(cmd.Context(), mrd)
	if err != nil {
		return nil
	}

	return nil
}

//nolint:funlen
func newMigrationRelationshipDestination(cmd *cobra.Command) (*migrationRelationshipDestination, error) {
	mrd := &migrationRelationshipDestination{}

	cm, err := cmd.Flags().GetString("copymethod")
	if err != nil {
		klog.Info("Copy method not provided, defaulting to copymethod: Snapshot")
		cm = "Snapshot"
	}

	mrd.Destination.CopyMethod = volsyncv1alpha1.CopyMethodType(cm)
	if mrd.Destination.CopyMethod != volsyncv1alpha1.CopyMethodNone &&
		mrd.Destination.CopyMethod != volsyncv1alpha1.CopyMethodClone &&
		mrd.Destination.CopyMethod != volsyncv1alpha1.CopyMethodSnapshot {
		return nil, fmt.Errorf("unsupported copymethod: %v", mrd.Destination.CopyMethod)
	}

	pvcname, err := cmd.Flags().GetString("pvcname")
	if err != nil {
		return nil, err
	}

	xcr, err := ParseXClusterName(pvcname)
	if err != nil {
		return nil, err
	}
	mrd.PVCName = xcr.Name
	mrd.Namespace = xcr.Namespace

	clientObject, err := newClient(mrd.Cluster)
	if err != nil {
		return nil, err
	}
	mrd.clientObject = clientObject

	mrd.PVC = getDestinationPVC(cmd, mrd)
	if mrd.PVC == nil {
		cap, err := cmd.Flags().GetString("capacity")
		if err != nil || cap == "" {
			klog.Infof("please provide capacity or provide name of exiting pvc, err: %v", err)
			return nil, fmt.Errorf("please provide capacity or provide name of exiting pvc, err: %w", err)
		}
		capacity, _ := resource.ParseQuantity(cap)
		mrd.Destination.Capacity = &capacity

		accessMode, err := cmd.Flags().GetString("accessmodes")
		if err != nil || accessMode == "" {
			klog.Info("access mode not provided, defaulting to accessMode: ReadWriteOnce")
			accessMode = "ReadWriteOnce"
		}

		if v1.PersistentVolumeAccessMode(accessMode) != v1.ReadWriteOnce &&
			v1.PersistentVolumeAccessMode(accessMode) != v1.ReadOnlyMany &&
			v1.PersistentVolumeAccessMode(accessMode) != v1.ReadWriteMany &&
			v1.PersistentVolumeAccessMode(accessMode) != v1.ReadWriteOncePod {
			klog.Infof("unsupported access mode: %v", accessMode)
			return nil, fmt.Errorf("unsupported access mode: %v", accessMode)
		}
		accessModes := []v1.PersistentVolumeAccessMode{v1.PersistentVolumeAccessMode(accessMode)}
		mrd.Destination.AccessModes = accessModes
	}

	serviceType, err := cmd.Flags().GetString("servicetype")
	if err != nil {
		klog.Infof("please provide service type, err = %v", err)
		return nil, fmt.Errorf("please provide service type, err = %w", err)
	}

	if v1.ServiceType(serviceType) != v1.ServiceTypeClusterIP &&
		v1.ServiceType(serviceType) != v1.ServiceTypeNodePort &&
		v1.ServiceType(serviceType) != v1.ServiceTypeLoadBalancer {
		klog.Infof("unsupported service type: %v", v1.ServiceType(serviceType))
		return nil, fmt.Errorf("unsupported service type: %v", v1.ServiceType(serviceType))
	}
	mrd.Destination.ServiceType = (*v1.ServiceType)(&serviceType)

	return mrd, nil
}

func createNamespace(ctx context.Context, mrd *migrationRelationshipDestination) error {
	ns := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: mrd.Namespace,
		},
	}

	if err := mrd.clientObject.Create(ctx, ns); err != nil {
		if kerrs.IsAlreadyExists(err) {
			klog.Infof("Namespace: %s already present, proceeding with this namespace",
				mrd.Namespace)
			return nil
		}
		return err
	}
	klog.Infof("Created destination namespace: %s", mrd.Namespace)
	return nil
}

func createDestinationPVC(ctx context.Context,
	mrd *migrationRelationshipDestination) (*v1.PersistentVolumeClaim, error) {
	destPVC := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mrd.PVCName,
			Namespace: mrd.Namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: mrd.Destination.AccessModes,
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: *mrd.Destination.Capacity,
				},
			},
		},
	}

	if err := mrd.clientObject.Create(ctx, destPVC); err != nil {
		return nil, err
	}

	klog.Infof("Created destination PVC: %s", mrd.PVCName)
	return destPVC, nil
}

func getDestinationPVC(cmd *cobra.Command, mrd *migrationRelationshipDestination) *v1.PersistentVolumeClaim {
	// Search for preexisting PVC
	destPVC := &v1.PersistentVolumeClaim{}
	pvcInfo := types.NamespacedName{
		Namespace: mrd.Namespace,
		Name:      mrd.PVCName,
	}

	err := mrd.clientObject.Get(cmd.Context(), pvcInfo, destPVC)
	if err == nil {
		return destPVC
	}

	return nil
}

func CreateDestination(ctx context.Context, mrd *migrationRelationshipDestination) error {
	rsyncSpec := &volsyncv1alpha1.ReplicationDestinationRsyncSpec{
		ReplicationDestinationVolumeOptions: volsyncv1alpha1.ReplicationDestinationVolumeOptions{
			CopyMethod:     mrd.Destination.CopyMethod,
			DestinationPVC: &mrd.PVCName,
		},
		ServiceType: mrd.Destination.ServiceType,
	}

	rdName := mrd.Namespace + "-" + mrd.PVCName + "-replication-dest"
	rd := &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rdName,
			Namespace: mrd.Namespace,
		},
		Spec: volsyncv1alpha1.ReplicationDestinationSpec{
			Rsync: rsyncSpec,
		},
	}
	if err := mrd.clientObject.Create(ctx, rd); err != nil {
		return err
	}
	klog.V(0).Infof("ReplicationDestination: %s created in namespace %s", rdName, mrd.Namespace)

	return nil
}
