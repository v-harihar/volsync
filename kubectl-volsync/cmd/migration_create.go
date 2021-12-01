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
	"fmt"
	"time"

	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	kerrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
	"sigs.k8s.io/controller-runtime/pkg/client"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
)

type migrationCreate struct {
	cobra.Command
	// client object associated with a cluster
	clientObject client.Client
	// PVC object associated with pvcName
	PVC *v1.PersistentVolumeClaim
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

	migrationCreateCmd.Flags().String("copymethod", "Direct",
		"Copy method used to copy the data from source to destination PVC viz: Direct, Snapshot")
	migrationCreateCmd.Flags().String("accessmodes", "ReadWriteOnce",
		"accessMode of the PVC to create. viz: ReadWriteOnce, ReadOnlyMany, ReadWriteMany, ReadWriteOncePod")
	migrationCreateCmd.Flags().String("capacity", "", "size of the PVC to create in Gi ex: 10Gi")
	migrationCreateCmd.Flags().String("pvcname", "", "name of the PVC to create or use: [context/]namespace/name")
	cobra.CheckErr(migrationCreateCmd.MarkFlagRequired("pvcname"))
	migrationCreateCmd.Flags().String("storageclass", "", "StorageClass name for the PVC")
	migrationCreateCmd.Flags().String("servicetype", "",
		"Service Type or ingress methods for a service. viz: ClusterIP, LoadBalancer")
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
	// build struct migrationRelationshipDestination from cmd line args
	mrd, err := newMigrationRelationshipDestination(cmd)
	if err != nil {
		return err
	}
	// create namespace if does not exist
	err = createNamespace(cmd, mrd)
	if err != nil {
		return err
	}
	// create destination PVC if does not exist
	if cmd.PVC == nil {
		cmd.PVC, err = createDestinationPVC(cmd, mrd)
		if err != nil {
			return err
		}
	}
	// create migration destination
	err = createDestination(cmd, mrd)
	if err != nil {
		return err
	}
	// save the destination details into config file
	m.data.Destination = mrd
	if err = m.Save(); err != nil {
		return fmt.Errorf("unable to save relationship configuration: %w", err)
	}
	return nil
}

//nolint:funlen
func newMigrationRelationshipDestination(mc *migrationCreate) (*migrationRelationshipDestination, error) {
	cmd := &mc.Command
	mrd := &migrationRelationshipDestination{}

	cm, err := cmd.Flags().GetString("copymethod")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch the copy method, err = %w", err)
	}
	mrd.Destination.CopyMethod = volsyncv1alpha1.CopyMethodType(cm)
	if mrd.Destination.CopyMethod != volsyncv1alpha1.CopyMethodDirect &&
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

	mc.clientObject, err = newClient(mrd.Cluster)
	if err != nil {
		return nil, err
	}

	mc.PVC = getDestinationPVC(mc, mrd)
	if mc.PVC == nil {
		cap, err := cmd.Flags().GetString("capacity")
		if err != nil || cap == "" {
			return nil, fmt.Errorf("please provide the capacity or create a PVC by name: %s", mrd.PVCName)
		}
		capacity, _ := resource.ParseQuantity(cap)
		mrd.Destination.Capacity = &capacity

		accessMode, err := cmd.Flags().GetString("accessmodes")
		if err != nil {
			return nil, fmt.Errorf("failed to fetch access mode, %w", err)
		}

		if v1.PersistentVolumeAccessMode(accessMode) != v1.ReadWriteOnce &&
			v1.PersistentVolumeAccessMode(accessMode) != v1.ReadOnlyMany &&
			v1.PersistentVolumeAccessMode(accessMode) != v1.ReadWriteMany &&
			v1.PersistentVolumeAccessMode(accessMode) != v1.ReadWriteOncePod {
			return nil, fmt.Errorf("unsupported access mode: %v", accessMode)
		}
		accessModes := []v1.PersistentVolumeAccessMode{v1.PersistentVolumeAccessMode(accessMode)}
		mrd.Destination.AccessModes = accessModes

		storageClass, err := cmd.Flags().GetString("storageclass")
		if err != nil || storageClass == "" {
			klog.Infof("storage class not provided, binding to default storage class")
		}
		mrd.Destination.StorageClassName = &storageClass
	} else {
		mrd.Destination.Capacity = mc.PVC.Spec.Resources.Requests.Storage()
		mrd.Destination.AccessModes = mc.PVC.Spec.AccessModes
		mrd.Destination.StorageClassName = mc.PVC.Spec.StorageClassName
		mrd.Cluster = mc.PVC.ClusterName
	}

	serviceType, err := cmd.Flags().GetString("servicetype")
	if err != nil {
		return nil, fmt.Errorf("please provide service type, err = %w", err)
	}

	if v1.ServiceType(serviceType) != v1.ServiceTypeClusterIP &&
		v1.ServiceType(serviceType) != v1.ServiceTypeLoadBalancer {
		return nil, fmt.Errorf("unsupported service type: %v", v1.ServiceType(serviceType))
	}
	mrd.Destination.ServiceType = (*v1.ServiceType)(&serviceType)
	mrd.MDName = mrd.Namespace + "-" + mrd.PVCName + "-migration-dest"

	return mrd, nil
}

func createNamespace(mc *migrationCreate, mrd *migrationRelationshipDestination) error {
	ns := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: mrd.Namespace,
		},
	}
	if err := mc.clientObject.Create(mc.Context(), ns); err != nil {
		if kerrs.IsAlreadyExists(err) {
			klog.Infof("Namespace: \"%s\" already present, proceeding with this namespace",
				mrd.Namespace)
			return nil
		}
		return err
	}
	klog.Infof("Created destination namespace: \"%s\"", mrd.Namespace)

	return nil
}

func createDestinationPVC(mc *migrationCreate,
	mrd *migrationRelationshipDestination) (*v1.PersistentVolumeClaim, error) {
	var destPVC *v1.PersistentVolumeClaim
	if *mrd.Destination.StorageClassName == "" {
		destPVC = &v1.PersistentVolumeClaim{
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
	} else {
		destPVC = &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      mrd.PVCName,
				Namespace: mrd.Namespace,
			},
			Spec: v1.PersistentVolumeClaimSpec{
				AccessModes:      mrd.Destination.AccessModes,
				StorageClassName: mrd.Destination.StorageClassName,
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceStorage: *mrd.Destination.Capacity,
					},
				},
			},
		}
	}
	if err := mc.clientObject.Create(mc.Context(), destPVC); err != nil {
		return nil, err
	}
	mrd.Destination.StorageClassName = destPVC.Spec.StorageClassName
	klog.Infof("Created destination PVC: \"%s\"", mrd.PVCName)

	return destPVC, nil
}

func getDestinationPVC(mc *migrationCreate, mrd *migrationRelationshipDestination) *v1.PersistentVolumeClaim {
	cmd := mc.Command
	// Search for preexisting PVC
	destPVC := &v1.PersistentVolumeClaim{}
	pvcInfo := types.NamespacedName{
		Namespace: mrd.Namespace,
		Name:      mrd.PVCName,
	}
	err := mc.clientObject.Get(cmd.Context(), pvcInfo, destPVC)
	if err == nil {
		return destPVC
	}

	return nil
}

func createDestination(mc *migrationCreate, mrd *migrationRelationshipDestination) error {
	rsyncSpec := &volsyncv1alpha1.ReplicationDestinationRsyncSpec{
		ReplicationDestinationVolumeOptions: volsyncv1alpha1.ReplicationDestinationVolumeOptions{
			CopyMethod:     mrd.Destination.CopyMethod,
			DestinationPVC: &mrd.PVCName,
		},
		ServiceType: mrd.Destination.ServiceType,
	}
	rd := &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mrd.MDName,
			Namespace: mrd.Namespace,
		},
		Spec: volsyncv1alpha1.ReplicationDestinationSpec{
			Rsync: rsyncSpec,
		},
	}
	if err := mc.clientObject.Create(mc.Context(), rd); err != nil {
		return err
	}

	// wait for migrationdestination to become ready
	nsName := types.NamespacedName{
		Namespace: mrd.Namespace,
		Name:      mrd.MDName,
	}
	rd = &volsyncv1alpha1.ReplicationDestination{}
	err := wait.PollImmediate(5*time.Second, 2*time.Minute, func() (bool, error) {
		err := mc.clientObject.Get(mc.Context(), nsName, rd)
		if err != nil {
			return false, err
		}
		if rd.Status == nil {
			return false, nil
		}
		if rd.Status.Rsync.Address == nil {
			klog.Infof("Waiting for MigrationDestination %s RSync address to populate", rd.Name)
			return false, nil
		}

		if rd.Status.Rsync.SSHKeys == nil {
			klog.Infof("Waiting for MigrationDestination %s RSync sshkeys to populate", rd.Name)
			return false, nil
		}

		klog.Infof("Found MigrationDestination RSync Address: %s", *rd.Status.Rsync.Address)
		return true, nil
	})
	if err != nil {
		return err
	}
	mrd.Destination.Address = rd.Status.Rsync.Address
	mrd.Destination.Port = rd.Status.Rsync.Port
	mrd.Destination.SSHKeys = rd.Status.Rsync.SSHKeys
	klog.Infof("ReplicationDestination: \"%s\" created in namespace: \"%s\"", mrd.MDName, mrd.Namespace)

	return nil
}
