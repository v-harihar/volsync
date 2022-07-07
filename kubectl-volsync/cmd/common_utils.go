package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/backube/volsync/controllers/utils"
	cron "github.com/robfig/cron/v3"
	"github.com/spf13/viper"
	corev1 "k8s.io/api/core/v1"
	kerrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type persistentVolumeClaim struct {
	pvc *corev1.PersistentVolumeClaim
}

func waitForSync(ctx context.Context, srcClient client.Client,
	rsName types.NamespacedName, rs volsyncv1alpha1.ReplicationSource) error {
	err := wait.PollImmediate(5*time.Second, defaultVolumeSyncTimeout, func() (bool, error) {
		if err := srcClient.Get(ctx, rsName, &rs); err != nil {
			return false, err
		}
		if rs.Spec.Trigger == nil || rs.Spec.Trigger.Manual == "" {
			return false, fmt.Errorf("internal error: manual trigger not specified")
		}
		if rs.Status == nil {
			return false, nil
		}
		if rs.Status.LastManualSync != rs.Spec.Trigger.Manual {
			return false, nil
		}
		return true, nil
	})
	return err
}

func createSecret(ctx context.Context, secret *corev1.Secret, cl client.Client) error {
	if err := cl.Create(ctx, secret); err != nil {
		if kerrs.IsAlreadyExists(err) {
			klog.Infof("Secret: \"%s\" is found, proceeding with the same", secret.Name)
			return nil
		}
		return fmt.Errorf("failed to create secret, %w", err)
	}
	klog.Infof("created secret: \"%s\", in the namespace \"%s\"", secret.Name, secret.Namespace)

	return nil
}

func deleteSecret(ctx context.Context, ns types.NamespacedName, cl client.Client) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ns.Name,
			Namespace: ns.Namespace,
		},
	}

	err := cl.Delete(ctx, secret)
	if err != nil {
		if client.IgnoreNotFound(err) == nil {
			klog.Infof("secret %s not found, ignore", ns.Name)
			return nil
		}
		return fmt.Errorf("failed to delete secret %s, %w", ns.Name, err)
	}

	return nil
}

func parseCronSpec(cs string) (*string, error) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	if _, err := parser.Parse(cs); err != nil {
		return nil, err
	}

	return &cs, nil
}

func parseResticConfig(filename string) (*resticConfig, error) {
	if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
		klog.Infof("config filename %s not found", filename)
		return nil, err
	}
	v := viper.New()
	v.SetConfigFile(filename)

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("unable to read in config file, %w", err)
	}

	return &resticConfig{
		Viper:    *v,
		filename: filename,
	}, nil
}

func (pvc *persistentVolumeClaim) checkPVCMountStatus(ctx context.Context,
	client client.Client) error {
	podsUsing, err := utils.PodsUsingPVC(ctx, client, pvc.pvc)
	if err != nil {
		return fmt.Errorf("failed to fetch the pvc affinity, %w", err)
	}

	if len(podsUsing) > 0 {
		podNames := []string{}
		for _, pod := range podsUsing {
			podNames = append(podNames, pod.Name)
		}
		return fmt.Errorf(`WARNING: The pvc "%s" is currently in use by following pods,"%v",
		this may result in pvc/data corruption, you may choose to temporarily stop the pods
		and continue restore operation. Aborting Restore`, pvc.pvc.Name, podNames)
	}
	return nil
}
