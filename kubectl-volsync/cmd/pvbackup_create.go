package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	cron "github.com/robfig/cron/v3"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type pvBackupCreate struct {
	// Cluster context name
	Cluster string
	// Namespace on Source cluster
	Namespace string
	// PVC to be backed up
	SourcePVC string
	// Name of the back up
	Name string
	// Name of the ReplicationSource object
	RSName string
	// Repository is the secret name containing repository info
	Repository string
	// Back up schedule
	schedule string
	// restic configuration details
	resticConfig
	// client object to communicate with a cluster
	client client.Client
	// backup relationship object to be persisted to a config file
	pr *pvBackupRelationship
}

// pvBackupCreateCmd represents the create command
var pvBackupCreateCmd = &cobra.Command{
	Use:   "create",
	Short: i18n.T("Create a new pv-backup relationship"),
	Long: templates.LongDesc(i18n.T(`This command creates the necessary configuration
	inside the Cluster/Namespace, builds replication source CR and saves the details into
	to relationship file.`)),
	RunE: func(cmd *cobra.Command, args []string) error {
		pc, err := newPVBackupCreate(cmd)
		if err != nil {
			return err
		}
		return pc.Run(cmd.Context())
	},
}

func init() {
	initPVBackupCreateCmd(pvBackupCreateCmd)
}

func initPVBackupCreateCmd(pvBackupCreateCmd *cobra.Command) {
	pvBackupCmd.AddCommand(pvBackupCreateCmd)

	pvBackupCreateCmd.Flags().String("name", "", `name of the backup that can be used to 
	address backup & restore`)
	cobra.CheckErr(pvBackupCreateCmd.MarkFlagRequired("name"))
	pvBackupCreateCmd.Flags().String("restic-config", "", `path for the restic config file`)
	cobra.CheckErr(pvBackupCreateCmd.MarkFlagRequired("restic-config"))
	pvBackupCreateCmd.Flags().String("pvcname", "", "name of the PVC to backup: [context/]namespace/name")
	cobra.CheckErr(pvBackupCreateCmd.MarkFlagRequired("pvcname"))
	pvBackupCreateCmd.Flags().String("cronspec", "", "Cronspec describing the backup schedule")
	//cobra.CheckErr(replicationScheduleCmd.MarkFlagRequired("cronspec"))
}

func newPVBackupCreate(cmd *cobra.Command) (*pvBackupCreate, error) {
	pc := &pvBackupCreate{}
	// build struct pvBackupRelationship from cmd line args
	mr, err := newPVBackupRelationship(cmd)
	if err != nil {
		return nil, err
	}
	pc.pr = mr

	if err = pc.parseCLI(cmd); err != nil {
		return nil, err
	}

	return pc, nil
}

func (pc *pvBackupCreate) parseCLI(cmd *cobra.Command) error {
	pvcname, err := cmd.Flags().GetString("pvcname")
	if err != nil || pvcname == "" {
		return fmt.Errorf("failed to fetch the pvcname, err = %w", err)
	}
	xcr, err := ParseXClusterName(pvcname)
	if err != nil {
		return fmt.Errorf("failed to parse cluster name from pvcname, err = %w", err)
	}
	pc.SourcePVC = xcr.Name
	pc.Namespace = xcr.Namespace
	pc.Cluster = xcr.Cluster

	backupName, err := cmd.Flags().GetString("name")
	if err != nil {
		return fmt.Errorf("failed to fetch the backup name, err = %w", err)
	}
	pc.Name = backupName
	pc.RSName = backupName + "-backup-source"

	resticConfigFile, err := cmd.Flags().GetString("restic-config")
	if err != nil {
		return fmt.Errorf("failed to fetch the restic-config, err = %w", err)
	}
	resticConfig, err := parseResticConfig(resticConfigFile)
	if err != nil {
		return err
	}
	pc.resticConfig = *resticConfig

	repository, ok := pc.resticConfig.Viper.Get("RESTIC_REPOSITORY").(string)
	if !ok {
		return err
	}
	pc.Repository = repository

	cronSpec, err := cmd.Flags().GetString("cronspec")
	if err != nil {
		return fmt.Errorf("failed to fetch the cronspec, err = %w", err)
	}

	cs, err := parseCronSpec(cronSpec)
	if err != nil {
		return fmt.Errorf("failed to parse the cronspec, err = %w", err)
	}
	pc.schedule = *cs

	return nil
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

	if v.Get("AWS_ACCESS_KEY_ID") == nil || v.Get("AWS_SECRET_ACCESS_KEY") == nil ||
		v.Get("RESTIC_REPOSITORY") == nil || v.Get("RESTIC_PASSWORD") == nil {
		klog.Infof("necessary configurations missing in %s config file", filename)
		return nil, os.ErrInvalid
	}

	return &resticConfig{
		Viper: *v,
		name:  filename,
	}, nil
}

func parseCronSpec(cs string) (*string, error) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	if _, err := parser.Parse(cs); err != nil {
		return nil, err
	}

	return &cs, nil
}

func (pc *pvBackupCreate) Run(ctx context.Context) error {
	k8sClient, err := newClient(pc.Cluster)
	if err != nil {
		return err
	}
	pc.client = k8sClient

	// Build struct pvBackupRelationshipSource from struct pvBackupCreate
	pc.pr.data.Source = pc.newPVBackupRelationship()
	if err != nil {
		return err
	}

	// Add restic configurations into cluster
	err = pc.ensureSecret(ctx)
	if err != nil {
		return fmt.Errorf("failed to create secrete, %w", err)
	}

	// Creates the RD if it doesn't exist
	_, err = pc.ensureReplicationSource(ctx)
	if err != nil {
		return err
	}

	// Wait for ReplicationSource
	_, err = pc.pr.data.waitForRSStatus(ctx, pc.client)
	if err != nil {
		return err
	}

	// Save the replication source details into relationship file
	if err = pc.pr.Save(); err != nil {
		return fmt.Errorf("unable to save relationship configuration: %w", err)
	}

	// Remove the configuration file saved
	os.Remove(pc.resticConfig.name)
	return nil
}

func (pc *pvBackupCreate) newPVBackupRelationship() *pvBackupRelationshipSource {
	prs := &pvBackupRelationshipSource{}

	// Assign the values from pvBackupCreate built after parsing cmd args
	prs.Namespace = pc.Namespace
	prs.Cluster = pc.Cluster
	prs.PVCName = pc.SourcePVC
	prs.RSName = pc.RSName
	prs.Source.Repository = pc.Repository
	prs.Trigger.Schedule = &pc.schedule

	return prs
}

func (pc *pvBackupCreate) ensureSecret(ctx context.Context) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pc.Name,
			Namespace: pc.Namespace,
		},
		StringData: map[string]string{
			"AWS_ACCESS_KEY_ID":     pc.resticConfig.Viper.Get("AWS_ACCESS_KEY_ID").(string),
			"AWS_SECRET_ACCESS_KEY": pc.resticConfig.Viper.Get("AWS_SECRET_ACCESS_KEY").(string),
			"RESTIC_REPOSITORY":     pc.resticConfig.Viper.Get("RESTIC_REPOSITORY").(string),
			"RESTIC_PASSWORD":       pc.resticConfig.Viper.Get("RESTIC_PASSWORD").(string),
		},
	}
	if err := pc.client.Create(ctx, secret); err != nil {
		return err
	}

	return nil
}

func (pc *pvBackupCreate) ensureReplicationSource(ctx context.Context) (
	*volsyncv1alpha1.ReplicationSource, error) {
	prs := pc.pr.data.Source

	rs := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      prs.RSName,
			Namespace: prs.Namespace,
		},
		Spec: volsyncv1alpha1.ReplicationSourceSpec{
			SourcePVC: prs.PVCName,
			Trigger: &volsyncv1alpha1.ReplicationSourceTriggerSpec{
				Schedule: prs.Trigger.Schedule,
			},
			Restic: &volsyncv1alpha1.ReplicationSourceResticSpec{
				Repository: prs.Source.Repository,
				ReplicationSourceVolumeOptions: volsyncv1alpha1.ReplicationSourceVolumeOptions{
					CopyMethod: volsyncv1alpha1.CopyMethodClone,
				},
			},
		},
	}

	if err := pc.client.Create(ctx, rs); err != nil {
		return nil, err
	}
	klog.Infof("Created ReplicationSource: \"%s\" in Namespace: \"%s\" and Cluster: \"%s\"",
		rs.Name, rs.Namespace, rs.ClusterName)

	return rs, nil
}

func (prd *pvBackupRelationshipData) waitForRSStatus(ctx context.Context, client client.Client) (
	*volsyncv1alpha1.ReplicationSource, error) {
	var (
		rs  *volsyncv1alpha1.ReplicationSource
		err error
	)
	klog.Infof("waiting for replication source to be available")
	err = wait.PollImmediate(5*time.Second, defaultRsyncKeyTimeout, func() (bool, error) {
		rs, err = prd.getReplicationSource(ctx, client)
		if err != nil {
			return false, err
		}
		// TODO: What should be the condition to break the wait ?
		if rs.Status == nil {
			return false, nil
		}

		klog.V(2).Infof("pvbackup replication Source is up: ")
		return true, nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch rs status: %w,", err)
	}

	return rs, nil
}

func (prd *pvBackupRelationshipData) getReplicationSource(ctx context.Context, client client.Client) (
	*volsyncv1alpha1.ReplicationSource, error) {
	nsName := types.NamespacedName{
		Namespace: prd.Source.Namespace,
		Name:      prd.Source.RSName,
	}
	rs := &volsyncv1alpha1.ReplicationSource{}
	err := client.Get(ctx, nsName, rs)
	if err != nil {
		return nil, err
	}

	return rs, nil
}