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
	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
)

// pvBackupRelationship defines the "type" of pvBackup Relationships
const PVBackupRelationshipType RelationshipType = "PVBackup"

// pvBackupRelationship holds the config state for pvBackup-type
// relationships
type pvBackupRelationship struct {
	Relationship
	data *pvBackupRelationshipData
}

// pvBackupRelationshipData is the state that will be saved to the
// relationship config file
type pvBackupRelationshipData struct {
	Version int
	// Config info for the source side of the relationship
	Source *pvBackupRelationshipSource
}

type resticConfig struct {
	viper.Viper
	name string
}

type pvBackupRelationshipSource struct {
	// Cluster context name
	Cluster string
	// Namespace on source cluster
	Namespace string
	// Name of PVC being replicated
	PVCName string
	// Name of ReplicationSource object
	RSName string
	// Parameters for the ReplicationSource
	Source volsyncv1alpha1.ReplicationSourceResticSpec
	// Scheduling parameters
	Trigger volsyncv1alpha1.ReplicationSourceTriggerSpec
}

func (pr *pvBackupRelationship) Save() error {
	err := pr.SetData(pr.data)
	if err != nil {
		return err
	}
	return pr.Relationship.Save()
}

func newPVBackupRelationship(cmd *cobra.Command) (*pvBackupRelationship, error) {
	r, err := CreateRelationshipFromCommand(cmd, PVBackupRelationshipType)
	if err != nil {
		return nil, err
	}

	return &pvBackupRelationship{
		Relationship: *r,
		data: &pvBackupRelationshipData{
			Version: 1,
		},
	}, nil
}

// pvBackupCmd represents the pvBackup command
var pvBackupCmd = &cobra.Command{
	Use:   "pv-backup",
	Short: i18n.T("Back up/Restore data into/from a restic repository"),
	Long: templates.LongDesc(i18n.T(`
	Automated backup/restore data from a restic repository.

	This set of commands is designed to configure the restic repository to provide
	automatic backups and restore`)),
}

func init() {
	rootCmd.AddCommand(pvBackupCmd)

	pvBackupCmd.PersistentFlags().StringP("relationship", "r", "", "relationship name")
	cobra.CheckErr(pvBackupCmd.MarkPersistentFlagRequired("relationship"))
	cobra.CheckErr(viper.BindPFlag("relationship", pvBackupCmd.PersistentFlags().Lookup("relationship")))
}
