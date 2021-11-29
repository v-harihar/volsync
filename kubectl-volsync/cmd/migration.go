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
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
	"sigs.k8s.io/controller-runtime/pkg/client"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
)

// MigrationRelationship defines the "type" of migration Relationships
const MigrationRelationshipType RelationshipType = "migration"

// migrationRelationship holds the config state for migration-type
// relationships
type migrationRelationship struct {
	Relationship
	data migrationRelationshipData
}

// migrationRelationshipData is the state that will be saved to the
// relationship config file
type migrationRelationshipData struct {
	Version     int
	Source      *migrationRelationshipSource
	Destination *migrationRelationshipDestination
}

type migrationRelationshipSource struct {
	// Volume to be migrated
	Volume string
	// Total volume size
	Size *resource.Quantity
}

type migrationRelationshipDestination struct {
	// Cluster context name
	Cluster string
	// Namespace on destination cluster
	Namespace string
	// Name of PVC being replicated
	PVCName string
	// PVC object
	PVC *v1.PersistentVolumeClaim
	// Name of the migrationDestination object
	MDName string
	// client object associated with a cluster
	clientObject client.Client
	// Name of Secret holding SSH keys
	SSHKeyName string
	// Parameters for the migrationDestination
	Destination volsyncv1alpha1.ReplicationDestinationRsyncSpec
}

func (mr *migrationRelationship) Save() error {
	mr.Set("data", mr.data)
	// resource.Quantity doesn't properly encode, so we need to do it manually
	/*
		if mr.data.Source != nil && mr.data.Source.Source.Capacity != nil {
			mr.Set("data.source.source.capacity", mr.data.Source.Source.Capacity.String())
		}
	*/
	if mr.data.Destination != nil && mr.data.Destination.Destination.Capacity != nil {
		mr.Set("data.destination.destination.capacity", mr.data.Destination.Destination.Capacity.String())
	}
	return mr.Relationship.Save()
}

func newmigrationRelationship(cmd *cobra.Command) (*migrationRelationship, error) {
	r, err := CreateRelationshipFromCommand(cmd, MigrationRelationshipType)
	if err != nil {
		return nil, err
	}

	return &migrationRelationship{
		Relationship: *r,
		data: migrationRelationshipData{
			Version: 1,
		},
	}, nil
}

// migrationCmd represents the migration command
var migrationCmd = &cobra.Command{
	Use:   "migration",
	Short: i18n.T("Migrate data into a PersistentVolume"),
	Long: templates.LongDesc(i18n.T(`
	Copy data from an external file system into a Kubernetes PersistentVolume.

	This set of commands is designed to help provision a PV and copy data from
	a directory tree into that newly provisioned volume.
	`)),
}

func init() {
	rootCmd.AddCommand(migrationCmd)

	migrationCmd.PersistentFlags().StringP("relationship", "r", "", "relationship name")
	cobra.CheckErr(migrationCmd.MarkPersistentFlagRequired("relationship"))
	cobra.CheckErr(viper.BindPFlag("relationship", migrationCmd.PersistentFlags().Lookup("relationship")))
}
