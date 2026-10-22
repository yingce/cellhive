package main

import (
	"github.com/spf13/cobra"
)

func newDiagnoseCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "diagnose",
		Short: "diagnose control-plane and object-store connectivity",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return diagnose()
		},
	}
}
