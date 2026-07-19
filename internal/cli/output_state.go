package cli

import (
	"fmt"
	"io"

	"github.com/lincyaw/ag/sdk"
)

type stateOutput struct {
	Backend            string                  `json:"backend"`
	Namespace          string                  `json:"namespace"`
	Selection          string                  `json:"selection,omitempty"`
	LegacyFileFallback bool                    `json:"legacy_file_fallback,omitempty"`
	Capabilities       sdk.StorageCapabilities `json:"capabilities"`
}

type prunePreviewOutput struct {
	Cutoff string `json:"cutoff"`
	DryRun bool   `json:"dry_run"`
}

func (application *app) writeState(value stateOutput) error {
	return application.render(value, func(writer io.Writer) error {
		table := newTable(writer)
		fmt.Fprintf(table, "Backend:\t%s\n", tableCell(value.Backend))
		fmt.Fprintf(table, "Namespace:\t%s\n", tableCell(emptyAs(value.Namespace, "default")))
		if value.Selection != "" {
			fmt.Fprintf(table, "Selection:\t%s\n", tableCell(value.Selection))
		}
		if err := table.Flush(); err != nil {
			return err
		}
		if value.LegacyFileFallback {
			if _, err := fmt.Fprintln(
				writer,
				"\nWarning: legacy file state was detected, so ag preserved the file backend for resume compatibility. New state directories default to DuckDB.",
			); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintln(writer, "\nCapabilities:"); err != nil {
			return err
		}
		capabilities := newTable(writer)
		fmt.Fprintf(capabilities, "  Durable:\t%s\n", yesNo(value.Capabilities.Durable))
		fmt.Fprintf(capabilities, "  Multi-process safe:\t%s\n", yesNo(value.Capabilities.MultiProcessSafe))
		fmt.Fprintf(capabilities, "  Atomic state:\t%s\n", yesNo(value.Capabilities.AtomicState))
		fmt.Fprintf(capabilities, "  Pagination:\t%s\n", yesNo(value.Capabilities.Pagination))
		fmt.Fprintf(capabilities, "  Maintenance:\t%s\n", yesNo(value.Capabilities.Maintenance))
		fmt.Fprintf(capabilities, "  Operation fencing:\t%s\n", yesNo(value.Capabilities.OperationFencing))
		fmt.Fprintf(capabilities, "  Named queues:\t%s\n", yesNo(value.Capabilities.NamedQueues))
		fmt.Fprintf(capabilities, "  Namespace isolation:\t%s\n", yesNo(value.Capabilities.NamespaceIsolation))
		fmt.Fprintf(capabilities, "  Encrypted at rest:\t%s\n", yesNo(value.Capabilities.EncryptedAtRest))
		return capabilities.Flush()
	})
}

func (application *app) writePrune(result sdk.PruneResult) error {
	return application.render(result, func(writer io.Writer) error {
		if _, err := fmt.Fprintln(writer, "State pruning complete."); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(writer); err != nil {
			return err
		}
		table := newTable(writer)
		fmt.Fprintf(table, "Operations deleted:\t%d\n", result.Operations)
		fmt.Fprintf(table, "Deliveries deleted:\t%d\n", result.Deliveries)
		fmt.Fprintf(table, "Trajectories deleted:\t%d\n", result.Trajectories)
		return table.Flush()
	})
}

func (application *app) writePrunePreview(value prunePreviewOutput) error {
	return application.render(value, func(writer io.Writer) error {
		if _, err := fmt.Fprintln(writer, "Would prune terminal state."); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(writer); err != nil {
			return err
		}
		table := newTable(writer)
		fmt.Fprintf(table, "Cutoff:\t%s\n", tableCell(value.Cutoff))
		fmt.Fprintf(table, "Dry run:\tyes\n")
		return table.Flush()
	})
}
