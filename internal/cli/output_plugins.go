package cli

import (
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/lincyaw/ag/registry"
	"github.com/lincyaw/ag/sdk"
)

type pluginDiscovery struct {
	Name         string            `json:"name"`
	URI          string            `json:"uri,omitempty"`
	Description  string            `json:"description,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
	Scheme       string            `json:"scheme"`
	Namespace    string            `json:"namespace"`
	InstanceID   string            `json:"instance_id"`
	Version      string            `json:"version"`
	RegisteredAt time.Time         `json:"registered_at"`
	UpdatedAt    time.Time         `json:"updated_at"`
	ExpiresAt    time.Time         `json:"expires_at"`
	Revision     uint64            `json:"revision"`
	Epoch        uint64            `json:"epoch"`
}

func (application *app) writePlugins(descriptors []sdk.PluginDescriptor) error {
	return application.render(descriptors, func(writer io.Writer) error {
		if len(descriptors) == 0 {
			_, err := fmt.Fprintln(writer, "No plugins found.")
			return err
		}
		table := newTable(writer)
		fmt.Fprintln(table, "NAME\tSCHEME\tURI\tDESCRIPTION")
		for _, descriptor := range descriptors {
			fmt.Fprintf(
				table,
				"%s\t%s\t%s\t%s\n",
				tableCell(descriptor.Name),
				tableCell(descriptor.Scheme),
				tableCell(emptyAs(descriptor.URI, "-")),
				tableCell(emptyAs(descriptor.Description, "-")),
			)
		}
		return table.Flush()
	})
}

func (application *app) writeRegistryReady(value registryReady) error {
	return application.render(value, func(writer io.Writer) error {
		if _, err := fmt.Fprintln(writer, "Registry ready"); err != nil {
			return err
		}
		return writeSection(
			writer,
			"Endpoint",
			[2]string{"URI", value.URI},
			[2]string{"Listen", value.Listen},
			[2]string{"Backend", value.Backend},
			[2]string{"PID", fmt.Sprint(value.PID)},
		)
	})
}

func (application *app) writePluginInstances(
	instances []registry.PluginInstance,
) error {
	values := make([]pluginDiscovery, 0, len(instances))
	for _, instance := range instances {
		scheme := ""
		if parsed, err := url.Parse(instance.URI); err == nil {
			scheme = parsed.Scheme
		}
		values = append(values, pluginDiscovery{
			Name:         instance.Name,
			URI:          instance.URI,
			Description:  instance.Manifest.Description,
			Labels:       instance.Labels,
			Scheme:       scheme,
			Namespace:    instance.Namespace,
			InstanceID:   instance.InstanceID,
			Version:      instance.Manifest.Version,
			RegisteredAt: instance.RegisteredAt,
			UpdatedAt:    instance.UpdatedAt,
			ExpiresAt:    instance.ExpiresAt,
			Revision:     instance.Revision,
			Epoch:        instance.Epoch,
		})
	}
	return application.render(values, func(writer io.Writer) error {
		if len(values) == 0 {
			_, err := fmt.Fprintln(
				writer,
				"No active plugin instances found.",
			)
			return err
		}
		table := newTable(writer)
		fmt.Fprintln(
			table,
			"NAMESPACE\tNAME\tINSTANCE\tVERSION\tURI\tEXPIRES\tLABELS",
		)
		for _, value := range values {
			fmt.Fprintf(
				table,
				"%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				tableCell(value.Namespace),
				tableCell(value.Name),
				tableCell(value.InstanceID),
				tableCell(value.Version),
				tableCell(value.URI),
				formatTime(value.ExpiresAt),
				tableCell(formatLabels(value.Labels)),
			)
		}
		return table.Flush()
	})
}

func (application *app) writeManifest(manifest sdk.Manifest) error {
	return application.render(manifest, func(writer io.Writer) error {
		minimumAPI, maximumAPI := manifest.APIRange()
		apiVersion := fmt.Sprint(minimumAPI)
		if minimumAPI != maximumAPI {
			apiVersion = fmt.Sprintf("%d-%d", minimumAPI, maximumAPI)
		}
		table := newTable(writer)
		fmt.Fprintf(table, "Name:\t%s\n", tableCell(manifest.Name))
		fmt.Fprintf(table, "Version:\t%s\n", tableCell(manifest.Version))
		fmt.Fprintf(table, "Description:\t%s\n", tableCell(manifest.Description))
		fmt.Fprintf(table, "API version:\t%s\n", apiVersion)
		fmt.Fprintf(table, "Requires:\t%s\n", listOrNone(manifest.Requires))
		fmt.Fprintf(table, "Conflicts:\t%s\n", listOrNone(manifest.Conflicts))
		if err := table.Flush(); err != nil {
			return err
		}
		if len(manifest.Registers) == 0 {
			_, err := fmt.Fprintln(writer, "\nRegisters: none")
			return err
		}
		if _, err := fmt.Fprintln(writer, "\nRegisters:"); err != nil {
			return err
		}
		for _, resource := range manifest.Registers {
			if _, err := fmt.Fprintf(writer, "  - %s\n", tableCell(resource)); err != nil {
				return err
			}
		}
		return nil
	})
}
