package cli

import (
	"slices"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type commandSchema struct {
	Name            string          `json:"name"`
	Use             string          `json:"use"`
	Short           string          `json:"short,omitempty"`
	Long            string          `json:"long,omitempty"`
	Example         string          `json:"example,omitempty"`
	Aliases         []string        `json:"aliases,omitempty"`
	Flags           []flagSchema    `json:"flags,omitempty"`
	PersistentFlags []flagSchema    `json:"persistent_flags,omitempty"`
	Commands        []commandSchema `json:"commands,omitempty"`
}

type flagSchema struct {
	Name          string   `json:"name"`
	Shorthand     string   `json:"shorthand,omitempty"`
	Usage         string   `json:"usage"`
	Type          string   `json:"type"`
	Default       string   `json:"default,omitempty"`
	NoOptDefault  string   `json:"no_opt_default,omitempty"`
	AllowedValues []string `json:"allowed_values,omitempty"`
	Repeatable    bool     `json:"repeatable,omitempty"`
}

func (application *app) writeSchema(command *cobra.Command) error {
	return writeJSON(application.stdout, schemaForCommand(command))
}

func schemaForCommand(command *cobra.Command) commandSchema {
	schema := commandSchema{
		Name:    command.Name(),
		Use:     command.UseLine(),
		Short:   command.Short,
		Long:    command.Long,
		Example: command.Example,
		Aliases: slices.Clone(command.Aliases),
	}
	schema.Flags = flagsForSchema(command.LocalFlags())
	schema.PersistentFlags = flagsForSchema(command.PersistentFlags())
	for _, child := range command.Commands() {
		if child.Hidden {
			continue
		}
		schema.Commands = append(schema.Commands, schemaForCommand(child))
	}
	return schema
}

func flagsForSchema(flags *pflag.FlagSet) []flagSchema {
	if flags == nil {
		return nil
	}
	var result []flagSchema
	flags.VisitAll(func(flag *pflag.Flag) {
		if flag.Hidden {
			return
		}
		entry := flagSchema{
			Name:         flag.Name,
			Shorthand:    flag.Shorthand,
			Usage:        flag.Usage,
			Type:         flag.Value.Type(),
			Default:      flag.DefValue,
			NoOptDefault: flag.NoOptDefVal,
			Repeatable:   repeatableFlag(flag.Value.Type()),
		}
		switch flag.Name {
		case "output":
			entry.AllowedValues = []string{outputText, outputJSON}
		case "progress":
			entry.AllowedValues = []string{
				progressAuto,
				progressTUI,
				progressPlain,
				progressAlways,
				progressNever,
			}
		case "color":
			entry.AllowedValues = []string{
				colorAuto,
				colorAlways,
				colorNever,
			}
		}
		result = append(result, entry)
	})
	return result
}

func repeatableFlag(flagType string) bool {
	switch flagType {
	case "stringSlice", "stringToString":
		return true
	default:
		return false
	}
}
