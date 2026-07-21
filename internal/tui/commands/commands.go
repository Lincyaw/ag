package commands

// Command represents a slash command available in the editor.
type Command struct {
	Hidden       bool
	SlashCommand string
	Name         string
	Aliases      []string
	Description  string
	Args         string
}

// Registry holds available commands for completion.
type Registry struct {
	commands []Command
}

func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) Register(cmd Command) { r.commands = append(r.commands, cmd) }

func (r *Registry) Commands() []Command { return r.commands }

func (r *Registry) Lookup(name string) (Command, bool) {
	for _, c := range r.commands {
		if c.Name == name {
			return c, true
		}
		for _, a := range c.Aliases {
			if a == name {
				return c, true
			}
		}
	}
	return Command{}, false
}

// Category groups commands for display in the completion menu.
type Category struct {
	Name     string
	Commands []Command
}
