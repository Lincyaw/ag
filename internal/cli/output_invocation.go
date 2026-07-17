package cli

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/lincyaw/ag/sdk"
)

func (application *app) writeInvocationGraph(
	graph sdk.InvocationGraph,
) error {
	return application.render(graph, func(writer io.Writer) error {
		table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
		fmt.Fprintf(
			table,
			"Invocation root:\t%s\n",
			tableCell(graph.RootID),
		)
		fmt.Fprintf(
			table,
			"Operations:\t%d\n\n",
			len(graph.Operations),
		)
		if len(graph.Operations) == 0 {
			fmt.Fprintln(table, "No durable child operations.")
			return table.Flush()
		}
		fmt.Fprintln(
			table,
			"KIND\tRESOURCE\tSTATE\tINVOCATION\tPARENT\tDEPENDS ON\tGROUP\tSESSION\tTARGET SESSION",
		)
		for _, record := range graph.Operations {
			fmt.Fprintf(
				table,
				"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				tableCell(string(record.Kind)),
				tableCell(record.Resource),
				tableCell(string(record.Operation.State)),
				tableCell(record.Invocation.ID),
				tableCell(emptyAs(record.Invocation.ParentID, "-")),
				tableCell(emptyAs(
					strings.Join(
						record.Invocation.Dependencies,
						",",
					),
					"-",
				)),
				tableCell(emptyAs(record.Invocation.GroupID, "-")),
				tableCell(emptyAs(record.Invocation.SessionID, "-")),
				tableCell(emptyAs(
					record.Invocation.TargetSessionID,
					"-",
				)),
			)
		}
		return table.Flush()
	})
}
