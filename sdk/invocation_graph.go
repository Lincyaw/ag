package sdk

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// InvocationGraph is the durable read model for one invocation root.
type InvocationGraph struct {
	RootID     string            `json:"root_id"`
	Nodes      []InvocationNode  `json:"nodes,omitempty"`
	Operations []OperationRecord `json:"operations"`
}

// InvocationNode is one projected node in an invocation graph. Record is nil
// for synthetic nodes such as the root trajectory execution or a referenced
// parent/dependency whose operation record has been pruned.
type InvocationNode struct {
	ID           string           `json:"id"`
	Invocation   Invocation       `json:"invocation,omitempty"`
	Record       *OperationRecord `json:"record,omitempty"`
	ParentID     string           `json:"parent_id,omitempty"`
	Children     []string         `json:"children,omitempty"`
	Dependencies []string         `json:"dependencies,omitempty"`
	Dependents   []string         `json:"dependents,omitempty"`
}

// InvocationGraphStore is the minimal operation read port required to project
// a rooted invocation graph.
type InvocationGraphStore interface {
	ListByInvocationRoot(
		context.Context,
		string,
	) ([]OperationRecord, error)
}

// LoadInvocationGraph projects durable operation records into one causal
// invocation graph. The root agent trajectory execution may not itself have an
// operation record; RootID still identifies it and every child node points to
// it through Invocation.RootID.
func LoadInvocationGraph(
	ctx context.Context,
	store InvocationGraphStore,
	rootID string,
) (InvocationGraph, error) {
	if store == nil {
		return InvocationGraph{}, errors.New("invocation graph store is nil")
	}
	if err := ValidateResourceName(
		"invocation root",
		rootID,
	); err != nil {
		return InvocationGraph{}, err
	}
	records, err := store.ListByInvocationRoot(ctx, rootID)
	if err != nil {
		return InvocationGraph{}, err
	}
	records = slices.DeleteFunc(records, func(record OperationRecord) bool {
		return record.Invocation.RootID != rootID
	})
	return BuildInvocationGraph(rootID, records)
}

// BuildInvocationGraph projects operation records into a rooted invocation
// graph. It keeps Operations for compatibility, while Nodes is the canonical
// graph projection presenters should use when they need relationships.
func BuildInvocationGraph(
	rootID string,
	records []OperationRecord,
) (InvocationGraph, error) {
	if err := ValidateResourceName(
		"invocation root",
		rootID,
	); err != nil {
		return InvocationGraph{}, err
	}
	graph := InvocationGraph{RootID: rootID}
	nodes := map[string]*InvocationNode{
		rootID: {ID: rootID},
	}
	for _, record := range records {
		if record.Invocation.RootID != rootID {
			continue
		}
		id := record.Invocation.ID
		if id == "" {
			continue
		}
		if existing := nodes[id]; existing != nil && existing.Record != nil {
			return InvocationGraph{}, fmt.Errorf(
				"invocation graph %q contains duplicate node %q",
				rootID,
				id,
			)
		}
		cloned := CloneOperationRecord(record)
		nodes[id] = &InvocationNode{
			ID:           id,
			Invocation:   CloneInvocation(cloned.Invocation),
			Record:       &cloned,
			ParentID:     cloned.Invocation.ParentID,
			Dependencies: slices.Clone(cloned.Invocation.Dependencies),
		}
	}
	references := make([]string, 0)
	for _, node := range nodes {
		if node.ParentID != "" {
			references = append(references, node.ParentID)
		}
		for _, dependency := range node.Dependencies {
			references = append(references, dependency)
		}
	}
	for _, id := range references {
		ensureInvocationNode(nodes, id)
	}
	addEdge := func(from, to string, child bool) {
		if from == "" || to == "" || from == to {
			return
		}
		source := ensureInvocationNode(nodes, from)
		if child {
			source.Children = appendUniqueInvocationID(
				source.Children,
				to,
			)
			return
		}
		source.Dependents = appendUniqueInvocationID(
			source.Dependents,
			to,
		)
	}
	for _, node := range nodes {
		addEdge(node.ParentID, node.ID, true)
		for _, dependency := range node.Dependencies {
			addEdge(dependency, node.ID, false)
		}
	}
	compare := func(left, right string) int {
		return compareInvocationGraphNodes(rootID, nodes[left], nodes[right])
	}
	ordered, err := sortInvocationGraphNodes(rootID, nodes, compare)
	if err != nil {
		return InvocationGraph{}, err
	}
	sortInvocationGraphEdges(nodes, ordered, compare)
	graph.Nodes = make([]InvocationNode, 0, len(ordered))
	for _, id := range ordered {
		node := cloneInvocationNode(*nodes[id])
		graph.Nodes = append(graph.Nodes, node)
		if node.Record != nil {
			graph.Operations = append(
				graph.Operations,
				CloneOperationRecord(*node.Record),
			)
		}
	}
	return graph, nil
}

// OrderedOperations returns the operation records in graph order. Graphs built
// before Nodes existed still expose their Operations list unchanged.
func (graph InvocationGraph) OrderedOperations() []OperationRecord {
	result := make([]OperationRecord, 0, len(graph.Operations))
	if len(graph.Nodes) == 0 {
		for _, record := range graph.Operations {
			result = append(result, CloneOperationRecord(record))
		}
		return result
	}
	for _, node := range graph.Nodes {
		if node.Record == nil {
			continue
		}
		result = append(result, CloneOperationRecord(*node.Record))
	}
	return result
}

func ensureInvocationNode(
	nodes map[string]*InvocationNode,
	id string,
) *InvocationNode {
	if node := nodes[id]; node != nil {
		return node
	}
	node := &InvocationNode{ID: id}
	nodes[id] = node
	return node
}

func appendUniqueInvocationID(values []string, value string) []string {
	if slices.Contains(values, value) {
		return values
	}
	return append(values, value)
}

func sortInvocationIDs(
	values []string,
	compare func(string, string) int,
) {
	slices.SortFunc(values, compare)
}

func sortInvocationGraphNodes(
	rootID string,
	nodes map[string]*InvocationNode,
	compare func(string, string) int,
) ([]string, error) {
	inbound := make(map[string]int, len(nodes))
	outbound := make(map[string][]string, len(nodes))
	for id := range nodes {
		inbound[id] = 0
	}
	addOrderEdge := func(from, to string) {
		if from == "" || to == "" || from == to ||
			nodes[from] == nil || nodes[to] == nil {
			return
		}
		if slices.Contains(outbound[from], to) {
			return
		}
		outbound[from] = append(outbound[from], to)
		inbound[to]++
	}
	for _, node := range nodes {
		addOrderEdge(node.ParentID, node.ID)
		for _, dependency := range node.Dependencies {
			addOrderEdge(dependency, node.ID)
		}
	}
	ready := make([]string, 0, len(nodes))
	for id, count := range inbound {
		if count == 0 {
			ready = append(ready, id)
		}
	}
	sortInvocationIDs(ready, compare)
	ordered := make([]string, 0, len(nodes))
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		ordered = append(ordered, id)
		sortInvocationIDs(outbound[id], compare)
		for _, target := range outbound[id] {
			inbound[target]--
			if inbound[target] == 0 {
				ready = append(ready, target)
			}
		}
		sortInvocationIDs(ready, compare)
	}
	if len(ordered) != len(nodes) {
		return nil, fmt.Errorf(
			"invocation graph %q contains a parent/dependency cycle",
			rootID,
		)
	}
	return ordered, nil
}

func sortInvocationGraphEdges(
	nodes map[string]*InvocationNode,
	ordered []string,
	compare func(string, string) int,
) {
	positions := make(map[string]int, len(ordered))
	for index, id := range ordered {
		positions[id] = index
	}
	byGraphOrder := func(left, right string) int {
		leftPosition, leftFound := positions[left]
		rightPosition, rightFound := positions[right]
		if leftFound && rightFound && leftPosition != rightPosition {
			if leftPosition < rightPosition {
				return -1
			}
			return 1
		}
		return compare(left, right)
	}
	for _, node := range nodes {
		sortInvocationIDs(node.Children, byGraphOrder)
		sortInvocationIDs(node.Dependents, byGraphOrder)
	}
}

func compareInvocationGraphNodes(
	rootID string,
	left *InvocationNode,
	right *InvocationNode,
) int {
	if order := compareRootInvocationNode(rootID, left, right); order != 0 {
		return order
	}
	leftInvocation, rightInvocation := Invocation{}, Invocation{}
	if left != nil {
		leftInvocation = left.Invocation
	}
	if right != nil {
		rightInvocation = right.Invocation
	}
	if leftInvocation.Ordinal != rightInvocation.Ordinal {
		if leftInvocation.Ordinal < rightInvocation.Ordinal {
			return -1
		}
		return 1
	}
	if order := compareInvocationRecordTime(left, right); order != 0 {
		return order
	}
	return strings.Compare(invocationNodeID(left), invocationNodeID(right))
}

func compareRootInvocationNode(
	rootID string,
	left *InvocationNode,
	right *InvocationNode,
) int {
	leftRoot := invocationNodeID(left) == rootID
	rightRoot := invocationNodeID(right) == rootID
	switch {
	case leftRoot && !rightRoot:
		return -1
	case rightRoot && !leftRoot:
		return 1
	default:
		return 0
	}
}

func compareInvocationRecordTime(
	left *InvocationNode,
	right *InvocationNode,
) int {
	if left == nil || right == nil ||
		left.Record == nil || right.Record == nil {
		return compareInvocationRecordPresence(left, right)
	}
	if order := left.Record.Operation.SubmittedAt.Compare(
		right.Record.Operation.SubmittedAt,
	); order != 0 {
		return order
	}
	return strings.Compare(
		left.Record.Operation.ID,
		right.Record.Operation.ID,
	)
}

func compareInvocationRecordPresence(
	left *InvocationNode,
	right *InvocationNode,
) int {
	leftHasRecord := left != nil && left.Record != nil
	rightHasRecord := right != nil && right.Record != nil
	switch {
	case leftHasRecord && !rightHasRecord:
		return -1
	case rightHasRecord && !leftHasRecord:
		return 1
	default:
		return 0
	}
}

func invocationNodeID(node *InvocationNode) string {
	if node == nil {
		return ""
	}
	return node.ID
}

func cloneInvocationNode(node InvocationNode) InvocationNode {
	node.Invocation = CloneInvocation(node.Invocation)
	node.Children = slices.Clone(node.Children)
	node.Dependencies = slices.Clone(node.Dependencies)
	node.Dependents = slices.Clone(node.Dependents)
	if node.Record != nil {
		record := CloneOperationRecord(*node.Record)
		node.Record = &record
	}
	return node
}
