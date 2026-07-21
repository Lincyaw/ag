package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html"
	"image"
	"image/color"
	"image/png"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/charmbracelet/x/ansi"
)

type captureConfig struct {
	Name          string        `json:"name"`
	Command       string        `json:"command"`
	Directory     string        `json:"directory,omitempty"`
	Output        string        `json:"output"`
	Width         int           `json:"width"`
	Height        int           `json:"height"`
	Settle        time.Duration `json:"-"`
	SettleMS      int           `json:"settle_ms,omitempty"`
	Actions       []action      `json:"actions,omitempty"`
	WaitFor       string        `json:"wait_for,omitempty"`
	WaitTimeoutMS int           `json:"wait_timeout_ms,omitempty"`
}

type action struct {
	Text   string   `json:"text,omitempty"`
	Keys   []string `json:"keys,omitempty"`
	WaitMS int      `json:"wait_ms,omitempty"`
}

type captureMetadata struct {
	Name      string    `json:"name"`
	Command   string    `json:"command"`
	Width     int       `json:"width"`
	Height    int       `json:"height"`
	Captured  time.Time `json:"captured_at"`
	PlainPath string    `json:"plain_path"`
	ANSIPath  string    `json:"ansi_path"`
	HTMLPath  string    `json:"html_path"`
	PNGPath   string    `json:"png_path,omitempty"`
}

type comparison struct {
	Reference       string  `json:"reference"`
	Candidate       string  `json:"candidate"`
	Rows            int     `json:"rows"`
	Columns         int     `json:"columns"`
	CellCount       int     `json:"cell_count"`
	CellMismatches  int     `json:"cell_mismatches"`
	CellSimilarity  float64 `json:"cell_similarity"`
	PixelCount      int     `json:"pixel_count,omitempty"`
	PixelMismatches int     `json:"pixel_mismatches,omitempty"`
	PixelSimilarity float64 `json:"pixel_similarity,omitempty"`
	Score           float64 `json:"score"`
}

func main() {
	if len(os.Args) < 2 {
		fatal(errors.New("usage: replica-lab capture|compare ..."))
	}
	var err error
	switch os.Args[1] {
	case "capture":
		err = captureCLI(os.Args[2:])
	case "compare":
		err = compareCLI(os.Args[2:])
	default:
		err = fmt.Errorf("unknown replica-lab command %q", os.Args[1])
	}
	if err != nil {
		fatal(err)
	}
}

func captureCLI(arguments []string) error {
	flags := flag.NewFlagSet("capture", flag.ContinueOnError)
	var actions actionList
	configPath := flags.String("config", "", "capture config JSON")
	command := flags.String("command", "", "shell command to capture")
	name := flags.String("name", "capture", "capture name")
	output := flags.String("out", "", "output directory")
	directory := flags.String("cwd", "", "command working directory")
	width := flags.Int("width", 120, "terminal columns")
	height := flags.Int("height", 40, "terminal rows")
	settle := flags.Duration("settle", 1500*time.Millisecond, "initial settle delay")
	waitFor := flags.String("wait-for", "", "wait until visible pane contains text")
	waitTimeout := flags.Duration("wait-timeout", 15*time.Second, "screen readiness timeout")
	flags.Var(
		&actions,
		"action",
		"scripted action JSON; repeat to preserve action order",
	)
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	config := captureConfig{
		Name: *name, Command: *command, Directory: *directory,
		Output: *output, Width: *width, Height: *height, Settle: *settle,
		WaitFor: *waitFor, WaitTimeoutMS: int(waitTimeout.Milliseconds()),
	}
	if *configPath != "" {
		raw, err := os.ReadFile(*configPath)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(raw, &config); err != nil {
			return err
		}
		config.Settle = time.Duration(config.SettleMS) * time.Millisecond
	}
	config.Actions = append(config.Actions, actions...)
	return capture(config)
}

type actionList []action

func (actions *actionList) String() string {
	raw, _ := json.Marshal(actions)
	return string(raw)
}

func (actions *actionList) Set(value string) error {
	var parsed action
	if err := json.Unmarshal([]byte(value), &parsed); err != nil {
		return fmt.Errorf("decode capture action: %w", err)
	}
	if parsed.Text == "" && len(parsed.Keys) == 0 && parsed.WaitMS <= 0 {
		return errors.New("capture action must contain text, keys, or wait_ms")
	}
	*actions = append(*actions, parsed)
	return nil
}

func capture(config captureConfig) error {
	if strings.TrimSpace(config.Command) == "" || strings.TrimSpace(config.Output) == "" {
		return errors.New("capture command and output are required")
	}
	if config.Width < 20 || config.Height < 10 {
		return errors.New("capture dimensions are too small")
	}
	if config.Settle <= 0 {
		config.Settle = 1500 * time.Millisecond
	}
	if err := os.MkdirAll(config.Output, 0o755); err != nil {
		return err
	}
	session := fmt.Sprintf("ag-replica-%d-%d", os.Getpid(), time.Now().UnixNano())
	command := config.Command
	if config.Directory != "" {
		command = "cd " + shellQuote(config.Directory) + " && " + command
	}
	if err := run("tmux", "new-session", "-d", "-x", strconv.Itoa(config.Width),
		"-y", strconv.Itoa(config.Height), "-s", session,
		"env TERM=xterm-256color COLORTERM=truecolor "+command); err != nil {
		return fmt.Errorf("start capture session: %w", err)
	}
	defer func() {
		command := exec.Command("tmux", "kill-session", "-t", session)
		command.Stdout = io.Discard
		command.Stderr = io.Discard
		_ = command.Run()
	}()
	if config.WaitFor != "" {
		deadline := time.Now().Add(time.Duration(config.WaitTimeoutMS) * time.Millisecond)
		if config.WaitTimeoutMS <= 0 {
			deadline = time.Now().Add(15 * time.Second)
		}
		for {
			pane, err := output("tmux", "capture-pane", "-p", "-t", session)
			if err == nil && strings.Contains(string(pane), config.WaitFor) {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf(
					"pane did not contain readiness text %q before timeout",
					config.WaitFor,
				)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	time.Sleep(config.Settle)
	for _, step := range config.Actions {
		if step.Text != "" {
			if err := run("tmux", "send-keys", "-t", session, "-l", step.Text); err != nil {
				return err
			}
		}
		for _, key := range step.Keys {
			if err := run("tmux", "send-keys", "-t", session, key); err != nil {
				return err
			}
		}
		wait := time.Duration(step.WaitMS) * time.Millisecond
		if wait <= 0 {
			wait = 300 * time.Millisecond
		}
		time.Sleep(wait)
	}
	plain, err := output("tmux", "capture-pane", "-p", "-t", session)
	if err != nil {
		return fmt.Errorf("capture plain pane: %w", err)
	}
	ansi, err := output("tmux", "capture-pane", "-p", "-e", "-t", session)
	if err != nil {
		return fmt.Errorf("capture ANSI pane: %w", err)
	}
	plain = normalizeScreen(plain, config.Width, config.Height)
	if err := os.WriteFile(filepath.Join(config.Output, "screen.txt"), plain, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(config.Output, "screen.ansi"), ansi, 0o644); err != nil {
		return err
	}
	htmlPath := filepath.Join(config.Output, "screen.html")
	if err := os.WriteFile(
		htmlPath,
		[]byte(renderHTML(string(ansi), config.Width, config.Height)),
		0o644,
	); err != nil {
		return err
	}
	pngPath := filepath.Join(config.Output, "screen.png")
	if err := renderChrome(htmlPath, pngPath, config.Width, config.Height); err != nil {
		pngPath = ""
	}
	metadata := captureMetadata{
		Name: config.Name, Command: config.Command,
		Width: config.Width, Height: config.Height, Captured: time.Now().UTC(),
		PlainPath: "screen.txt", ANSIPath: "screen.ansi",
		HTMLPath: "screen.html", PNGPath: filepath.Base(pngPath),
	}
	return writeJSON(filepath.Join(config.Output, "capture.json"), metadata)
}

func compareCLI(arguments []string) error {
	flags := flag.NewFlagSet("compare", flag.ContinueOnError)
	reference := flags.String("reference", "", "reference capture directory")
	candidate := flags.String("candidate", "", "candidate capture directory")
	outputDir := flags.String("out", "", "comparison output directory")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	result, err := compareCaptures(*reference, *candidate, *outputDir)
	if err != nil {
		return err
	}
	raw, _ := json.Marshal(result)
	fmt.Println(string(raw))
	return nil
}

func compareCaptures(reference, candidate, outputDir string) (comparison, error) {
	if reference == "" || candidate == "" || outputDir == "" {
		return comparison{}, errors.New("reference, candidate, and output are required")
	}
	left, err := os.ReadFile(filepath.Join(reference, "screen.txt"))
	if err != nil {
		return comparison{}, err
	}
	right, err := os.ReadFile(filepath.Join(candidate, "screen.txt"))
	if err != nil {
		return comparison{}, err
	}
	rows, columns, cells, mismatches := compareCells(string(left), string(right))
	result := comparison{
		Reference: reference, Candidate: candidate, Rows: rows, Columns: columns,
		CellCount: cells, CellMismatches: mismatches,
		CellSimilarity: similarity(cells, mismatches),
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return comparison{}, err
	}
	pixels, pixelMismatches, pixelErr := comparePixels(
		filepath.Join(reference, "screen.png"),
		filepath.Join(candidate, "screen.png"),
		filepath.Join(outputDir, "pixel-diff.png"),
	)
	if pixelErr == nil {
		result.PixelCount = pixels
		result.PixelMismatches = pixelMismatches
		result.PixelSimilarity = similarity(pixels, pixelMismatches)
		result.Score = (result.CellSimilarity + result.PixelSimilarity) / 2
	} else {
		result.Score = result.CellSimilarity
	}
	if err := writeJSON(filepath.Join(outputDir, "report.json"), result); err != nil {
		return comparison{}, err
	}
	markdown := fmt.Sprintf(
		"# Replica comparison\n\n- Score: %.6f\n- Cell similarity: %.6f (%d/%d mismatched)\n- Pixel similarity: %.6f (%d/%d mismatched)\n\nReference: `%s`\n\nCandidate: `%s`\n",
		result.Score, result.CellSimilarity, result.CellMismatches, result.CellCount,
		result.PixelSimilarity, result.PixelMismatches, result.PixelCount,
		reference, candidate,
	)
	if err := os.WriteFile(filepath.Join(outputDir, "report.md"), []byte(markdown), 0o644); err != nil {
		return comparison{}, err
	}
	return result, nil
}

func compareCells(left, right string) (rows, columns, count, mismatches int) {
	leftRows := strings.Split(strings.TrimSuffix(left, "\n"), "\n")
	rightRows := strings.Split(strings.TrimSuffix(right, "\n"), "\n")
	rows = max(len(leftRows), len(rightRows))
	for row := 0; row < rows; row++ {
		var a, b []string
		if row < len(leftRows) {
			a = terminalCells(leftRows[row])
		}
		if row < len(rightRows) {
			b = terminalCells(rightRows[row])
		}
		columns = max(columns, max(len(a), len(b)))
		for column := 0; column < max(len(a), len(b)); column++ {
			leftCell, rightCell := " ", " "
			if column < len(a) {
				leftCell = a[column]
			}
			if column < len(b) {
				rightCell = b[column]
			}
			count++
			if leftCell != rightCell {
				mismatches++
			}
		}
	}
	return
}

func terminalCells(line string) []string {
	cells := make([]string, 0, ansi.StringWidth(line))
	for line != "" {
		cluster, width := ansi.FirstGraphemeCluster(line, ansi.GraphemeWidth)
		if cluster == "" {
			break
		}
		line = line[len(cluster):]
		if width <= 0 {
			if len(cells) > 0 {
				cells[len(cells)-1] += cluster
			}
			continue
		}
		cells = append(cells, cluster)
		for continuation := 1; continuation < width; continuation++ {
			cells = append(cells, "\x00"+cluster)
		}
	}
	return cells
}

func comparePixels(leftPath, rightPath, diffPath string) (int, int, error) {
	left, err := decodePNG(leftPath)
	if err != nil {
		return 0, 0, err
	}
	right, err := decodePNG(rightPath)
	if err != nil {
		return 0, 0, err
	}
	bounds := left.Bounds().Union(right.Bounds())
	diff := image.NewRGBA(bounds)
	count, mismatches := 0, 0
	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			count++
			a := color.RGBAModel.Convert(left.At(x, y)).(color.RGBA)
			b := color.RGBAModel.Convert(right.At(x, y)).(color.RGBA)
			if a != b {
				mismatches++
				diff.SetRGBA(x, y, color.RGBA{R: 255, A: 220})
			}
		}
	}
	file, err := os.Create(diffPath)
	if err != nil {
		return 0, 0, err
	}
	err = png.Encode(file, diff)
	return count, mismatches, errors.Join(err, file.Close())
}

func decodePNG(path string) (image.Image, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return png.Decode(file)
}

func normalizeScreen(raw []byte, width, height int) []byte {
	value := strings.ReplaceAll(string(raw), "\r", "")
	value = strings.TrimSuffix(value, "\n")
	lines := strings.Split(value, "\n")
	if len(lines) > height {
		lines = lines[len(lines)-height:]
	}
	for len(lines) < height {
		lines = append(lines, "")
	}
	for index, line := range lines {
		lines[index] = strings.TrimRight(ansi.Truncate(line, width, ""), " ")
	}
	return []byte(strings.Join(lines, "\n") + "\n")
}

func renderHTML(ansi string, width, height int) string {
	content := ansiHTML(ansi)
	return fmt.Sprintf(`<!doctype html><meta charset="utf-8"><style>
html,body{margin:0;width:%dch;height:%dpx;overflow:hidden;background:#0d0d0d}
body{color:#e6e6e6;font:14px/18px Menlo,"SF Mono",monospace}
pre{margin:0;white-space:pre;width:%dch;height:%dpx}.bold{font-weight:700}.dim{opacity:.65}.underline{text-decoration:underline}
</style><pre>%s</pre>`, width, height*18, width, height*18, content)
}

type ansiStyle struct {
	fg, bg               string
	bold, dim, underline bool
}

func ansiHTML(input string) string {
	var result strings.Builder
	style := ansiStyle{}
	open := false
	writeStyle := func(next ansiStyle) {
		if open {
			result.WriteString("</span>")
		}
		style = next
		css := []string{}
		classes := []string{}
		if style.fg != "" {
			css = append(css, "color:"+style.fg)
		}
		if style.bg != "" {
			css = append(css, "background:"+style.bg)
		}
		if style.bold {
			classes = append(classes, "bold")
		}
		if style.dim {
			classes = append(classes, "dim")
		}
		if style.underline {
			classes = append(classes, "underline")
		}
		result.WriteString(`<span class="` + strings.Join(classes, " ") + `" style="` + strings.Join(css, ";") + `">`)
		open = true
	}
	writeStyle(style)
	for index := 0; index < len(input); {
		if input[index] == 0x1b && index+1 < len(input) && input[index+1] == '[' {
			end := index + 2
			for end < len(input) && (input[end] < '@' || input[end] > '~') {
				end++
			}
			if end < len(input) {
				if input[end] == 'm' {
					style = applySGR(style, input[index+2:end])
					writeStyle(style)
				}
				index = end + 1
				continue
			}
		}
		_, size := utf8.DecodeRuneInString(input[index:])
		if size == 0 {
			size = 1
		}
		result.WriteString(html.EscapeString(input[index : index+size]))
		index += size
	}
	if open {
		result.WriteString("</span>")
	}
	return result.String()
}

func applySGR(style ansiStyle, parameters string) ansiStyle {
	values := []int{0}
	if parameters != "" {
		values = nil
		for _, raw := range strings.Split(parameters, ";") {
			value, _ := strconv.Atoi(raw)
			values = append(values, value)
		}
	}
	palette := []string{"#1b1b1b", "#d75f5f", "#5faf5f", "#d7af5f", "#5f87d7", "#af5fd7", "#5fafaf", "#d0d0d0"}
	for index := 0; index < len(values); index++ {
		value := values[index]
		switch {
		case value == 0:
			style = ansiStyle{}
		case value == 1:
			style.bold = true
		case value == 2:
			style.dim = true
		case value == 4:
			style.underline = true
		case value == 22:
			style.bold, style.dim = false, false
		case value == 24:
			style.underline = false
		case value == 39:
			style.fg = ""
		case value == 49:
			style.bg = ""
		case value >= 30 && value <= 37:
			style.fg = palette[value-30]
		case value >= 40 && value <= 47:
			style.bg = palette[value-40]
		case value >= 90 && value <= 97:
			style.fg = palette[value-90]
		case value >= 100 && value <= 107:
			style.bg = palette[value-100]
		case (value == 38 || value == 48) && index+4 < len(values) && values[index+1] == 2:
			color := fmt.Sprintf("#%02x%02x%02x", values[index+2], values[index+3], values[index+4])
			if value == 38 {
				style.fg = color
			} else {
				style.bg = color
			}
			index += 4
		}
	}
	return style
}

func renderChrome(htmlPath, pngPath string, width, height int) error {
	chrome := "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
	if _, err := os.Stat(chrome); err != nil {
		return err
	}
	absolute, err := filepath.Abs(htmlPath)
	if err != nil {
		return err
	}
	return run(chrome, "--headless", "--disable-gpu", "--hide-scrollbars",
		"--force-device-scale-factor=1", fmt.Sprintf("--window-size=%d,%d", width*9, height*18),
		"--screenshot="+pngPath, "file://"+absolute)
}

func similarity(total, mismatches int) float64 {
	if total == 0 {
		return 1
	}
	return 1 - float64(mismatches)/float64(total)
}

func run(name string, arguments ...string) error {
	command := exec.Command(name, arguments...)
	command.Stdout = io.Discard
	command.Stderr = os.Stderr
	return command.Run()
}

func output(name string, arguments ...string) ([]byte, error) {
	command := exec.Command(name, arguments...)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	raw, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return raw, nil
}

func writeJSON(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}

func shellQuote(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'" }

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "replica-lab:", err)
	os.Exit(1)
}
