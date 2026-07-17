package file

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/lincyaw/ag/sdk"
)

const (
	defaultMaxReadBytes  int64 = 1 << 20
	defaultMaxWriteBytes int64 = 1 << 20
	defaultMaxEntries          = 1000
	defaultReadLineLimit       = 250
)

type Config struct {
	Root          string
	MaxReadBytes  int64
	MaxWriteBytes int64
	MaxEntries    int
	EnableWrite   bool
}

type plugin struct {
	config Config
}

func New(config Config) sdk.Plugin { return &plugin{config: config} }

func (plugin *plugin) Manifest() sdk.Manifest {
	registers := []string{
		sdk.ToolResource("read_file"),
		sdk.ToolResource("list_files"),
		sdk.ToolResource("search_files"),
	}
	if plugin.config.EnableWrite {
		registers = append(
			registers,
			sdk.ToolResource("write_file"),
			sdk.ToolResource("edit_file"),
		)
	}
	return sdk.Manifest{
		Name:        "file",
		Version:     "1.1.0",
		Description: "root-confined, revision-aware file tools for agent-native read, search, and edit workflows",
		APIVersion:  sdk.APIVersion,
		Registers:   registers,
	}
}

func (plugin *plugin) Install(
	_ context.Context,
	registrar sdk.Registrar,
) error {
	filesystem, err := newRootedFS(plugin.config)
	if err != nil {
		return err
	}
	if err := registrar.RegisterTool(readTool{filesystem: filesystem}); err != nil {
		return err
	}
	if err := registrar.RegisterTool(listTool{filesystem: filesystem}); err != nil {
		return err
	}
	if err := registrar.RegisterTool(searchTool{filesystem: filesystem}); err != nil {
		return err
	}
	if plugin.config.EnableWrite {
		if err := registrar.RegisterTool(writeTool{filesystem: filesystem}); err != nil {
			return err
		}
		return registrar.RegisterTool(editTool{filesystem: filesystem})
	}
	return nil
}

type rootedFS struct {
	root          string
	maxReadBytes  int64
	maxWriteBytes int64
	maxEntries    int
	writeMu       sync.Mutex
}

func newRootedFS(config Config) (*rootedFS, error) {
	root := strings.TrimSpace(config.Root)
	if root == "" {
		root = "."
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve file root: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return nil, fmt.Errorf("resolve file root symlinks: %w", err)
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return nil, fmt.Errorf("stat file root: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("file root %q is not a directory", resolved)
	}
	if config.MaxReadBytes == 0 {
		config.MaxReadBytes = defaultMaxReadBytes
	}
	if config.MaxWriteBytes == 0 {
		config.MaxWriteBytes = defaultMaxWriteBytes
	}
	if config.MaxEntries == 0 {
		config.MaxEntries = defaultMaxEntries
	}
	if config.MaxReadBytes < 1 || config.MaxWriteBytes < 1 || config.MaxEntries < 1 {
		return nil, errors.New("file limits must be positive")
	}
	return &rootedFS{
		root:          resolved,
		maxReadBytes:  config.MaxReadBytes,
		maxWriteBytes: config.MaxWriteBytes,
		maxEntries:    config.MaxEntries,
	}, nil
}

func (filesystem *rootedFS) existing(relative string) (string, error) {
	name, err := relativePath(relative)
	if err != nil {
		return "", err
	}
	root, err := filesystem.openRoot()
	if err != nil {
		return "", err
	}
	defer root.Close()
	if _, err := root.Stat(name); err != nil {
		return "", err
	}
	return name, nil
}

func (filesystem *rootedFS) writable(relative string) (string, error) {
	name, err := relativePath(relative)
	if err != nil {
		return "", err
	}
	base := filepath.Base(name)
	if base == "." || base == string(filepath.Separator) {
		return "", errors.New("file path has no basename")
	}
	root, err := filesystem.openRoot()
	if err != nil {
		return "", err
	}
	defer root.Close()
	info, err := root.Stat(filepath.Dir(name))
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", errors.New("destination parent is not a directory")
	}
	if info, err := root.Lstat(name); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("refusing to replace a symbolic link")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	return name, nil
}

func relativePath(relative string) (string, error) {
	value := strings.TrimSpace(relative)
	if value == "" {
		return "", errors.New("path is empty")
	}
	if filepath.IsAbs(value) {
		return "", errors.New("path must be relative to the file root")
	}
	clean := filepath.Clean(value)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", errors.New("path escapes the file root")
	}
	return clean, nil
}

func (filesystem *rootedFS) openRoot() (*os.Root, error) {
	return os.OpenRoot(filesystem.root)
}

func (filesystem *rootedFS) readText(path string) ([]byte, os.FileInfo, error) {
	root, err := filesystem.openRoot()
	if err != nil {
		return nil, nil, err
	}
	defer root.Close()
	return filesystem.readTextAt(root, path)
}

func (filesystem *rootedFS) readTextAt(
	root *os.Root,
	path string,
) ([]byte, os.FileInfo, error) {
	file, err := root.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, nil, errors.New("path is not a regular file")
	}
	data, err := io.ReadAll(io.LimitReader(file, filesystem.maxReadBytes+1))
	if err != nil {
		return nil, nil, err
	}
	if int64(len(data)) > filesystem.maxReadBytes {
		return nil, nil, fmt.Errorf("file exceeds %d byte read limit", filesystem.maxReadBytes)
	}
	if !utf8.Valid(data) {
		return nil, nil, errors.New("file is not valid UTF-8 text")
	}
	return data, info, nil
}

func (filesystem *rootedFS) atomicWrite(
	ctx context.Context,
	target string,
	data []byte,
	mode os.FileMode,
) error {
	root, err := filesystem.openRoot()
	if err != nil {
		return err
	}
	defer root.Close()
	temporary, temporaryPath, err := createTemporary(root, filepath.Dir(target))
	if err != nil {
		return err
	}
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = root.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(mode); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := root.Rename(temporaryPath, target); err != nil {
		return err
	}
	removeTemporary = false
	directory, err := root.Open(filepath.Dir(target))
	if err != nil {
		return err
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return err
	}
	written, err := root.ReadFile(target)
	if err != nil {
		return fmt.Errorf("verify write: %w", err)
	}
	if !bytes.Equal(written, data) {
		return errors.New("verify write: on-disk content differs")
	}
	return nil
}

func createTemporary(root *os.Root, directory string) (*os.File, string, error) {
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, "", fmt.Errorf("generate temporary file name: %w", err)
	}
	name := filepath.Join(
		directory,
		".agentm-file-"+hex.EncodeToString(random[:])+".tmp",
	)
	file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, "", err
	}
	return file, name, nil
}

func splitTextLines(content string) []string {
	if content == "" {
		return nil
	}
	lines := strings.Split(content, "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	for index := range lines {
		lines[index] = strings.TrimSuffix(lines[index], "\r")
	}
	return lines
}

func fileRevision(data []byte) string {
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum)
}

func isSHA256Revision(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9') &&
			!(character >= 'a' && character <= 'f') &&
			!(character >= 'A' && character <= 'F') {
			return false
		}
	}
	return true
}

func cleanDisplayPath(value string) string {
	return filepath.ToSlash(filepath.Clean(strings.TrimSpace(value)))
}

func toolFailure(err error) sdk.ToolResult {
	return sdk.ToolResult{Content: err.Error(), IsError: true}
}

func pathSchema(description string) map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"path": map[string]any{"type": "string", "description": description},
		},
		"required":             []string{"path"},
		"additionalProperties": false,
	}
}

func decodeArguments(raw json.RawMessage, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode tool arguments: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("tool arguments contain trailing JSON")
	}
	return nil
}
