package result

import (
	"encoding/json"
	"fmt"
	"os"
)

const (
	// maxResultFileSize limits result file size to prevent memory exhaustion
	maxResultFileSize = 1 * 1024 * 1024 // 1MB
)

// Parser handles parsing adapter result files
type Parser struct{}

// NewParser creates a new result parser
func NewParser() *Parser {
	return &Parser{}
}

// ParseFile reads and parses a result file from the given path
func (p *Parser) ParseFile(path string) (*AdapterResult, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read result file path=%s: %w", path, err)
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("result file is empty: path=%s", path)
	}

	if len(data) > maxResultFileSize {
		return nil, fmt.Errorf("result file too large: path=%s size=%d max=%d", path, len(data), maxResultFileSize)
	}

	return p.Parse(data)
}

// Parse parses result data from JSON bytes
func (p *Parser) Parse(data []byte) (*AdapterResult, error) {
	var result AdapterResult

	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	if err := result.Validate(); err != nil {
		return nil, fmt.Errorf("invalid result format: %w", err)
	}

	return &result, nil
}
