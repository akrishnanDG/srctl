package kafka

import (
	"encoding/json"
	"fmt"
)

// KeyType represents the type of _schemas topic message.
type KeyType string

const (
	KeyTypeSchema        KeyType = "SCHEMA"
	KeyTypeConfig        KeyType = "CONFIG"
	KeyTypeMode          KeyType = "MODE"
	KeyTypeDeleteSubject KeyType = "DELETE_SUBJECT"
	KeyTypeClearSubject  KeyType = "CLEAR_SUBJECT"
	KeyTypeNoop          KeyType = "NOOP"
)

// schemaKey represents the key of a _schemas topic message.
type schemaKey struct {
	KeyType KeyType `json:"keytype"`
	Subject string  `json:"subject,omitempty"`
	Version int     `json:"version,omitempty"`
	Magic   int     `json:"magic,omitempty"`
}

// schemaValue represents the value for a SCHEMA message.
type schemaValue struct {
	Subject    string           `json:"subject"`
	Version    int              `json:"version"`
	ID         int              `json:"id"`
	Schema     string           `json:"schema"`
	SchemaType string           `json:"schemaType,omitempty"`
	References []ReferenceValue `json:"references,omitempty"`
	Deleted    bool             `json:"deleted"`
}

// ReferenceValue represents a schema reference in _schemas topic format.
type ReferenceValue struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

// configValue represents the value for a CONFIG message.
type configValue struct {
	CompatibilityLevel string `json:"compatibilityLevel"`
}

// modeValue represents the value for a MODE message.
type modeValue struct {
	Mode string `json:"mode"`
}

// SchemaEvent is the parsed, unified event from the _schemas topic.
type SchemaEvent struct {
	Type      KeyType
	Subject   string
	Version   int
	Offset    int64
	Partition int32
	Tombstone bool // null value = permanent deletion

	// For SCHEMA events
	SchemaID   int
	Schema     string
	SchemaType string
	References []ReferenceValue
	Deleted    bool // soft delete (deleted=true in value)

	// For CONFIG events
	Compatibility string

	// For MODE events
	Mode string
}

// ParseRecord parses a raw Kafka record into a SchemaEvent.
// Returns nil for NOOP events or empty keys.
func ParseRecord(key, value []byte, offset int64, partition int32) (*SchemaEvent, error) {
	if len(key) == 0 {
		return nil, nil
	}

	var k schemaKey
	if err := json.Unmarshal(key, &k); err != nil {
		return nil, fmt.Errorf("failed to parse _schemas key: %w", err)
	}

	if k.KeyType == KeyTypeNoop {
		return nil, nil
	}

	event := &SchemaEvent{
		Type:      k.KeyType,
		Subject:   k.Subject,
		Version:   k.Version,
		Offset:    offset,
		Partition: partition,
	}

	// Tombstone = null/empty value = permanent deletion
	if len(value) == 0 {
		event.Tombstone = true
		return event, nil
	}

	switch k.KeyType {
	case KeyTypeSchema:
		var v schemaValue
		if err := json.Unmarshal(value, &v); err != nil {
			return nil, fmt.Errorf("failed to parse SCHEMA value: %w", err)
		}
		event.SchemaID = v.ID
		event.Schema = v.Schema
		event.SchemaType = v.SchemaType
		event.References = v.References
		event.Deleted = v.Deleted
		if event.SchemaType == "" {
			event.SchemaType = "AVRO"
		}

	case KeyTypeConfig:
		var v configValue
		if err := json.Unmarshal(value, &v); err != nil {
			return nil, fmt.Errorf("failed to parse CONFIG value: %w", err)
		}
		event.Compatibility = v.CompatibilityLevel

	case KeyTypeMode:
		var v modeValue
		if err := json.Unmarshal(value, &v); err != nil {
			return nil, fmt.Errorf("failed to parse MODE value: %w", err)
		}
		event.Mode = v.Mode

	case KeyTypeDeleteSubject, KeyTypeClearSubject:
		// Key contains the subject; value may have metadata but we only need the subject.

	default:
		// Unknown key type, skip
		return nil, nil
	}

	return event, nil
}
