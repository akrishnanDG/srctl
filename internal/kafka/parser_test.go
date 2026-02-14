package kafka

import (
	"testing"
)

func TestParseRecord_Schema(t *testing.T) {
	key := []byte(`{"keytype":"SCHEMA","subject":"user-events-value","version":1,"magic":1}`)
	value := []byte(`{"subject":"user-events-value","version":1,"id":100,"schema":"{\"type\":\"record\",\"name\":\"User\",\"fields\":[]}","schemaType":"AVRO","deleted":false}`)

	event, err := ParseRecord(key, value, 42, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if event == nil {
		t.Fatal("expected event, got nil")
	}
	if event.Type != KeyTypeSchema {
		t.Errorf("expected type SCHEMA, got %s", event.Type)
	}
	if event.Subject != "user-events-value" {
		t.Errorf("expected subject user-events-value, got %s", event.Subject)
	}
	if event.Version != 1 {
		t.Errorf("expected version 1, got %d", event.Version)
	}
	if event.SchemaID != 100 {
		t.Errorf("expected schema ID 100, got %d", event.SchemaID)
	}
	if event.SchemaType != "AVRO" {
		t.Errorf("expected schema type AVRO, got %s", event.SchemaType)
	}
	if event.Deleted {
		t.Error("expected deleted=false")
	}
	if event.Tombstone {
		t.Error("expected tombstone=false")
	}
	if event.Offset != 42 {
		t.Errorf("expected offset 42, got %d", event.Offset)
	}
}

func TestParseRecord_SchemaWithReferences(t *testing.T) {
	key := []byte(`{"keytype":"SCHEMA","subject":"order-events-value","version":2,"magic":1}`)
	value := []byte(`{"subject":"order-events-value","version":2,"id":200,"schema":"{}","schemaType":"AVRO","references":[{"name":"common.Address","subject":"address-value","version":1}],"deleted":false}`)

	event, err := ParseRecord(key, value, 10, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(event.References) != 1 {
		t.Fatalf("expected 1 reference, got %d", len(event.References))
	}
	ref := event.References[0]
	if ref.Name != "common.Address" {
		t.Errorf("expected ref name common.Address, got %s", ref.Name)
	}
	if ref.Subject != "address-value" {
		t.Errorf("expected ref subject address-value, got %s", ref.Subject)
	}
	if ref.Version != 1 {
		t.Errorf("expected ref version 1, got %d", ref.Version)
	}
}

func TestParseRecord_SchemaDeleted(t *testing.T) {
	key := []byte(`{"keytype":"SCHEMA","subject":"old-events-value","version":1,"magic":1}`)
	value := []byte(`{"subject":"old-events-value","version":1,"id":50,"schema":"{}","deleted":true}`)

	event, err := ParseRecord(key, value, 5, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !event.Deleted {
		t.Error("expected deleted=true")
	}
	if event.SchemaType != "AVRO" {
		t.Errorf("expected default AVRO type, got %s", event.SchemaType)
	}
}

func TestParseRecord_SchemaProtobuf(t *testing.T) {
	key := []byte(`{"keytype":"SCHEMA","subject":"proto-events-value","version":1,"magic":1}`)
	value := []byte(`{"subject":"proto-events-value","version":1,"id":300,"schema":"syntax = \"proto3\";","schemaType":"PROTOBUF","deleted":false}`)

	event, err := ParseRecord(key, value, 15, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if event.SchemaType != "PROTOBUF" {
		t.Errorf("expected PROTOBUF, got %s", event.SchemaType)
	}
}

func TestParseRecord_ConfigSubject(t *testing.T) {
	key := []byte(`{"keytype":"CONFIG","subject":"user-events-value"}`)
	value := []byte(`{"compatibilityLevel":"FULL_TRANSITIVE"}`)

	event, err := ParseRecord(key, value, 20, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if event.Type != KeyTypeConfig {
		t.Errorf("expected type CONFIG, got %s", event.Type)
	}
	if event.Subject != "user-events-value" {
		t.Errorf("expected subject user-events-value, got %s", event.Subject)
	}
	if event.Compatibility != "FULL_TRANSITIVE" {
		t.Errorf("expected FULL_TRANSITIVE, got %s", event.Compatibility)
	}
}

func TestParseRecord_ConfigGlobal(t *testing.T) {
	key := []byte(`{"keytype":"CONFIG","subject":""}`)
	value := []byte(`{"compatibilityLevel":"BACKWARD"}`)

	event, err := ParseRecord(key, value, 1, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if event.Type != KeyTypeConfig {
		t.Errorf("expected type CONFIG, got %s", event.Type)
	}
	if event.Subject != "" {
		t.Errorf("expected empty subject for global config, got %s", event.Subject)
	}
	if event.Compatibility != "BACKWARD" {
		t.Errorf("expected BACKWARD, got %s", event.Compatibility)
	}
}

func TestParseRecord_Mode(t *testing.T) {
	key := []byte(`{"keytype":"MODE","subject":"user-events-value"}`)
	value := []byte(`{"mode":"READONLY"}`)

	event, err := ParseRecord(key, value, 30, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if event.Type != KeyTypeMode {
		t.Errorf("expected type MODE, got %s", event.Type)
	}
	if event.Mode != "READONLY" {
		t.Errorf("expected READONLY, got %s", event.Mode)
	}
}

func TestParseRecord_DeleteSubject(t *testing.T) {
	key := []byte(`{"keytype":"DELETE_SUBJECT","subject":"old-events-value"}`)
	value := []byte(`{}`)

	event, err := ParseRecord(key, value, 40, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if event.Type != KeyTypeDeleteSubject {
		t.Errorf("expected type DELETE_SUBJECT, got %s", event.Type)
	}
	if event.Subject != "old-events-value" {
		t.Errorf("expected subject old-events-value, got %s", event.Subject)
	}
}

func TestParseRecord_Tombstone(t *testing.T) {
	key := []byte(`{"keytype":"SCHEMA","subject":"removed-value","version":1,"magic":1}`)

	event, err := ParseRecord(key, nil, 50, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !event.Tombstone {
		t.Error("expected tombstone=true for nil value")
	}
	if event.Type != KeyTypeSchema {
		t.Errorf("expected type SCHEMA, got %s", event.Type)
	}

	// Also test empty byte slice
	event2, err := ParseRecord(key, []byte{}, 51, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !event2.Tombstone {
		t.Error("expected tombstone=true for empty value")
	}
}

func TestParseRecord_Noop(t *testing.T) {
	key := []byte(`{"keytype":"NOOP"}`)
	value := []byte(`{}`)

	event, err := ParseRecord(key, value, 60, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if event != nil {
		t.Error("expected nil for NOOP event")
	}
}

func TestParseRecord_EmptyKey(t *testing.T) {
	event, err := ParseRecord(nil, []byte(`{}`), 0, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if event != nil {
		t.Error("expected nil for empty key")
	}

	event2, err := ParseRecord([]byte{}, []byte(`{}`), 0, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if event2 != nil {
		t.Error("expected nil for empty key")
	}
}

func TestParseRecord_InvalidKeyJSON(t *testing.T) {
	_, err := ParseRecord([]byte(`not json`), []byte(`{}`), 0, 0)
	if err == nil {
		t.Error("expected error for invalid key JSON")
	}
}

func TestParseRecord_InvalidSchemaValueJSON(t *testing.T) {
	key := []byte(`{"keytype":"SCHEMA","subject":"test","version":1,"magic":1}`)
	_, err := ParseRecord(key, []byte(`not json`), 0, 0)
	if err == nil {
		t.Error("expected error for invalid schema value JSON")
	}
}

func TestParseRecord_InvalidConfigValueJSON(t *testing.T) {
	key := []byte(`{"keytype":"CONFIG","subject":"test"}`)
	_, err := ParseRecord(key, []byte(`not json`), 0, 0)
	if err == nil {
		t.Error("expected error for invalid config value JSON")
	}
}

func TestParseRecord_InvalidModeValueJSON(t *testing.T) {
	key := []byte(`{"keytype":"MODE","subject":"test"}`)
	_, err := ParseRecord(key, []byte(`not json`), 0, 0)
	if err == nil {
		t.Error("expected error for invalid mode value JSON")
	}
}

func TestParseRecord_DefaultsToAvro(t *testing.T) {
	key := []byte(`{"keytype":"SCHEMA","subject":"test-value","version":1,"magic":1}`)
	value := []byte(`{"subject":"test-value","version":1,"id":1,"schema":"{}","deleted":false}`)

	event, err := ParseRecord(key, value, 0, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if event.SchemaType != "AVRO" {
		t.Errorf("expected default AVRO, got %s", event.SchemaType)
	}
}

func TestParseRecord_UnknownKeyType(t *testing.T) {
	key := []byte(`{"keytype":"UNKNOWN_TYPE","subject":"test"}`)
	value := []byte(`{}`)

	event, err := ParseRecord(key, value, 0, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if event != nil {
		t.Error("expected nil for unknown key type")
	}
}

func TestParseRecord_ClearSubject(t *testing.T) {
	key := []byte(`{"keytype":"CLEAR_SUBJECT","subject":"cleared-value"}`)
	value := []byte(`{}`)

	event, err := ParseRecord(key, value, 70, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if event.Type != KeyTypeClearSubject {
		t.Errorf("expected type CLEAR_SUBJECT, got %s", event.Type)
	}
	if event.Subject != "cleared-value" {
		t.Errorf("expected subject cleared-value, got %s", event.Subject)
	}
}
