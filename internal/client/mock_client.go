package client

import (
	"fmt"
	"sync"
)

// MockSchemaRegistryClient is a mock implementation for testing
type MockSchemaRegistryClient struct {
	mu sync.RWMutex

	// Storage
	Subjects       map[string][]Schema
	SubjectConfigs map[string]*Config
	SubjectModes   map[string]*Mode
	GlobalConfig   *Config
	GlobalMode     *Mode
	Contexts       []string
	Tags           []Tag
	TagAssignments map[string][]TagAssignment // key: "subject" or "subject:version"

	// Error simulation
	ShouldError       bool
	ErrorMessage      string
	GetSubjectsError  error
	GetVersionsError  error
	GetSchemaError    error
	DeleteError       error
	RegisterError     error
	ConfigError       error
	ModeError         error

	// Call tracking
	Calls []MockCall
}

// MockCall tracks method calls for verification
type MockCall struct {
	Method string
	Args   []interface{}
}

// NewMockClient creates a new mock client with default data
func NewMockClient() *MockSchemaRegistryClient {
	return &MockSchemaRegistryClient{
		Subjects:       make(map[string][]Schema),
		SubjectConfigs: make(map[string]*Config),
		SubjectModes:   make(map[string]*Mode),
		GlobalConfig:   &Config{CompatibilityLevel: "BACKWARD"},
		GlobalMode:     &Mode{Mode: "READWRITE"},
		Contexts:       []string{"."},
		Tags:           []Tag{},
		TagAssignments: make(map[string][]TagAssignment),
		Calls:          []MockCall{},
	}
}

// AddSubject adds a subject with schemas to the mock
func (m *MockSchemaRegistryClient) AddSubject(subject string, schemas []Schema) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Subjects[subject] = schemas
}

// RecordCall records a method call
func (m *MockSchemaRegistryClient) RecordCall(method string, args ...interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls = append(m.Calls, MockCall{Method: method, Args: args})
}

// GetCalls returns all recorded calls
func (m *MockSchemaRegistryClient) GetCalls() []MockCall {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Calls
}

// GetCallCount returns the number of calls to a specific method
func (m *MockSchemaRegistryClient) GetCallCount(method string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := 0
	for _, call := range m.Calls {
		if call.Method == method {
			count++
		}
	}
	return count
}

// --- Mock implementations of SchemaRegistryClient methods ---

func (m *MockSchemaRegistryClient) GetContexts() ([]string, error) {
	m.RecordCall("GetContexts")
	if m.ShouldError {
		return nil, fmt.Errorf(m.ErrorMessage)
	}
	return m.Contexts, nil
}

func (m *MockSchemaRegistryClient) GetSubjects(includeDeleted bool) ([]string, error) {
	m.RecordCall("GetSubjects", includeDeleted)
	if m.GetSubjectsError != nil {
		return nil, m.GetSubjectsError
	}
	if m.ShouldError {
		return nil, fmt.Errorf(m.ErrorMessage)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	subjects := make([]string, 0, len(m.Subjects))
	for subj := range m.Subjects {
		subjects = append(subjects, subj)
	}
	return subjects, nil
}

func (m *MockSchemaRegistryClient) GetVersions(subject string, includeDeleted bool) ([]int, error) {
	m.RecordCall("GetVersions", subject, includeDeleted)
	if m.GetVersionsError != nil {
		return nil, m.GetVersionsError
	}
	if m.ShouldError {
		return nil, fmt.Errorf(m.ErrorMessage)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	schemas, ok := m.Subjects[subject]
	if !ok {
		return nil, fmt.Errorf("subject not found: %s", subject)
	}

	versions := make([]int, len(schemas))
	for i, s := range schemas {
		versions[i] = s.Version
	}
	return versions, nil
}

func (m *MockSchemaRegistryClient) GetSchema(subject string, version string) (*Schema, error) {
	m.RecordCall("GetSchema", subject, version)
	if m.GetSchemaError != nil {
		return nil, m.GetSchemaError
	}
	if m.ShouldError {
		return nil, fmt.Errorf(m.ErrorMessage)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	schemas, ok := m.Subjects[subject]
	if !ok {
		return nil, fmt.Errorf("subject not found: %s", subject)
	}

	if version == "latest" {
		if len(schemas) == 0 {
			return nil, fmt.Errorf("no versions found")
		}
		s := schemas[len(schemas)-1]
		return &s, nil
	}

	var ver int
	fmt.Sscanf(version, "%d", &ver)
	for _, s := range schemas {
		if s.Version == ver {
			return &s, nil
		}
	}

	return nil, fmt.Errorf("version not found: %s", version)
}

func (m *MockSchemaRegistryClient) GetSchemaWithDeleted(subject string, version string, includeDeleted bool) (*Schema, error) {
	m.RecordCall("GetSchemaWithDeleted", subject, version, includeDeleted)
	return m.GetSchema(subject, version)
}

func (m *MockSchemaRegistryClient) GetSchemaByID(id int) (*Schema, error) {
	m.RecordCall("GetSchemaByID", id)
	if m.ShouldError {
		return nil, fmt.Errorf(m.ErrorMessage)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, schemas := range m.Subjects {
		for _, s := range schemas {
			if s.ID == id {
				return &s, nil
			}
		}
	}

	return nil, fmt.Errorf("schema ID not found: %d", id)
}

func (m *MockSchemaRegistryClient) GetSchemaReferencedBy(subject string, version int) ([]int, error) {
	m.RecordCall("GetSchemaReferencedBy", subject, version)
	if m.ShouldError {
		return nil, fmt.Errorf(m.ErrorMessage)
	}
	// By default, return empty (no references)
	return []int{}, nil
}

func (m *MockSchemaRegistryClient) GetSchemaSubjectVersionsByID(id int) ([]SubjectVersion, error) {
	m.RecordCall("GetSchemaSubjectVersionsByID", id)
	if m.ShouldError {
		return nil, fmt.Errorf(m.ErrorMessage)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var results []SubjectVersion
	for subj, schemas := range m.Subjects {
		for _, s := range schemas {
			if s.ID == id {
				results = append(results, SubjectVersion{Subject: subj, Version: s.Version})
			}
		}
	}
	return results, nil
}

func (m *MockSchemaRegistryClient) RegisterSchema(subject string, schema *Schema) (int, error) {
	m.RecordCall("RegisterSchema", subject, schema)
	if m.RegisterError != nil {
		return 0, m.RegisterError
	}
	if m.ShouldError {
		return 0, fmt.Errorf(m.ErrorMessage)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Generate new ID
	maxID := 0
	for _, schemas := range m.Subjects {
		for _, s := range schemas {
			if s.ID > maxID {
				maxID = s.ID
			}
		}
	}
	newID := maxID + 1

	// Generate new version
	existingSchemas := m.Subjects[subject]
	newVersion := len(existingSchemas) + 1

	newSchema := Schema{
		Subject:    subject,
		Version:    newVersion,
		ID:         newID,
		SchemaType: schema.SchemaType,
		Schema:     schema.Schema,
		References: schema.References,
	}

	m.Subjects[subject] = append(existingSchemas, newSchema)
	return newID, nil
}

func (m *MockSchemaRegistryClient) DeleteSubject(subject string, permanent bool) ([]int, error) {
	m.RecordCall("DeleteSubject", subject, permanent)
	if m.DeleteError != nil {
		return nil, m.DeleteError
	}
	if m.ShouldError {
		return nil, fmt.Errorf(m.ErrorMessage)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	schemas, ok := m.Subjects[subject]
	if !ok {
		return nil, fmt.Errorf("subject not found: %s", subject)
	}

	versions := make([]int, len(schemas))
	for i, s := range schemas {
		versions[i] = s.Version
	}

	if permanent {
		delete(m.Subjects, subject)
	}

	return versions, nil
}

func (m *MockSchemaRegistryClient) DeleteVersion(subject string, version string, permanent bool) (int, error) {
	m.RecordCall("DeleteVersion", subject, version, permanent)
	if m.DeleteError != nil {
		return 0, m.DeleteError
	}
	if m.ShouldError {
		return 0, fmt.Errorf(m.ErrorMessage)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	schemas, ok := m.Subjects[subject]
	if !ok {
		return 0, fmt.Errorf("subject not found: %s", subject)
	}

	var ver int
	fmt.Sscanf(version, "%d", &ver)

	for i, s := range schemas {
		if s.Version == ver {
			if permanent {
				m.Subjects[subject] = append(schemas[:i], schemas[i+1:]...)
			}
			return ver, nil
		}
	}

	return 0, fmt.Errorf("version not found: %s", version)
}

func (m *MockSchemaRegistryClient) GetConfig() (*Config, error) {
	m.RecordCall("GetConfig")
	if m.ConfigError != nil {
		return nil, m.ConfigError
	}
	if m.ShouldError {
		return nil, fmt.Errorf(m.ErrorMessage)
	}
	return m.GlobalConfig, nil
}

func (m *MockSchemaRegistryClient) GetSubjectConfig(subject string, defaultToGlobal bool) (*Config, error) {
	m.RecordCall("GetSubjectConfig", subject, defaultToGlobal)
	if m.ConfigError != nil {
		return nil, m.ConfigError
	}
	if m.ShouldError {
		return nil, fmt.Errorf(m.ErrorMessage)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if config, ok := m.SubjectConfigs[subject]; ok {
		return config, nil
	}
	if defaultToGlobal {
		return m.GlobalConfig, nil
	}
	return nil, nil
}

func (m *MockSchemaRegistryClient) SetConfig(compatibility string) error {
	m.RecordCall("SetConfig", compatibility)
	if m.ConfigError != nil {
		return m.ConfigError
	}
	if m.ShouldError {
		return fmt.Errorf(m.ErrorMessage)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.GlobalConfig = &Config{CompatibilityLevel: compatibility}
	return nil
}

func (m *MockSchemaRegistryClient) SetSubjectConfig(subject string, compatibility string) error {
	m.RecordCall("SetSubjectConfig", subject, compatibility)
	if m.ConfigError != nil {
		return m.ConfigError
	}
	if m.ShouldError {
		return fmt.Errorf(m.ErrorMessage)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.SubjectConfigs[subject] = &Config{CompatibilityLevel: compatibility}
	return nil
}

func (m *MockSchemaRegistryClient) GetMode() (*Mode, error) {
	m.RecordCall("GetMode")
	if m.ModeError != nil {
		return nil, m.ModeError
	}
	if m.ShouldError {
		return nil, fmt.Errorf(m.ErrorMessage)
	}
	return m.GlobalMode, nil
}

func (m *MockSchemaRegistryClient) GetSubjectMode(subject string, defaultToGlobal bool) (*Mode, error) {
	m.RecordCall("GetSubjectMode", subject, defaultToGlobal)
	if m.ModeError != nil {
		return nil, m.ModeError
	}
	if m.ShouldError {
		return nil, fmt.Errorf(m.ErrorMessage)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if mode, ok := m.SubjectModes[subject]; ok {
		return mode, nil
	}
	if defaultToGlobal {
		return m.GlobalMode, nil
	}
	return nil, nil
}

func (m *MockSchemaRegistryClient) SetMode(mode string) error {
	m.RecordCall("SetMode", mode)
	if m.ModeError != nil {
		return m.ModeError
	}
	if m.ShouldError {
		return fmt.Errorf(m.ErrorMessage)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.GlobalMode = &Mode{Mode: mode}
	return nil
}

func (m *MockSchemaRegistryClient) SetSubjectMode(subject string, mode string) error {
	m.RecordCall("SetSubjectMode", subject, mode)
	if m.ModeError != nil {
		return m.ModeError
	}
	if m.ShouldError {
		return fmt.Errorf(m.ErrorMessage)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.SubjectModes[subject] = &Mode{Mode: mode}
	return nil
}

func (m *MockSchemaRegistryClient) CheckCompatibility(subject string, schema *Schema, version string) (bool, error) {
	m.RecordCall("CheckCompatibility", subject, schema, version)
	if m.ShouldError {
		return false, fmt.Errorf(m.ErrorMessage)
	}
	return true, nil
}

func (m *MockSchemaRegistryClient) GetAllSchemas(includeDeleted bool) ([]Schema, error) {
	m.RecordCall("GetAllSchemas", includeDeleted)
	if m.ShouldError {
		return nil, fmt.Errorf(m.ErrorMessage)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var all []Schema
	for _, schemas := range m.Subjects {
		all = append(all, schemas...)
	}
	return all, nil
}

func (m *MockSchemaRegistryClient) GetSchemaTypes() ([]string, error) {
	m.RecordCall("GetSchemaTypes")
	if m.ShouldError {
		return nil, fmt.Errorf(m.ErrorMessage)
	}
	return []string{"AVRO", "PROTOBUF", "JSON"}, nil
}

// Tag methods
func (m *MockSchemaRegistryClient) GetTags() ([]Tag, error) {
	m.RecordCall("GetTags")
	if m.ShouldError {
		return nil, fmt.Errorf(m.ErrorMessage)
	}
	return m.Tags, nil
}

func (m *MockSchemaRegistryClient) CreateTag(tag *Tag) error {
	m.RecordCall("CreateTag", tag)
	if m.ShouldError {
		return fmt.Errorf(m.ErrorMessage)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Tags = append(m.Tags, *tag)
	return nil
}

func (m *MockSchemaRegistryClient) DeleteTag(tagName string) error {
	m.RecordCall("DeleteTag", tagName)
	if m.ShouldError {
		return fmt.Errorf(m.ErrorMessage)
	}
	return nil
}

func (m *MockSchemaRegistryClient) GetSubjectTags(subject string) ([]TagAssignment, error) {
	m.RecordCall("GetSubjectTags", subject)
	if m.ShouldError {
		return nil, fmt.Errorf(m.ErrorMessage)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if tags, ok := m.TagAssignments[subject]; ok {
		return tags, nil
	}
	return []TagAssignment{}, nil
}

func (m *MockSchemaRegistryClient) AssignTagToSubject(subject, tagName string) error {
	m.RecordCall("AssignTagToSubject", subject, tagName)
	if m.ShouldError {
		return fmt.Errorf(m.ErrorMessage)
	}
	return nil
}

func (m *MockSchemaRegistryClient) RemoveTagFromSubject(subject, tagName string) error {
	m.RecordCall("RemoveTagFromSubject", subject, tagName)
	if m.ShouldError {
		return fmt.Errorf(m.ErrorMessage)
	}
	return nil
}

func (m *MockSchemaRegistryClient) GetSchemaTags(subject string, version int) ([]TagAssignment, error) {
	m.RecordCall("GetSchemaTags", subject, version)
	if m.ShouldError {
		return nil, fmt.Errorf(m.ErrorMessage)
	}

	key := fmt.Sprintf("%s:%d", subject, version)
	m.mu.RLock()
	defer m.mu.RUnlock()

	if tags, ok := m.TagAssignments[key]; ok {
		return tags, nil
	}
	return []TagAssignment{}, nil
}

func (m *MockSchemaRegistryClient) AssignTagToSchema(subject string, version int, tagName string) error {
	m.RecordCall("AssignTagToSchema", subject, version, tagName)
	if m.ShouldError {
		return fmt.Errorf(m.ErrorMessage)
	}
	return nil
}

func (m *MockSchemaRegistryClient) RemoveTagFromSchema(subject string, version int, tagName string) error {
	m.RecordCall("RemoveTagFromSchema", subject, version, tagName)
	if m.ShouldError {
		return fmt.Errorf(m.ErrorMessage)
	}
	return nil
}

// WithContext returns a copy of the mock client (for compatibility)
func (m *MockSchemaRegistryClient) WithContext(ctx string) *MockSchemaRegistryClient {
	return m
}

// NewError creates a new error for testing
func NewError(msg string) error {
	return fmt.Errorf(msg)
}

// ErrSubjectNotFound is a common error for testing
var ErrSubjectNotFound = fmt.Errorf("subject not found")
