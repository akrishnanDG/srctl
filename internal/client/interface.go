package client

// SchemaRegistryClientInterface defines the interface for Schema Registry operations
// This allows for easy mocking in tests
type SchemaRegistryClientInterface interface {
	// Subjects
	GetSubjects(includeDeleted bool) ([]string, error)
	GetVersions(subject string, includeDeleted bool) ([]int, error)
	
	// Schemas
	GetSchema(subject string, version string) (*Schema, error)
	GetSchemaWithDeleted(subject string, version string, includeDeleted bool) (*Schema, error)
	GetSchemaByID(id int) (*Schema, error)
	GetSchemaSubjectVersionsByID(id int) ([]SubjectVersion, error)
	GetSchemaReferencedBy(subject string, version int) ([]int, error)
	RegisterSchema(subject string, schema *Schema) (int, error)
	CheckCompatibility(subject string, schema *Schema, version string) (bool, error)
	GetAllSchemas(includeDeleted bool) ([]Schema, error)
	GetSchemaTypes() ([]string, error)
	
	// Subjects operations
	DeleteSubject(subject string, permanent bool) ([]int, error)
	DeleteVersion(subject string, version string, permanent bool) (int, error)
	
	// Config
	GetConfig() (*Config, error)
	GetSubjectConfig(subject string, defaultToGlobal bool) (*Config, error)
	SetConfig(compatibility string) error
	SetSubjectConfig(subject string, compatibility string) error
	
	// Mode
	GetMode() (*Mode, error)
	GetSubjectMode(subject string, defaultToGlobal bool) (*Mode, error)
	SetMode(mode string) error
	SetSubjectMode(subject string, mode string) error
	
	// Contexts
	GetContexts() ([]string, error)
	
	// Tags
	GetTags() ([]Tag, error)
	CreateTag(tag *Tag) error
	DeleteTag(name string) error
	GetSubjectTags(subject string) ([]TagAssignment, error)
	AssignTagToSubject(subject, tagName string) error
	RemoveTagFromSubject(subject, tagName string) error
	GetSchemaTags(subject string, version int) ([]TagAssignment, error)
	AssignTagToSchema(subject string, version int, tagName string) error
	RemoveTagFromSchema(subject string, version int, tagName string) error
}

// Ensure MockSchemaRegistryClient implements the interface
var _ SchemaRegistryClientInterface = (*MockSchemaRegistryClient)(nil)

// Ensure SchemaRegistryClient implements the interface
var _ SchemaRegistryClientInterface = (*SchemaRegistryClient)(nil)
