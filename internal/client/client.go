package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// SchemaRegistryClient is the main client for interacting with Schema Registry
type SchemaRegistryClient struct {
	BaseURL    string
	HTTPClient *http.Client
	Auth       *AuthConfig
	Context    string // Default context (empty for default context ".")
}

// AuthConfig holds authentication configuration
type AuthConfig struct {
	Username string
	Password string
	// Add more auth methods as needed (OAuth, mTLS, etc.)
}

// Schema represents a schema in the registry
type Schema struct {
	Subject    string            `json:"subject,omitempty"`
	Version    int               `json:"version,omitempty"`
	ID         int               `json:"id,omitempty"`
	SchemaType string            `json:"schemaType,omitempty"`
	Schema     string            `json:"schema"`
	References []SchemaReference `json:"references,omitempty"`
	Deleted    bool              `json:"deleted,omitempty"`
}

// SchemaReference represents a reference to another schema
type SchemaReference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

// SubjectVersion represents a subject with its version info
type SubjectVersion struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

// Config represents compatibility configuration
type Config struct {
	CompatibilityLevel string `json:"compatibilityLevel,omitempty"`
	Compatibility      string `json:"compatibility,omitempty"`
}

// Mode represents the mode of a subject or the registry
type Mode struct {
	Mode string `json:"mode"`
}

// ServerInfo represents schema registry server information
type ServerInfo struct {
	Version string `json:"version,omitempty"`
}

// NewClient creates a new Schema Registry client
func NewClient(baseURL string, auth *AuthConfig) *SchemaRegistryClient {
	return &SchemaRegistryClient{
		BaseURL: strings.TrimRight(baseURL, "/"),
		HTTPClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		Auth:    auth,
		Context: "",
	}
}

// WithContext returns a copy of the client with a specific context
func (c *SchemaRegistryClient) WithContext(ctx string) *SchemaRegistryClient {
	newClient := *c
	newClient.Context = ctx
	return &newClient
}

// buildURL constructs the URL with optional context prefix
func (c *SchemaRegistryClient) buildURL(path string) string {
	if c.Context != "" && c.Context != "." {
		// Context-aware URL: /contexts/{context}/...
		return fmt.Sprintf("%s/contexts/%s%s", c.BaseURL, url.PathEscape(c.Context), path)
	}
	return c.BaseURL + path
}

// doRequest performs an HTTP request with authentication
func (c *SchemaRegistryClient) doRequest(method, urlPath string, body interface{}) ([]byte, int, error) {
	var reqBody io.Reader
	if body != nil {
		jsonBytes, err := json.Marshal(body)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to marshal request body: %w", err)
		}
		reqBody = bytes.NewReader(jsonBytes)
	}

	req, err := http.NewRequest(method, urlPath, reqBody)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json")
	req.Header.Set("Confluent-Accept-Unknown-Properties", "true")

	if c.Auth != nil && c.Auth.Username != "" {
		req.SetBasicAuth(c.Auth.Username, c.Auth.Password)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("failed to read response body: %w", err)
	}

	return respBody, resp.StatusCode, nil
}

// GetContexts returns all contexts in the registry
func (c *SchemaRegistryClient) GetContexts() ([]string, error) {
	respBody, statusCode, err := c.doRequest("GET", c.BaseURL+"/contexts", nil)
	if err != nil {
		return nil, err
	}

	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get contexts: %s (status %d)", string(respBody), statusCode)
	}

	var contexts []string
	if err := json.Unmarshal(respBody, &contexts); err != nil {
		return nil, fmt.Errorf("failed to parse contexts response: %w", err)
	}

	return contexts, nil
}

// GetSubjects returns all subjects, optionally filtered by prefix and including deleted
func (c *SchemaRegistryClient) GetSubjects(includeDeleted bool) ([]string, error) {
	urlPath := c.buildURL("/subjects")
	if includeDeleted {
		urlPath += "?deleted=true"
	}

	respBody, statusCode, err := c.doRequest("GET", urlPath, nil)
	if err != nil {
		return nil, err
	}

	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get subjects: %s (status %d)", string(respBody), statusCode)
	}

	var subjects []string
	if err := json.Unmarshal(respBody, &subjects); err != nil {
		return nil, fmt.Errorf("failed to parse subjects response: %w", err)
	}

	return subjects, nil
}

// GetVersions returns all versions for a subject
func (c *SchemaRegistryClient) GetVersions(subject string, includeDeleted bool) ([]int, error) {
	urlPath := c.buildURL(fmt.Sprintf("/subjects/%s/versions", url.PathEscape(subject)))
	if includeDeleted {
		urlPath += "?deleted=true"
	}

	respBody, statusCode, err := c.doRequest("GET", urlPath, nil)
	if err != nil {
		return nil, err
	}

	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get versions: %s (status %d)", string(respBody), statusCode)
	}

	var versions []int
	if err := json.Unmarshal(respBody, &versions); err != nil {
		return nil, fmt.Errorf("failed to parse versions response: %w", err)
	}

	return versions, nil
}

// GetSchema returns a schema for a subject at a specific version
func (c *SchemaRegistryClient) GetSchema(subject string, version string) (*Schema, error) {
	return c.GetSchemaWithDeleted(subject, version, false)
}

// GetSchemaWithDeleted returns a schema, optionally including deleted schemas
func (c *SchemaRegistryClient) GetSchemaWithDeleted(subject string, version string, includeDeleted bool) (*Schema, error) {
	urlPath := c.buildURL(fmt.Sprintf("/subjects/%s/versions/%s", url.PathEscape(subject), version))
	if includeDeleted {
		urlPath += "?deleted=true"
	}

	respBody, statusCode, err := c.doRequest("GET", urlPath, nil)
	if err != nil {
		return nil, err
	}

	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get schema: %s (status %d)", string(respBody), statusCode)
	}

	var schema Schema
	if err := json.Unmarshal(respBody, &schema); err != nil {
		return nil, fmt.Errorf("failed to parse schema response: %w", err)
	}

	return &schema, nil
}

// GetSchemaByID returns a schema by its global ID
func (c *SchemaRegistryClient) GetSchemaByID(id int) (*Schema, error) {
	urlPath := fmt.Sprintf("%s/schemas/ids/%d", c.BaseURL, id)

	respBody, statusCode, err := c.doRequest("GET", urlPath, nil)
	if err != nil {
		return nil, err
	}

	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get schema by ID: %s (status %d)", string(respBody), statusCode)
	}

	var schema Schema
	if err := json.Unmarshal(respBody, &schema); err != nil {
		return nil, fmt.Errorf("failed to parse schema response: %w", err)
	}
	schema.ID = id

	return &schema, nil
}

// GetSchemaReferencedBy returns schema IDs that reference the given subject/version
func (c *SchemaRegistryClient) GetSchemaReferencedBy(subject string, version int) ([]int, error) {
	urlPath := c.buildURL(fmt.Sprintf("/subjects/%s/versions/%d/referencedby", url.PathEscape(subject), version))

	respBody, statusCode, err := c.doRequest("GET", urlPath, nil)
	if err != nil {
		return nil, err
	}

	if statusCode == http.StatusNotFound {
		return []int{}, nil
	}

	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get referencedby: %s (status %d)", string(respBody), statusCode)
	}

	var schemaIDs []int
	if err := json.Unmarshal(respBody, &schemaIDs); err != nil {
		return nil, fmt.Errorf("failed to parse referencedby response: %w", err)
	}

	return schemaIDs, nil
}

// GetSchemaSubjectVersionsByID returns all subjects/versions that use a schema ID
func (c *SchemaRegistryClient) GetSchemaSubjectVersionsByID(id int) ([]SubjectVersion, error) {
	urlPath := fmt.Sprintf("%s/schemas/ids/%d/versions", c.BaseURL, id)

	respBody, statusCode, err := c.doRequest("GET", urlPath, nil)
	if err != nil {
		return nil, err
	}

	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get schema versions by ID: %s (status %d)", string(respBody), statusCode)
	}

	var subjectVersions []SubjectVersion
	if err := json.Unmarshal(respBody, &subjectVersions); err != nil {
		return nil, fmt.Errorf("failed to parse subject versions response: %w", err)
	}

	return subjectVersions, nil
}

// RegisterSchema registers a new schema under a subject
func (c *SchemaRegistryClient) RegisterSchema(subject string, schema *Schema) (int, error) {
	urlPath := c.buildURL(fmt.Sprintf("/subjects/%s/versions", url.PathEscape(subject)))

	reqBody := map[string]interface{}{
		"schema": schema.Schema,
	}
	if schema.SchemaType != "" && schema.SchemaType != "AVRO" {
		reqBody["schemaType"] = schema.SchemaType
	}
	if len(schema.References) > 0 {
		reqBody["references"] = schema.References
	}
	// Include ID for IMPORT mode (requires registry to be in IMPORT mode)
	if schema.ID > 0 {
		reqBody["id"] = schema.ID
	}

	respBody, statusCode, err := c.doRequest("POST", urlPath, reqBody)
	if err != nil {
		return 0, err
	}

	if statusCode != http.StatusOK {
		return 0, fmt.Errorf("failed to register schema: %s (status %d)", string(respBody), statusCode)
	}

	var result struct {
		ID int `json:"id"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return 0, fmt.Errorf("failed to parse register response: %w", err)
	}

	return result.ID, nil
}

// DeleteSubject deletes a subject (soft delete by default)
func (c *SchemaRegistryClient) DeleteSubject(subject string, permanent bool) ([]int, error) {
	urlPath := c.buildURL(fmt.Sprintf("/subjects/%s", url.PathEscape(subject)))
	if permanent {
		urlPath += "?permanent=true"
	}

	respBody, statusCode, err := c.doRequest("DELETE", urlPath, nil)
	if err != nil {
		return nil, err
	}

	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to delete subject: %s (status %d)", string(respBody), statusCode)
	}

	var versions []int
	if err := json.Unmarshal(respBody, &versions); err != nil {
		return nil, fmt.Errorf("failed to parse delete response: %w", err)
	}

	return versions, nil
}

// DeleteVersion deletes a specific version (soft delete by default)
func (c *SchemaRegistryClient) DeleteVersion(subject string, version string, permanent bool) (int, error) {
	urlPath := c.buildURL(fmt.Sprintf("/subjects/%s/versions/%s", url.PathEscape(subject), version))
	if permanent {
		urlPath += "?permanent=true"
	}

	respBody, statusCode, err := c.doRequest("DELETE", urlPath, nil)
	if err != nil {
		return 0, err
	}

	if statusCode != http.StatusOK {
		return 0, fmt.Errorf("failed to delete version: %s (status %d)", string(respBody), statusCode)
	}

	var deletedVersion int
	if err := json.Unmarshal(respBody, &deletedVersion); err != nil {
		return 0, fmt.Errorf("failed to parse delete response: %w", err)
	}

	return deletedVersion, nil
}

// GetConfig returns the global compatibility configuration
func (c *SchemaRegistryClient) GetConfig() (*Config, error) {
	urlPath := c.buildURL("/config")

	respBody, statusCode, err := c.doRequest("GET", urlPath, nil)
	if err != nil {
		return nil, err
	}

	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get config: %s (status %d)", string(respBody), statusCode)
	}

	var config Config
	if err := json.Unmarshal(respBody, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config response: %w", err)
	}

	return &config, nil
}

// GetSubjectConfig returns the compatibility configuration for a subject
func (c *SchemaRegistryClient) GetSubjectConfig(subject string, defaultToGlobal bool) (*Config, error) {
	urlPath := c.buildURL(fmt.Sprintf("/config/%s", url.PathEscape(subject)))
	if defaultToGlobal {
		urlPath += "?defaultToGlobal=true"
	}

	respBody, statusCode, err := c.doRequest("GET", urlPath, nil)
	if err != nil {
		return nil, err
	}

	if statusCode == http.StatusNotFound {
		// No specific config, return nil to indicate using global
		return nil, nil
	}

	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get subject config: %s (status %d)", string(respBody), statusCode)
	}

	var config Config
	if err := json.Unmarshal(respBody, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config response: %w", err)
	}

	return &config, nil
}

// SetConfig sets the global compatibility configuration
func (c *SchemaRegistryClient) SetConfig(compatibility string) error {
	urlPath := c.buildURL("/config")
	body := map[string]string{"compatibility": compatibility}

	respBody, statusCode, err := c.doRequest("PUT", urlPath, body)
	if err != nil {
		return err
	}

	if statusCode != http.StatusOK {
		return fmt.Errorf("failed to set config: %s (status %d)", string(respBody), statusCode)
	}

	return nil
}

// SetSubjectConfig sets the compatibility configuration for a subject
func (c *SchemaRegistryClient) SetSubjectConfig(subject string, compatibility string) error {
	urlPath := c.buildURL(fmt.Sprintf("/config/%s", url.PathEscape(subject)))
	body := map[string]string{"compatibility": compatibility}

	respBody, statusCode, err := c.doRequest("PUT", urlPath, body)
	if err != nil {
		return err
	}

	if statusCode != http.StatusOK {
		return fmt.Errorf("failed to set subject config: %s (status %d)", string(respBody), statusCode)
	}

	return nil
}

// GetMode returns the global mode
func (c *SchemaRegistryClient) GetMode() (*Mode, error) {
	urlPath := c.buildURL("/mode")

	respBody, statusCode, err := c.doRequest("GET", urlPath, nil)
	if err != nil {
		return nil, err
	}

	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get mode: %s (status %d)", string(respBody), statusCode)
	}

	var mode Mode
	if err := json.Unmarshal(respBody, &mode); err != nil {
		return nil, fmt.Errorf("failed to parse mode response: %w", err)
	}

	return &mode, nil
}

// GetSubjectMode returns the mode for a subject
func (c *SchemaRegistryClient) GetSubjectMode(subject string, defaultToGlobal bool) (*Mode, error) {
	urlPath := c.buildURL(fmt.Sprintf("/mode/%s", url.PathEscape(subject)))
	if defaultToGlobal {
		urlPath += "?defaultToGlobal=true"
	}

	respBody, statusCode, err := c.doRequest("GET", urlPath, nil)
	if err != nil {
		return nil, err
	}

	if statusCode == http.StatusNotFound {
		return nil, nil
	}

	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get subject mode: %s (status %d)", string(respBody), statusCode)
	}

	var mode Mode
	if err := json.Unmarshal(respBody, &mode); err != nil {
		return nil, fmt.Errorf("failed to parse mode response: %w", err)
	}

	return &mode, nil
}

// SetMode sets the global mode
func (c *SchemaRegistryClient) SetMode(mode string) error {
	urlPath := c.buildURL("/mode")
	body := map[string]string{"mode": mode}

	respBody, statusCode, err := c.doRequest("PUT", urlPath, body)
	if err != nil {
		return err
	}

	if statusCode != http.StatusOK {
		return fmt.Errorf("failed to set mode: %s (status %d)", string(respBody), statusCode)
	}

	return nil
}

// SetSubjectMode sets the mode for a subject
func (c *SchemaRegistryClient) SetSubjectMode(subject string, mode string) error {
	urlPath := c.buildURL(fmt.Sprintf("/mode/%s", url.PathEscape(subject)))
	body := map[string]string{"mode": mode}

	respBody, statusCode, err := c.doRequest("PUT", urlPath, body)
	if err != nil {
		return err
	}

	if statusCode != http.StatusOK {
		return fmt.Errorf("failed to set subject mode: %s (status %d)", string(respBody), statusCode)
	}

	return nil
}

// CheckCompatibility checks if a schema is compatible with the latest version
func (c *SchemaRegistryClient) CheckCompatibility(subject string, schema *Schema, version string) (bool, error) {
	urlPath := c.buildURL(fmt.Sprintf("/compatibility/subjects/%s/versions/%s", url.PathEscape(subject), version))

	reqBody := map[string]interface{}{
		"schema": schema.Schema,
	}
	if schema.SchemaType != "" && schema.SchemaType != "AVRO" {
		reqBody["schemaType"] = schema.SchemaType
	}
	if len(schema.References) > 0 {
		reqBody["references"] = schema.References
	}

	respBody, statusCode, err := c.doRequest("POST", urlPath, reqBody)
	if err != nil {
		return false, err
	}

	if statusCode != http.StatusOK {
		return false, fmt.Errorf("failed to check compatibility: %s (status %d)", string(respBody), statusCode)
	}

	var result struct {
		IsCompatible bool `json:"is_compatible"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return false, fmt.Errorf("failed to parse compatibility response: %w", err)
	}

	return result.IsCompatible, nil
}

// GetAllSchemas returns all schemas in the registry (for stats)
func (c *SchemaRegistryClient) GetAllSchemas(includeDeleted bool) ([]Schema, error) {
	urlPath := c.BaseURL + "/schemas"
	if includeDeleted {
		urlPath += "?deleted=true"
	}

	respBody, statusCode, err := c.doRequest("GET", urlPath, nil)
	if err != nil {
		return nil, err
	}

	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get all schemas: %s (status %d)", string(respBody), statusCode)
	}

	var schemas []Schema
	if err := json.Unmarshal(respBody, &schemas); err != nil {
		return nil, fmt.Errorf("failed to parse schemas response: %w", err)
	}

	return schemas, nil
}

// GetSchemaTypes returns all registered schema types
func (c *SchemaRegistryClient) GetSchemaTypes() ([]string, error) {
	urlPath := c.BaseURL + "/schemas/types"

	respBody, statusCode, err := c.doRequest("GET", urlPath, nil)
	if err != nil {
		return nil, err
	}

	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get schema types: %s (status %d)", string(respBody), statusCode)
	}

	var types []string
	if err := json.Unmarshal(respBody, &types); err != nil {
		return nil, fmt.Errorf("failed to parse schema types response: %w", err)
	}

	return types, nil
}

// Tag represents a tag definition
type Tag struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// TagAssignment represents a tag assignment to a schema or subject
type TagAssignment struct {
	TypeName   string `json:"typeName"`
	EntityType string `json:"entityType"`
	EntityName string `json:"entityName"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// GetTags returns all tag definitions
func (c *SchemaRegistryClient) GetTags() ([]Tag, error) {
	urlPath := c.BaseURL + "/catalog/v1/types/tagdefs"

	respBody, statusCode, err := c.doRequest("GET", urlPath, nil)
	if err != nil {
		return nil, err
	}

	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get tags: %s (status %d)", string(respBody), statusCode)
	}

	var tags []Tag
	if err := json.Unmarshal(respBody, &tags); err != nil {
		return nil, fmt.Errorf("failed to parse tags response: %w", err)
	}

	return tags, nil
}

// CreateTag creates a new tag definition
func (c *SchemaRegistryClient) CreateTag(tag *Tag) error {
	urlPath := c.BaseURL + "/catalog/v1/types/tagdefs"

	respBody, statusCode, err := c.doRequest("POST", urlPath, []Tag{*tag})
	if err != nil {
		return err
	}

	if statusCode != http.StatusOK && statusCode != http.StatusCreated {
		return fmt.Errorf("failed to create tag: %s (status %d)", string(respBody), statusCode)
	}

	return nil
}

// DeleteTag deletes a tag definition
func (c *SchemaRegistryClient) DeleteTag(tagName string) error {
	urlPath := fmt.Sprintf("%s/catalog/v1/types/tagdefs/%s", c.BaseURL, url.PathEscape(tagName))

	respBody, statusCode, err := c.doRequest("DELETE", urlPath, nil)
	if err != nil {
		return err
	}

	if statusCode != http.StatusOK && statusCode != http.StatusNoContent {
		return fmt.Errorf("failed to delete tag: %s (status %d)", string(respBody), statusCode)
	}

	return nil
}

// GetSubjectTags returns tags assigned to a subject
func (c *SchemaRegistryClient) GetSubjectTags(subject string) ([]TagAssignment, error) {
	qualifiedName := fmt.Sprintf("lsrc:%s", subject)
	urlPath := fmt.Sprintf("%s/catalog/v1/entity/type/sr_subject/name/%s/tags", c.BaseURL, url.PathEscape(qualifiedName))

	respBody, statusCode, err := c.doRequest("GET", urlPath, nil)
	if err != nil {
		return nil, err
	}

	if statusCode == http.StatusNotFound {
		return []TagAssignment{}, nil
	}

	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get subject tags: %s (status %d)", string(respBody), statusCode)
	}

	var tags []TagAssignment
	if err := json.Unmarshal(respBody, &tags); err != nil {
		return nil, fmt.Errorf("failed to parse subject tags response: %w", err)
	}

	return tags, nil
}

// AssignTagToSubject assigns a tag to a subject
func (c *SchemaRegistryClient) AssignTagToSubject(subject, tagName string) error {
	qualifiedName := fmt.Sprintf("lsrc:%s", subject)
	urlPath := fmt.Sprintf("%s/catalog/v1/entity/type/sr_subject/name/%s/tags", c.BaseURL, url.PathEscape(qualifiedName))

	body := []map[string]string{
		{"typeName": tagName},
	}

	respBody, statusCode, err := c.doRequest("POST", urlPath, body)
	if err != nil {
		return err
	}

	if statusCode != http.StatusOK && statusCode != http.StatusCreated {
		return fmt.Errorf("failed to assign tag: %s (status %d)", string(respBody), statusCode)
	}

	return nil
}

// RemoveTagFromSubject removes a tag from a subject
func (c *SchemaRegistryClient) RemoveTagFromSubject(subject, tagName string) error {
	qualifiedName := fmt.Sprintf("lsrc:%s", subject)
	urlPath := fmt.Sprintf("%s/catalog/v1/entity/type/sr_subject/name/%s/tags/%s", c.BaseURL, url.PathEscape(qualifiedName), url.PathEscape(tagName))

	respBody, statusCode, err := c.doRequest("DELETE", urlPath, nil)
	if err != nil {
		return err
	}

	if statusCode != http.StatusOK && statusCode != http.StatusNoContent {
		return fmt.Errorf("failed to remove tag: %s (status %d)", string(respBody), statusCode)
	}

	return nil
}

// GetSchemaTags returns tags assigned to a specific schema version
func (c *SchemaRegistryClient) GetSchemaTags(subject string, version int) ([]TagAssignment, error) {
	qualifiedName := fmt.Sprintf("lsrc:%s:%d", subject, version)
	urlPath := fmt.Sprintf("%s/catalog/v1/entity/type/sr_schema/name/%s/tags", c.BaseURL, url.PathEscape(qualifiedName))

	respBody, statusCode, err := c.doRequest("GET", urlPath, nil)
	if err != nil {
		return nil, err
	}

	if statusCode == http.StatusNotFound {
		return []TagAssignment{}, nil
	}

	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get schema tags: %s (status %d)", string(respBody), statusCode)
	}

	var tags []TagAssignment
	if err := json.Unmarshal(respBody, &tags); err != nil {
		return nil, fmt.Errorf("failed to parse schema tags response: %w", err)
	}

	return tags, nil
}

// AssignTagToSchema assigns a tag to a specific schema version
func (c *SchemaRegistryClient) AssignTagToSchema(subject string, version int, tagName string) error {
	qualifiedName := fmt.Sprintf("lsrc:%s:%d", subject, version)
	urlPath := fmt.Sprintf("%s/catalog/v1/entity/type/sr_schema/name/%s/tags", c.BaseURL, url.PathEscape(qualifiedName))

	body := []map[string]string{
		{"typeName": tagName},
	}

	respBody, statusCode, err := c.doRequest("POST", urlPath, body)
	if err != nil {
		return err
	}

	if statusCode != http.StatusOK && statusCode != http.StatusCreated {
		return fmt.Errorf("failed to assign tag to schema: %s (status %d)", string(respBody), statusCode)
	}

	return nil
}

// RemoveTagFromSchema removes a tag from a specific schema version
func (c *SchemaRegistryClient) RemoveTagFromSchema(subject string, version int, tagName string) error {
	qualifiedName := fmt.Sprintf("lsrc:%s:%d", subject, version)
	urlPath := fmt.Sprintf("%s/catalog/v1/entity/type/sr_schema/name/%s/tags/%s", c.BaseURL, url.PathEscape(qualifiedName), url.PathEscape(tagName))

	respBody, statusCode, err := c.doRequest("DELETE", urlPath, nil)
	if err != nil {
		return err
	}

	if statusCode != http.StatusOK && statusCode != http.StatusNoContent {
		return fmt.Errorf("failed to remove tag from schema: %s (status %d)", string(respBody), statusCode)
	}

	return nil
}

