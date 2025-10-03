# Documentation Update Guide

This guide provides step-by-step instructions for keeping documentation in sync with code changes.

## Quick Reference

### Before Making Code Changes
1. Identify which documentation needs updating
2. Plan documentation changes alongside code changes
3. Consider impact on existing documentation

### After Making Code Changes
1. Update relevant documentation files
2. Run link checking: `make link-check`
3. Test all examples and code snippets
4. Update cross-references and links

## Documentation Update Workflow

### 1. Identify Documentation Impact

When making code changes, ask yourself:

- **Does this change the API?** → Update `doc/api/`
- **Does this add/modify workflows?** → Update `doc/workflows/`
- **Does this change architecture?** → Update `doc/architecture/`
- **Does this affect users?** → Update `doc/guides/`
- **Does this change configuration?** → Update configuration docs

### 2. Update Documentation Files

#### API Changes
```bash
# Update API documentation
vim doc/api/overview.md
vim doc/api/endpoints.md

# Update OpenAPI spec if applicable
vim google-proxy/api/gcp-api.yaml
```

#### Workflow Changes
```bash
# Create new workflow documentation
touch doc/workflows/core/new-workflow.md

# Update workflow index
vim doc/workflows/README.md

# Update existing workflow docs
vim doc/workflows/core/volume-workflows.md
```

#### Architecture Changes
```bash
# Create new ADR
touch doc/architecture/decisions/0012-new-decision.md

# Create design document
touch doc/architecture/designs/0009-new-design.md

# Update existing architecture docs
vim doc/architecture/decisions/0011-slog-logging-framework.md
```

#### Guide Updates
```bash
# Update user guides
vim doc/guides/onboarding.md
vim doc/guides/temporal-debugging.md

# Create new guides
touch doc/guides/new-feature-guide.md
```

### 3. Update Code Comments

```go
// Before
// func CreateVolume(params VolumeParams) error {
//     // implementation
// }

// After
// CreateVolume creates a new volume with the specified parameters.
// It validates the parameters, creates the volume in ONTAP,
// and updates the database with the volume information.
//
// Parameters:
//   - params: Volume creation parameters including name, size, and pool
//
// Returns:
//   - error: Any error that occurred during volume creation
//
// Example:
//   params := VolumeParams{
//       Name: "my-volume",
//       Size: "100GB",
//       Pool: "pool-1",
//   }
//   err := CreateVolume(params)
// (function implementation omitted in docs)
```

### 4. Test Documentation

```bash
# Check all links
make link-check

# Test code examples
go run examples/volume-creation.go

# Verify CLI commands
./vsa-cli volume create --help
```

## Common Documentation Update Scenarios

### Adding a New Workflow

1. **Create Workflow Documentation**
   ```bash
   # Create workflow file
   touch doc/workflows/core/new-workflow.md
   ```

2. **Document the Workflow**
   ```markdown
   # New Workflow

   ## Overview
   Brief description of what the workflow does.

   ## Setup
   Prerequisites and configuration.

   ## Execution Flow
   1. Step 1
   2. Step 2
   3. Step 3

   ## Activities
   - Activity1: Description
   - Activity2: Description

   ## Error Handling
   How errors are handled.

   ## Examples
   Code examples and usage.
   ```

3. **Update Workflow Index**
   ```markdown
   # Workflows

   ## Core Workflows
   - [Volume Workflows](../workflows/core/volume-workflows.md)
   ```

4. **Update Code Comments**
   ```go
   // NewWorkflow creates and manages new resources.
   // This workflow handles the complete lifecycle of resource creation,
   // including validation, provisioning, and cleanup on failure.
   func NewWorkflow(ctx workflow.Context, params NewWorkflowParams) error {
       // implementation
   }
   ```

### Modifying an Existing API

1. **Update API Documentation**
   ```markdown
   ## POST /api/v1/volumes

   Creates a new volume with the specified parameters.

   ### Request Body
   ```json
   {
     "name": "string",
     "size": "string",
     "pool": "string",
     "newField": "string"  // Add new field
   }
   ```

   ### Response
   ```json
   {
     "id": "string",
     "status": "string",
     "newStatus": "string"  // Add new field
   }
   ```
   ```

2. **Update OpenAPI Spec**
   ```yaml
   /api/v1/volumes:
     post:
       requestBody:
         content:
           application/json:
             schema:
               type: object
               properties:
                 name:
                   type: string
                 newField:  # Add new field
                   type: string
   ```

3. **Update Code Comments**
   ```go
   // CreateVolumeParams represents the parameters for creating a volume.
   type CreateVolumeParams struct {
       Name     string `json:"name"`
       Size     string `json:"size"`
       Pool     string `json:"pool"`
       NewField string `json:"newField"` // Add new field
   }
   ```

### Adding a New Configuration Option

1. **Update Configuration Documentation**
   ```markdown
   ## Environment Variables

   | Variable | Description | Default | Required |
   |----------|-------------|---------|----------|
   | `NEW_FEATURE_ENABLED` | Enable new feature | `false` | No |
   ```

2. **Update Code Comments**
   ```go
   // NewFeatureEnabled determines if the new feature is enabled.
   // This can be set via the NEW_FEATURE_ENABLED environment variable.
   var NewFeatureEnabled = env.GetBool("NEW_FEATURE_ENABLED", false)
   ```

3. **Update Examples**
   ```bash
   # Enable new feature
   export NEW_FEATURE_ENABLED=true
   ./vsa-control-plane
   ```

### Deprecating a Feature

1. **Add Deprecation Notice**
   ```markdown
   ## Deprecated Features

   ### Old API Endpoint
   **Deprecated**: `POST /api/v1/old-endpoint`
   **Replacement**: `POST /api/v1/new-endpoint`
   **Removal Date**: 2024-06-01

   This endpoint is deprecated and will be removed in version 2.0.
   Please use the new endpoint instead.
   ```

2. **Update Code Comments**
   ```go
   // OldFunction is deprecated and will be removed in version 2.0.
   // Use NewFunction instead.
   //
   // Deprecated: Use NewFunction instead.
   func OldFunction() error {
       // implementation
   }
   ```

3. **Update Migration Guide**
   ```markdown
   # Migration Guide

   ## From OldFunction to NewFunction

   ### Before
   ```go
   err := OldFunction()
   ```

   ### After
   ```go
   err := NewFunction()
   ```
   ```

## Documentation Quality Checklist

### Before Submitting PR

- [ ] **Links Work**
  ```bash
  make link-check
  ```

- [ ] **Examples Run**
  ```bash
  # Test all code examples
  go run examples/...
  ```

- [ ] **Cross-References Updated**
  - Check all internal links
  - Update related documentation
  - Verify references are current

- [ ] **Formatting Consistent**
  - Use proper markdown formatting
  - Follow existing style patterns
  - Include table of contents for long docs

- [ ] **Content Accurate**
  - Verify all information is current
  - Check configuration examples
  - Validate API examples

### Code Comment Quality

- [ ] **Function Comments**
  - Describe what the function does
  - Document parameters and return values
  - Include usage examples

- [ ] **Struct Comments**
  - Describe the purpose of the struct
  - Document field meanings
  - Include usage examples

- [ ] **Package Comments**
  - Describe package purpose
  - Include usage examples
  - Document main concepts

## Automation and Tools

### Link Checking
```bash
# Check all documentation
make link-check

# Check specific directory
go run ./scripts/link-checker/link-checker.go doc/architecture/

# Check specific file
go run ./scripts/link-checker/link-checker.go doc/guides/onboarding.md
```

### Documentation Generation
```bash
# Generate API documentation (if applicable)
make docs

# Generate workflow documentation
make workflow-docs
```

### CI/CD Integration
The following checks run automatically on PRs:

- Link checking
- Code linting
- Test execution
- Documentation validation

## Common Pitfalls

### 1. Forgetting to Update Cross-References
**Problem**: Updating one document but forgetting related documents.

**Solution**: Use `grep` to find references:
```bash
grep -r "old-function-name" doc/
grep -r "old-endpoint" doc/
```

### 2. Breaking Links When Moving Files
```bash
# Before moving
[Workflow Guide](../workflows/core/volume-workflows.md)

# After moving
[Workflow Guide](../workflows/core/volume-workflows.md)
```

### 3. Outdated Examples
**Problem**: Code examples that no longer work.

**Solution**: Test all examples:
```bash
# Test API examples
curl -X POST http://localhost:8080/api/v1/volumes \
  -H "Content-Type: application/json" \
  -d '{"name": "test-volume", "size": "100GB"}'

# Test CLI examples
./vsa-cli volume create --name test-volume --size 100GB
```

### 4. Inconsistent Terminology
**Problem**: Using different terms for the same concept.

**Solution**: Maintain a terminology glossary:
```markdown
# Terminology

- **Volume**: A storage volume in ONTAP
- **Pool**: A storage pool containing volumes
- **Workflow**: A Temporal workflow for orchestration
```

## Best Practices

### 1. Update Documentation with Code
- Don't treat documentation as an afterthought
- Update docs in the same commit as code changes
- Include documentation updates in PR descriptions

### 2. Use Consistent Patterns
- Follow existing documentation structure
- Use similar formatting across documents
- Maintain consistent terminology

### 3. Test Everything
- Run link checking before submitting
- Test all code examples
- Verify configuration examples work

### 4. Keep It Simple
- Write for your audience
- Use clear, concise language
- Include practical examples

### 5. Make It Discoverable
- Use descriptive titles
- Include in table of contents
- Cross-reference related topics

## Getting Help

### Resources
- **Link Checker**: `make link-check`
- **Code Examples**: Check `examples/` directory
- **Existing Docs**: Review similar documentation

### Support
- **Issues**: GitHub Issues for documentation problems
- **Discussions**: GitHub Discussions for questions
- **Code Review**: PR comments for specific questions

## Contributing to This Guide

This guide is a living document. If you find areas for improvement:

1. **Open an Issue** - Report problems or suggest improvements
2. **Submit a PR** - Propose specific changes
3. **Discuss** - Use GitHub Discussions for broader topics

Remember: Good documentation is essential for project success. Take the time to keep it accurate and up-to-date!