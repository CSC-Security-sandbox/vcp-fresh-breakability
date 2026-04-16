# Contributing to VSA Control Plane

Thank you for your interest in contributing to the VSA Control Plane! This document provides guidelines and best practices for contributing to the project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Documentation Guidelines](#documentation-guidelines)
- [Code Standards](#code-standards)
- [Testing](#testing)
- [Pull Request Process](#pull-request-process)
- [Release Process](#release-process)

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you agree to uphold this code.

## Getting Started

### Prerequisites

- Go 1.21 or later
- Docker and Docker Compose
- Git
- Make

### Development Setup

1. **Fork and Clone**
   ```bash
   git clone https://github.com/your-username/vsa-control-plane.git
   cd vsa-control-plane
   ```

2. **Install Dependencies**
   ```bash
   make install-deps
   ```

3. **Run Tests**
   ```bash
   make test
   ```

4. **Start Development Environment**
   ```bash
   make dev
   ```

## Development Workflow

### Branch Naming

Use descriptive branch names with prefixes:

- `feature/` - New features
- `fix/` - Bug fixes
- `docs/` - Documentation updates
- `refactor/` - Code refactoring
- `test/` - Test improvements
- `chore/` - Maintenance tasks

Examples:
- `feature/add-volume-replication`
- `fix/temporal-workflow-timeout`
- `docs/update-api-documentation`

### Commit Messages

All commits and merge request titles must follow the MR commit recommendation format:

<JIRA-ID>: <commit message>

Examples:

PROJ-123: fix volume creation NPE
PROJ-456: docs: update workflow guide

Please include the relevant JIRA/issue ID at the start of the commit message so it can be tracked in release notes and automation.

## Documentation Guidelines

### When to Update Documentation

**Always update documentation when making code changes that affect:**

1. **API Changes**
   - New endpoints
   - Modified request/response schemas
   - Deprecated endpoints
   - Authentication changes

2. **Workflow Changes**
   - New workflows or activities
   - Modified workflow parameters
   - Changed error handling
   - Updated retry policies

3. **Configuration Changes**
   - New environment variables
   - Modified configuration files
   - Changed default values
   - New feature flags

4. **Architecture Changes**
   - New services or components
   - Modified data models
   - Changed database schemas
   - Updated deployment configurations

5. **User-Facing Changes**
   - New CLI commands
   - Modified user interfaces
   - Changed error messages
   - Updated help text

### Documentation Update Checklist

Before submitting a PR, ensure you've updated:

- [ ] **API Documentation** (`doc/api/`)
  - [ ] `overview.md` - High-level API description
  - [ ] `endpoints.md` - Detailed endpoint documentation
  - [ ] Update OpenAPI/Swagger specs if applicable

- [ ] **Architecture Documentation** (`doc/architecture/`)
  - [ ] **Decisions** (`decisions/`) - New ADRs for architectural decisions
  - [ ] **Designs** (`designs/`) - Design documents for new features
  - [ ] Update existing documents if changes affect them

- [ ] **Workflow Documentation** (`doc/workflows/`)
  - [ ] Create new workflow files for new workflows
  - [ ] Update existing workflow documentation
  - [ ] Update `README.md` index

- [ ] **Guides** (`doc/guides/`)
  - [ ] Update relevant guides
  - [ ] Create new guides for new features
  - [ ] Update troubleshooting guides

- [ ] **Code Comments**
  - [ ] Update function/struct comments
  - [ ] Add examples where helpful
  - [ ] Update package documentation

### Documentation Standards

1. **Markdown Formatting**
   - Use proper heading hierarchy (H1 → H2 → H3)
   - Include table of contents for long documents
   - Use code blocks with language specification
   - Include examples and use cases

2. **Code References**
   - Use relative paths for internal links
   - Include line numbers for specific code references
   - Keep links up-to-date with code changes

3. **Consistency**
   - Follow existing documentation patterns
   - Use consistent terminology
   - Maintain similar structure across documents

4. **Accuracy**
   - Verify all code examples work
   - Test all links (use `make link-check`)
   - Keep information current and relevant

### Link Checking

**Always run link checking before submitting PRs:**

```bash
# Check all documentation links
make link-check

# Check specific directory
go run ./scripts/link-checker/link-checker.go doc/architecture/

# Check specific file
go run ./scripts/link-checker/link-checker.go doc/guides/onboarding.md
```

**Fix broken links immediately** - PRs with broken links will be rejected.

## Code Standards

### Go Code Style

1. **Formatting**
   ```bash
   make fix-imports  # Fix imports and formatting
   ```

2. **Linting**
   ```bash
   make lint  # Run all linting checks
   ```

3. **Code Review Guidelines**
   - Follow Go best practices
   - Use meaningful variable and function names
   - Add comments for complex logic
   - Handle errors appropriately
   - Write testable code

### File Organization

```
doc/
├── api/                    # API documentation
├── architecture/           # Architecture documentation
│   ├── decisions/         # Architecture Decision Records (ADRs)
│   └── designs/           # Design documents
├── guides/                # User and developer guides
└── workflows/             # Workflow documentation
    ├── core/              # Core workflows
    ├── background/        # Background workflows
    ├── replication/       # Replication workflows
    ├── kms/              # KMS workflows
    ├── flexcache/        # FlexCache workflows
    └── control/          # Control workflows
```

## Testing

### Test Requirements

1. **Unit Tests**
   - Test all new functions and methods
   - Achieve >80% code coverage
   - Use table-driven tests where appropriate

2. **Integration Tests**
   - Test workflow integrations
   - Test API endpoints
   - Test database operations

3. **Documentation Tests**
   - Run link checking
   - Verify code examples work
   - Test all CLI commands

### Running Tests

```bash
# Run all tests
make test

# Run specific test package
go test ./core/orchestrator/workflows/...

# Run tests with coverage
go test -cover ./...

# Run link checking
make link-check
```

## Pull Request Process

### Before Submitting

1. **Update Documentation**
   - Follow the [Documentation Update Checklist](#documentation-update-checklist)
   - Run `make link-check` and fix any broken links
   - Update relevant guides and examples

2. **Run All Checks**
   ```bash
   make lint          # Code linting
   make test          # Run tests
   make link-check    # Check documentation links
   ```

3. **Commit Changes**
   - Use conventional commit messages
   - Include documentation updates in commits
   - Keep commits focused and atomic

### PR Description Template

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Documentation Changes
- [ ] API documentation updated
- [ ] Architecture documentation updated
- [ ] Workflow documentation updated
- [ ] Guides updated
- [ ] Code comments updated

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] Link checking passed
- [ ] Manual testing completed

## Checklist
- [ ] Code follows project style guidelines
- [ ] Self-review completed
- [ ] Documentation is up-to-date
- [ ] No broken links
- [ ] Tests pass
```

### Review Process

1. **Automated Checks**
   - Code linting
   - Unit tests
   - Link checking
   - Security scanning

2. **Manual Review**
   - Code quality
   - Documentation accuracy
   - Test coverage
   - Performance impact

3. **Documentation Review**
   - Accuracy of changes
   - Completeness of updates
   - Link validity
   - Example correctness

## Release Process

### Documentation for Releases

1. **Update Changelog**
   - Document new features
   - List bug fixes
   - Note breaking changes
   - Update migration guides

2. **Update Version Documentation**
   - Update API version information
   - Update compatibility matrices
   - Update installation guides

3. **Review Documentation**
   - Ensure all links work
   - Verify examples are current
   - Check for outdated information

## Documentation Maintenance

### Regular Tasks

1. **Monthly Reviews**
   - Check for broken links
   - Update outdated information
   - Review user feedback

2. **Release Reviews**
   - Update version-specific information
   - Review API changes
   - Update migration guides

3. **Continuous Monitoring**
   - Monitor link checker results
   - Address documentation issues quickly
   - Keep examples current

### Tools and Automation

1. **Link Checking**
   ```bash
   make link-check  # Check all documentation links
   ```

2. **Documentation Generation**
   ```bash
   make docs        # Generate documentation (if applicable)
   ```

3. **CI/CD Integration**
   - Automated link checking in PRs
   - Documentation validation
   - Broken link detection

## Getting Help

### Resources

- **Documentation**: Check `doc/` directory
- **Code Examples**: Look in `examples/` directory
- **API Reference**: See `doc/api/` directory
- **Architecture**: Review `doc/architecture/` directory

### Support Channels

- **Issues**: GitHub Issues for bugs and feature requests
- **Discussions**: GitHub Discussions for questions
- **Code Review**: PR comments for specific code questions

### Common Issues

1. **Broken Links**
   - Run `make link-check` to identify issues
   - Use relative paths for internal links
   - Update links when moving files

2. **Outdated Documentation**
   - Review documentation with each code change
   - Update examples when APIs change
   - Keep configuration examples current

3. **Missing Documentation**
   - Add documentation for new features
   - Include examples and use cases
   - Document configuration options

## Best Practices

### Documentation

1. **Write for Your Audience**
   - Developers for API docs
   - Operators for deployment guides
   - Users for feature documentation

2. **Keep It Simple**
   - Use clear, concise language
   - Include practical examples
   - Avoid unnecessary complexity

3. **Make It Discoverable**
   - Use descriptive titles
   - Include in table of contents
   - Cross-reference related topics

4. **Keep It Current**
   - Update with code changes
   - Remove outdated information
   - Test all examples

### Code

1. **Self-Documenting Code**
   - Use meaningful names
   - Write clear comments
   - Structure code logically

2. **Consistent Patterns**
   - Follow project conventions
   - Use established patterns
   - Maintain consistency

3. **Test Everything**
   - Write comprehensive tests
   - Test edge cases
   - Verify documentation examples

## Contributing to This Guide

This contributing guide is a living document. If you find areas for improvement:

1. **Open an Issue** - Report problems or suggest improvements
2. **Submit a PR** - Propose specific changes
3. **Discuss** - Use GitHub Discussions for broader topics

Thank you for helping make the VSA Control Plane documentation better for everyone!