# VSA Control Plane Documentation System

## Overview

This is a simplified, visual documentation system for the VSA Control Plane that helps new developers onboard and provides easy navigation from documentation to code.

## Features

### 🎯 **Simple Visual Navigation**
- Clean, card-based interface
- Direct navigation to specialized browsers
- Search functionality across all documentation
- Mobile responsive design

### 🔧 **Specialized Browsers**

1. **API Browser** (`api-browser.html`)
   - Real API data with actual GitHub links to code
   - Visual cards showing method, endpoint, description
   - Direct links to workflow code, handlers, and specs
   - Activity lists showing what each API does

2. **Workflow Browser** (`workflow-browser.html`)
   - Categorized workflows (Core, Background, Replication, Control)
   - Visual workflow cards with descriptions
   - Direct GitHub links to workflow code
   - Tab-based filtering by category

3. **Architecture Browser** (`architecture-docs.html`)
   - Complete system overview with component visualization
   - All ADRs (Architecture Decision Records) with direct links
   - All design documents with descriptions
   - Architecture How-To guide

### 📁 **Complete Documentation Coverage**

The system maps to all existing documentation in the `/doc/` folder:

- **Architecture**: `/doc/architecture/decisions/` and `/doc/architecture/designs/`
- **Workflows**: `/doc/workflows/core/`, `/doc/workflows/background/`, etc.
- **APIs**: Links to actual API specs and implementation code
- **Testing**: `/doc/testing/test-plans/`
- **Guides**: `/doc/guides/`
- **Infrastructure**: `/doc/infrastructure/`

## Quick Start

1. **Start the server from project root**:
```bash
cd /path/to/vsa-control-plane
python3 -m http.server 8080
```

2. **Open the main portal**:
   http://localhost:8080/doc/web-ui

**Note**: All documentation links point directly to GitHub for reliable access. The server must be run from the project root to ensure all file paths resolve correctly.

3. **Follow the onboarding path**:
   - Start with Architecture → Explore APIs → Study Workflows → Run Tests

## Architecture Documents Included

### Architecture Decision Records (ADRs)
- ADR-0001: Record Architecture Decisions
- ADR-0002: Database Choice for VCP
- ADR-0003: OpenAPI 3.0.4 for API Definition
- ADR-0004: Use Chi as Go Server Framework
- ADR-0005: VMRS for Preview
- ADR-0006: Large Volume VMRS Decision Maker
- ADR-0007: Orphan Backup Deletion Architecture
- ADR-0008: Backup Scheduler Decision
- ADR-0009: Hybrid Replications
- ADR-0010: Temporal as Orchestrator Engine
- ADR-0011: Slog Logging Framework

### Design Documents
- Design-0001: Cluster Serial Number Generation
- Design-0002: FlexCache Volumes Design
- Design-0003: Backup Vault Design
- Design-0003b: Metrics and Billing Design
- Design-0004: Backup Design
- Design-0005: VSA Auto Tiering Design
- Design-0006: Volume Replication Design
- Design-0007: Background Jobs Design
- Design-0008: Queuing Mechanism for Jobs
- Design-0009: LRO Generic Sequence

### Additional Resources
- Architecture How-To Guide
- System component overview
- Direct links to all source code

## Key Benefits

1. **New Developer Onboarding**: Clear visual path from architecture to implementation
2. **Code Navigation**: Direct links from documentation to GitHub source code
3. **Complete Coverage**: Maps to all existing documentation in the project
4. **Simple Maintenance**: Clean HTML/CSS/JS - easy to update and maintain
5. **Mobile Friendly**: Responsive design works on all devices

## File Structure

```
doc/web-ui/
├── index.html              # Main portal
├── api-browser.html        # API documentation browser
├── workflow-browser.html   # Workflow documentation browser
├── architecture-docs.html  # Architecture documentation browser
├── css/                    # Stylesheets
├── js/                     # JavaScript files
└── README.md              # This file
```

## Development

The system is designed to be simple and maintainable:
- No complex frameworks or dependencies
- Clean, semantic HTML
- CSS Grid and Flexbox for layout
- Vanilla JavaScript for functionality
- Direct links to actual documentation files

## Notes

- All links point to actual files in the repository
- GitHub links use the `document-visualiser` branch
- Local file links use relative paths from the web-ui directory
- The system is designed to work both locally and when deployed to GitHub Pages