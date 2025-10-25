#!/usr/bin/env python3
"""
Enhanced Documentation Generator Integration

Integrates Go code analysis and workflow tracing into documentation generation.
Generates human-level detail documentation with:
- Activity tables with signatures
- Detailed sequence diagrams
- JSONB attribute expansion
- Retry/timeout configurations
"""

from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

from go_code_analyzer import GoCodeAnalyzer, WorkflowAnalysis
from workflow_tracer import WorkflowExecutionTracer


class EnhancedDocumentationGenerator:
    """Generates enhanced documentation with deep code analysis"""
    
    def __init__(self, repo_root: Path):
        self.repo_root = repo_root
        self.analyzer = GoCodeAnalyzer(repo_root)
        self.tracer = WorkflowExecutionTracer(self.analyzer)
        
        # Pre-load all activities for faster lookup
        self.analyzer.analyze_all_activities()
    
    def generate_workflow_implementation_section(self, workflow_analysis: WorkflowAnalysis) -> str:
        """
        Generate Workflow Implementation section with activity details.
        
        Returns:
            Markdown section with activity table and sequence diagram
        """
        sections = []
        
        # Activity Table
        sections.append("## Workflow Implementation")
        sections.append("")
        sections.append("### Activities")
        sections.append("")
        sections.append(self._generate_activity_table(workflow_analysis))
        sections.append("")
        
        # Sequence Diagram
        sections.append("### Execution Flow")
        sections.append("")
        sections.append(self.tracer.generate_sequence_diagram(workflow_analysis))
        sections.append("")
        
        return "\n".join(sections)
    
    def _generate_activity_table(self, workflow_analysis: WorkflowAnalysis) -> str:
        """Generate detailed activity table."""
        # Get unique activities
        activity_names = list(dict.fromkeys([call.activity_name for call in workflow_analysis.activity_calls]))
        
        lines = [
            "| Activity | Purpose | Input Parameters | Return Types |",
            "|----------|---------|------------------|--------------|"
        ]
        
        for activity_name in activity_names:
            # Get activity definition
            activity_def = self.analyzer.activity_definitions.get(activity_name)
            
            if activity_def:
                purpose = activity_def.docstring[:60] + "..." if len(activity_def.docstring) > 60 else activity_def.docstring
                
                # Format parameters
                params = ", ".join([f"`{p['name']}`: {p['type']}" for p in activity_def.parameters[:2]])
                if len(activity_def.parameters) > 2:
                    params += f", ... ({len(activity_def.parameters)} total)"
                
                # Format return types
                returns = ", ".join([f"`{rt}`" for rt in activity_def.return_types])
                
                lines.append(f"| **{activity_name}** | {purpose} | {params} | {returns} |")
            else:
                # Fallback if definition not found
                purpose = self._infer_purpose(activity_name)
                lines.append(f"| **{activity_name}** | {purpose} | - | - |")
        
        return "\n".join(lines)
    
    def _infer_purpose(self, activity_name: str) -> str:
        """Infer purpose from activity name."""
        import re
        name = activity_name.replace('Activity', '')
        words = re.sub(r'([A-Z])', r' \1', name).strip().split()
        
        if words[0].lower() in ['get', 'find', 'fetch']:
            return f"Retrieve {' '.join(words[1:]).lower()}"
        elif words[0].lower() in ['create', 'add']:
            return f"Create new {' '.join(words[1:]).lower()}"
        elif words[0].lower() in ['delete', 'remove']:
            return f"Delete {' '.join(words[1:]).lower()}"
        elif words[0].lower() in ['update', 'modify']:
            return f"Update {' '.join(words[1:]).lower()}"
        else:
            return ' '.join(words)
    
    def generate_error_handling_section(self, workflow_analysis: WorkflowAnalysis) -> str:
        """Generate Error Handling and Retry section."""
        sections = []
        
        sections.append("## Error Handling and Retry Configuration")
        sections.append("")
        
        if workflow_analysis.error_handling:
            sections.append("### Error Handling Patterns")
            sections.append("")
            for pattern in workflow_analysis.error_handling:
                sections.append(f"- **{pattern}**")
            sections.append("")
        
        if workflow_analysis.retry_config:
            sections.append("### Retry Policy")
            sections.append("")
            sections.append("| Parameter | Value |")
            sections.append("|-----------|-------|")
            
            rc = workflow_analysis.retry_config
            if rc.initial_interval:
                sections.append(f"| Initial Interval | `{rc.initial_interval}` |")
            if rc.backoff_coefficient:
                sections.append(f"| Backoff Coefficient | `{rc.backoff_coefficient}` |")
            if rc.maximum_interval:
                sections.append(f"| Maximum Interval | `{rc.maximum_interval}` |")
            if rc.maximum_attempts:
                sections.append(f"| Maximum Attempts | `{rc.maximum_attempts}` |")
            if rc.non_retryable_errors:
                errors_list = ", ".join([f"`{e}`" for e in rc.non_retryable_errors])
                sections.append(f"| Non-Retryable Errors | {errors_list} |")
            
            sections.append("")
        
        if workflow_analysis.timeouts:
            sections.append("### Timeout Configuration")
            sections.append("")
            sections.append("| Timeout Type | Value |")
            sections.append("|--------------|-------|")
            
            for timeout_name, timeout_value in workflow_analysis.timeouts.items():
                sections.append(f"| {timeout_name} | `{timeout_value}` |")
            
            sections.append("")
        
        return "\n".join(sections)
    
    def generate_data_model_with_jsonb(self, resource_type: str) -> str:
        """Generate enhanced data model section with JSONB expansion."""
        sections = []
        
        sections.append("## Data Model")
        sections.append("")
        
        # Try to extract struct definition
        models_file = self.repo_root / "core" / "datamodel" / "models.go"
        
        if models_file.exists():
            # Find the main struct
            struct_name = resource_type.title().replace('_', '')
            struct_def = self.analyzer.extract_struct_definition(models_file, struct_name)
            
            if struct_def:
                sections.append(f"### {struct_name} Entity")
                sections.append("")
                sections.append("| Field | Type | GORM Tags | Description |")
                sections.append("|-------|------|-----------|-------------|")
                
                for field in struct_def.fields[:15]:  # Limit to first 15 fields
                    gorm = field.gorm_tag or "-"
                    desc = field.description or "-"
                    sections.append(f"| **{field.name}** | `{field.type}` | {gorm} | {desc} |")
                
                if len(struct_def.fields) > 15:
                    sections.append(f"| ... | ... | ... | *{len(struct_def.fields) - 15} more fields* |")
                
                sections.append("")
                
                # Find JSONB attributes
                jsonb_attrs = self.analyzer.find_jsonb_attributes(models_file, struct_name)
                
                if jsonb_attrs:
                    sections.append("### JSONB Attributes")
                    sections.append("")
                    
                    for attr_name, attr_struct in jsonb_attrs.items():
                        sections.append(f"#### {attr_name}")
                        sections.append("")
                        sections.append("| Field | Type | Description |")
                        sections.append("|-------|------|-------------|")
                        
                        for field in attr_struct.fields[:20]:
                            desc = field.description or "-"
                            sections.append(f"| **{field.name}** | `{field.type}` | {desc} |")
                        
                        if len(attr_struct.fields) > 20:
                            sections.append(f"| ... | ... | *{len(attr_struct.fields) - 20} more fields* |")
                        
                        sections.append("")
        
        return "\n".join(sections)
    
    def generate_conditional_logic_section(self, workflow_analysis: WorkflowAnalysis) -> str:
        """Generate section documenting conditional branches."""
        if not workflow_analysis.conditional_branches:
            return ""
        
        sections = []
        
        sections.append("## Conditional Logic")
        sections.append("")
        sections.append("The workflow includes the following conditional paths:")
        sections.append("")
        
        for i, branch in enumerate(workflow_analysis.conditional_branches, 1):
            if branch['type'] == 'if':
                condition = branch['condition'][:80]
                sections.append(f"{i}. **If Condition** (Line {branch['line_number']})")
                sections.append(f"   - Condition: `{condition}`")
                if branch.get('has_activities'):
                    sections.append(f"   - Contains activity calls")
                sections.append("")
            elif branch['type'] == 'switch':
                sections.append(f"{i}. **Switch Statement** (Line {branch['line_number']})")
                sections.append(f"   - Cases: {branch.get('case_count', 'unknown')}")
                sections.append("")
        
        return "\n".join(sections)
    
    def generate_complete_workflow_documentation(self, 
                                                 workflow_name: str,
                                                 resource_type: str,
                                                 operations: List,
                                                 feature_info: Dict) -> str:
        """
        Generate complete workflow documentation with all enhancements.
        
        Args:
            workflow_name: Name of the workflow
            resource_type: Resource type (e.g., 'backup', 'volume')
            operations: List of API operations
            feature_info: Feature metadata
        
        Returns:
            Complete markdown documentation
        """
        # Find workflow file
        workflow_file = self.repo_root / "core" / "orchestrator" / "workflows" / f"{workflow_name}_workflow.go"
        if not workflow_file.exists():
            workflow_file = self.repo_root / "core" / "orchestrator" / "workflows" / f"{workflow_name}_workflows.go"
        
        workflow_analysis = None
        if workflow_file.exists():
            workflow_analysis = self.analyzer.analyze_workflow_file(workflow_file)
        
        # Build document
        resource_title = resource_type.replace('_', ' ').title()
        doc_type = "Lifecycle Management" if feature_info.get('is_foundational') else "Advanced Enhancement"
        
        sections = []
        
        # Header
        sections.append(f"# {resource_title} {doc_type} Workflow Design")
        sections.append("")
        sections.append("> 🤖 **Note**: This document was automatically generated with deep code analysis.")
        sections.append("")
        
        # Table of Contents
        sections.append("## Table of Contents")
        sections.append("1. [Overview](#overview)")
        sections.append("2. [Data Model](#data-model)")
        sections.append("3. [API Operations](#api-operations)")
        if workflow_analysis:
            sections.append("4. [Workflow Implementation](#workflow-implementation)")
            sections.append("5. [Error Handling](#error-handling-and-retry-configuration)")
            if workflow_analysis.conditional_branches:
                sections.append("6. [Conditional Logic](#conditional-logic)")
        sections.append("")
        
        # Overview
        sections.append("## Overview")
        sections.append("")
        sections.append(f"The {resource_title} workflow provides {'foundational infrastructure' if feature_info.get('is_foundational') else 'advanced capabilities'} for the VSA Control Plane.")
        sections.append("")
        sections.append("### Key Features")
        sections.append(f"- **Feature Type**: {feature_info.get('feature_type', 'Unknown').title()}")
        sections.append(f"- **Workflow Type**: {feature_info.get('workflow_type', 'Management').title()}")
        sections.append(f"- **Operations**: {len(operations)} API operations")
        sections.append("")
        
        # Data Model
        sections.append(self.generate_data_model_with_jsonb(resource_type))
        sections.append("")
        
        # API Operations
        sections.append("## API Operations")
        sections.append("")
        sections.append(f"Found {len(operations)} operations for {resource_title}:")
        sections.append("")
        sections.append("| Operation | Service |")
        sections.append("|-----------|----------|")
        for op in operations[:15]:
            sections.append(f"| {op.operation_id} | {op.service} |")
        if len(operations) > 15:
            sections.append(f"| ... | *{len(operations) - 15} more operations* |")
        sections.append("")
        
        # Workflow Implementation (if analysis available)
        if workflow_analysis:
            sections.append(self.generate_workflow_implementation_section(workflow_analysis))
            sections.append("")
            
            # Error Handling
            sections.append(self.generate_error_handling_section(workflow_analysis))
            sections.append("")
            
            # Conditional Logic
            if workflow_analysis.conditional_branches:
                sections.append(self.generate_conditional_logic_section(workflow_analysis))
                sections.append("")
        
        # Footer
        sections.append("---")
        sections.append("*Generated by VSA Control Plane Documentation Generator with Deep Code Analysis*")
        sections.append(f"*Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
        
        return "\n".join(sections)


# Testing
if __name__ == "__main__":
    repo_root = Path(__file__).parent.parent
    generator = EnhancedDocumentationGenerator(repo_root)
    
    # Test with backup workflow
    from api_workflow_analyzer import APIOperation
    
    operations = [
        APIOperation(
            operation_id="post_backup",
            method="POST",
            path="/backups",
            service="google-proxy",
            resource_type="backup",
            operation_type="create"
        )
    ]
    
    feature_info = {
        'is_foundational': True,
        'feature_type': 'foundational',
        'workflow_type': 'management'
    }
    
    doc = generator.generate_complete_workflow_documentation(
        workflow_name="backup",
        resource_type="backup",
        operations=operations,
        feature_info=feature_info
    )
    
    print(doc)
