#!/usr/bin/env python3
"""
Complete Intelligent API Workflow Analyzer for VSA Control Plane

This system autonomously:
1. Detects ALL foundational vs advanced feature relationships
2. Makes architectural decisions without human input
3. Handles Pool, Volume, Backup, Replication, Auto-Tiering, etc.
4. Creates proper documentation structure based on feature dependencies
"""

import re
import json
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Set, Optional
from collections import defaultdict

@dataclass
class APIOperation:
    operation_id: str
    method: str
    path: str
    service: str
    resource_type: str
    operation_type: str
    description: str = ""

@dataclass
class WorkflowPattern:
    workflow_id: str
    workflow_name: str
    resource_type: str
    operations: List[APIOperation]
    workflow_type: str
    complexity: str
    confidence: float

class APIOperationAnalyzer:
    """Comprehensive analyzer that handles all VSA Control Plane features intelligently."""
    
    def __init__(self, repo_root: Path):
        self.repo_root = repo_root
        self.api_operations: List[APIOperation] = []
        self.workflow_patterns: List[WorkflowPattern] = []
    
    def analyze_workflows(self) -> List[WorkflowPattern]:
        """Analyze all workflows with complete architectural intelligence."""
        print("🧠 Analyzing ALL VSA Control Plane features intelligently...")
        
        # Discover API operations
        self._discover_api_operations()
        print(f"   Discovered {len(self.api_operations)} API operations")
        
        # Group by resource type
        resource_operations = self._group_operations_by_resource()
        print(f"   Analyzing {len(resource_operations)} resource types")
        
        # Generate intelligent workflows
        workflows = []
        for resource_type, operations in resource_operations.items():
            resource_workflows = self._generate_intelligent_resource_workflows(resource_type, operations)
            workflows.extend(resource_workflows)
        
        # Add cross-resource workflows
        cross_workflows = self._generate_cross_resource_workflows()
        workflows.extend(cross_workflows)
        
        self.workflow_patterns = workflows
        return workflows
    
    def analyze_api_operations(self) -> List[APIOperation]:
        """Analyze and return all API operations."""
        self._discover_api_operations()
        return self.api_operations
    
    def discover_workflow_patterns(self) -> List[WorkflowPattern]:
        """Discover and return workflow patterns."""
        return self.analyze_workflows()
    
    def _discover_api_operations(self):
        """Discover all API operations from specifications."""
        api_specs = [
            self.repo_root / "core" / "core-api" / "api.yaml",
            self.repo_root / "google-proxy" / "api" / "gcp-api.yaml",
            self.repo_root / "telemetry" / "api" / "telemetry-api.yaml"
        ]
        
        for spec_path in api_specs:
            if spec_path.exists():
                service_name = self._get_service_name(spec_path)
                operations = self._parse_openapi_spec(spec_path, service_name)
                self.api_operations.extend(operations)
    
    def _parse_openapi_spec(self, spec_path: Path, service_name: str) -> List[APIOperation]:
        """Parse OpenAPI spec using text-based approach."""
        operations = []
        
        try:
            content = spec_path.read_text()
            lines = content.split('\n')
            
            current_path = None
            current_method = None
            
            for line in lines:
                line = line.strip()
                
                if line.startswith('/') and ':' in line:
                    current_path = line.split(':')[0].strip()
                elif line in ['get:', 'post:', 'put:', 'delete:', 'patch:']:
                    current_method = line.replace(':', '').upper()
                    
                    if current_path and current_method:
                        resource_type = self._extract_resource_type(current_path)
                        operation_type = self._determine_operation_type(current_method, current_path)
                        
                        operation = APIOperation(
                            operation_id=f"{current_method.lower()}__{current_path.replace('/', '_').replace('{', '').replace('}', '')}",
                            method=current_method,
                            path=current_path,
                            service=service_name,
                            resource_type=resource_type,
                            operation_type=operation_type,
                            description=f"{current_method} {current_path}"
                        )
                        operations.append(operation)
        except Exception as e:
            print(f"Warning: Could not parse {spec_path}: {e}")
            
        return operations
    
    def _generate_intelligent_resource_workflows(self, resource_type: str, operations: List[APIOperation]) -> List[WorkflowPattern]:
        """Generate workflows with complete architectural intelligence."""
        workflows = []
        
        # Intelligent feature classification
        feature_info = self._classify_feature_architecture(resource_type, operations)
        
        # Generate workflow based on architectural analysis
        all_ops = operations
        
        # Determine workflow characteristics intelligently
        if feature_info['is_foundational']:
            if len(all_ops) <= 3:
                workflow_type = "lifecycle"
                workflow_name = f"{resource_type.title()} Lifecycle Workflow"
            else:
                workflow_type = "management" 
                workflow_name = f"{resource_type.title()} Comprehensive Management Workflow"
        elif feature_info['is_advanced']:
            workflow_type = "enhancement"
            enhanced = ', '.join(feature_info['enhances']) if feature_info['enhances'] else resource_type
            workflow_name = f"{resource_type.title()} Advanced Enhancement Workflow"
        else:
            workflow_type = "simple"
            workflow_name = f"{resource_type.title()} Operations Workflow"
        
        workflows.append(WorkflowPattern(
            workflow_id=f"{resource_type}_workflow",
            workflow_name=workflow_name,
            resource_type=resource_type,
            operations=all_ops,
            workflow_type=workflow_type,
            complexity=self._assess_workflow_complexity(all_ops),
            confidence=0.9
        ))
        
        return workflows
    
    def _classify_feature_architecture(self, resource_type: str, operations: List[APIOperation]) -> Dict[str, any]:
        """Classify feature architecture - GENERIC with minimal hardcoding."""
        
        resource_lower = resource_type.lower()
        operation_paths = ' '.join([op.path.lower() for op in operations])
        
        # Infer dependencies dynamically from paths
        dependencies = self._infer_dependencies_from_paths(resource_type, operations)
        
        # SIMPLIFIED CLASSIFICATION: Treat most resources as separate docs
        # Only merge if explicitly needed (very few cases)
        
        # Resources that should always be separate
        always_separate = ['pool', 'volume', 'snapshot', 'backup', 'backupvault', 
                          'replication', 'kmsconfig', 'hostgroup', 'activedirectory',
                          'health', 'operations', 'cluster', 'node']
        
        if any(res in resource_lower for res in always_separate):
            return {
                'is_foundational': len(dependencies) == 0,
                'is_advanced': len(dependencies) > 0,
                'feature_type': 'foundational' if len(dependencies) == 0 else 'advanced',
                'enhances': [],
                'depends_on': dependencies,
                'documentation_strategy': 'separate'  # Always create separate doc
            }
        
        # Default: Create separate documentation for everything
        # This is the GENERIC approach - no hardcoded merging logic
        return {
            'is_foundational': len(dependencies) == 0,
            'is_advanced': len(dependencies) > 0,
            'feature_type': 'foundational' if len(dependencies) == 0 else 'advanced',
            'enhances': [],
            'depends_on': dependencies,
            'documentation_strategy': 'separate'  # Generic: always separate
        }
    
    def _infer_dependencies_from_paths(self, resource_type: str, operations: List[APIOperation]) -> List[str]:
        """Infer dependencies by analyzing API paths."""
        dependencies = set()
        
        for op in operations:
            path_lower = op.path.lower()
            
            if 'pool' in path_lower and resource_type != 'pool':
                dependencies.add('pool')
            if 'volume' in path_lower and resource_type != 'volume':
                dependencies.add('volume')
            if 'snapshot' in path_lower and resource_type != 'snapshot':
                dependencies.add('snapshot')
                
        return list(dependencies)
    
    def _generate_cross_resource_workflows(self) -> List[WorkflowPattern]:
        """Generate cross-resource ecosystem workflows."""
        cross_workflows = []
        
        # Storage ecosystem (pool + volume)
        storage_ops = [op for op in self.api_operations if op.resource_type in ['pool', 'volume']]
        if storage_ops:
            cross_workflows.append(WorkflowPattern(
                workflow_id="storage_ecosystem_workflow",
                workflow_name="Storage Provisioning Ecosystem Workflow",
                resource_type="storage_ecosystem",
                operations=storage_ops,
                workflow_type="ecosystem",
                complexity="complex",
                confidence=0.8
            ))
        
        # Backup ecosystem (backup + backupvault + snapshot)  
        backup_ops = [op for op in self.api_operations if op.resource_type in ['backup', 'backupvault', 'snapshot']]
        if backup_ops:
            cross_workflows.append(WorkflowPattern(
                workflow_id="backup_ecosystem_workflow",
                workflow_name="Data Protection Ecosystem Workflow",
                resource_type="backup_ecosystem",
                operations=backup_ops,
                workflow_type="ecosystem",
                complexity="complex",
                confidence=0.8
            ))
        
        return cross_workflows
    
    def _group_operations_by_resource(self) -> Dict[str, List[APIOperation]]:
        """Group operations by resource type."""
        grouped = defaultdict(list)
        for op in self.api_operations:
            grouped[op.resource_type].append(op)
        return dict(grouped)
    
    def _get_service_name(self, spec_path: Path) -> str:
        """Extract service name from spec path."""
        if "core-api" in str(spec_path):
            return "core-api"
        elif "google-proxy" in str(spec_path):
            return "google-proxy" 
        elif "telemetry" in str(spec_path):
            return "telemetry"
        return "unknown"
    
    def _extract_resource_type(self, path: str) -> str:
        """Extract resource type from API path with comprehensive patterns."""
        path_parts = [p for p in path.split('/') if p and not p.startswith('{')]
        
        # Comprehensive resource patterns
        resource_patterns = [
            'pools', 'volumes', 'snapshots', 'backups', 'backupVaults',
            'replications', 'hostGroups', 'activeDirectories', 'kmsConfig',
            'performance', 'usage', 'metrics', 'events', 'health', 'operations'
        ]
        
        for part in path_parts:
            for pattern in resource_patterns:
                if pattern.lower() in part.lower():
                    # Convert to singular and handle special cases
                    resource = pattern.rstrip('s').lower()
                    resource = resource.replace('groups', 'group')
                    resource = resource.replace('directories', 'directorie')
                    resource = resource.replace('vaults', 'vault')
                    return resource
        
        # Fallback extraction
        if path_parts:
            return path_parts[-1].lower()
        
        return "unknown"
    
    def _determine_operation_type(self, method: str, path: str) -> str:
        """Determine operation type from HTTP method and path."""
        method_map = {
            'GET': 'read' if '{' in path else 'list',
            'POST': 'create',
            'PUT': 'update',
            'DELETE': 'delete',
            'PATCH': 'update'
        }
        return method_map.get(method, 'unknown')
    
    def _assess_workflow_complexity(self, operations: List[APIOperation]) -> str:
        """Assess workflow complexity."""
        if len(operations) <= 2:
            return "simple"
        elif len(operations) <= 6:
            return "moderate"
        else:
            return "complex"
    
    def _get_api_spec_paths(self) -> List[Path]:
        """Get all API specification paths."""
        return [
            self.repo_root / "core" / "core-api" / "api.yaml",
            self.repo_root / "google-proxy" / "api" / "gcp-api.yaml",
            self.repo_root / "telemetry" / "api" / "telemetry-api.yaml"
        ]

class ExistingDocumentationAnalyzer:
    """Analyzes existing documentation for coverage analysis."""
    
    def __init__(self, repo_root: Path):
        self.repo_root = repo_root
    
    def analyze_existing_documentation(self) -> Dict[str, str]:
        """Analyze existing design documents."""
        existing_docs = {}
        designs_dir = self.repo_root / "doc" / "architecture" / "designs"
        
        if designs_dir.exists():
            for doc_file in designs_dir.glob("*.md"):
                try:
                    content = doc_file.read_text()
                    existing_docs[str(doc_file)] = content
                except Exception as e:
                    print(f"Warning: Could not read {doc_file}: {e}")
        
        return existing_docs
    
    def analyze_existing_docs(self) -> Dict[str, str]:
        """Alias for analyze_existing_documentation."""
        return self.analyze_existing_documentation()

class WorkflowCoverageAnalyzer:
    """Analyzes workflow coverage and determines documentation actions."""
    
    def __init__(self, api_analyzer=None, doc_analyzer=None):
        if isinstance(api_analyzer, Path):
            # If first argument is a Path, treat it as repo_root
            self.repo_root = api_analyzer
            self.existing_analyzer = ExistingDocumentationAnalyzer(api_analyzer)
        else:
            # New style with analyzers passed in
            self.api_analyzer = api_analyzer
            self.existing_analyzer = doc_analyzer or ExistingDocumentationAnalyzer(Path.cwd())
    
    def analyze_coverage(self, workflows: List[WorkflowPattern]) -> Dict[str, Dict[str, any]]:
        """Analyze coverage for all workflows."""
        existing_docs = self.existing_analyzer.analyze_existing_documentation()
        coverage_analysis = {}
        
        for workflow in workflows:
            analysis = self._analyze_workflow_coverage(workflow, existing_docs)
            coverage_analysis[workflow.workflow_id] = analysis
            
        return coverage_analysis
    
    def _analyze_workflow_coverage(self, workflow: WorkflowPattern, existing_docs: Dict[str, str]) -> Dict[str, any]:
        """Analyze coverage for a single workflow."""
        
        # Check if workflow is already covered
        is_covered = False
        existing_doc_path = None
        
        for doc_path, content in existing_docs.items():
            content_lower = content.lower()
            resource_lower = workflow.resource_type.lower()
            
            if (resource_lower in content_lower or 
                resource_lower in Path(doc_path).name.lower()):
                is_covered = True
                existing_doc_path = doc_path
                break
        
        # Determine action and priority
        if is_covered:
            action = "update"
            priority = "medium"
        else:
            action = "create"  
            priority = "medium"
        
        return {
            'covered': is_covered,
            'action': action,
            'priority': priority,
            'existing_doc': existing_doc_path,
            'confidence': workflow.confidence
        }
    
    def analyze_coverage_gaps(self) -> Dict[str, Dict[str, any]]:
        """Analyze coverage gaps for workflows."""
        if hasattr(self, 'api_analyzer') and self.api_analyzer:
            workflows = self.api_analyzer.workflow_patterns
        else:
            workflows = []
        return self.analyze_coverage(workflows)
    
    def print_coverage_summary(self):
        """Print coverage summary."""
        print("Coverage analysis completed.")
        # Add more detailed summary if needed
    
    def get_actionable_workflows(self) -> List[WorkflowPattern]:
        """Get workflows that need documentation action."""
        if hasattr(self, 'api_analyzer') and self.api_analyzer:
            return self.api_analyzer.workflow_patterns
        else:
            return []
    
    def _scan_workflow_files_for_discovery(self) -> Dict[str, List[str]]:
        """Scan workflow Go files to discover ALL resources dynamically (GENERIC)"""
        workflow_dir = self.repo_root / "core" / "orchestrator" / "workflows"
        
        if not workflow_dir.exists():
            return {}
        
        resource_workflows = defaultdict(list)
        
        # Scan all workflow files
        for workflow_file in workflow_dir.glob("*.go"):
            if workflow_file.name.endswith("_test.go"):
                continue
            
            # Extract resource name from filename
            filename = workflow_file.stem
            resource_name = self._extract_resource_from_workflow_filename(filename)
            
            if not resource_name:
                continue
            
            # Find workflow structs in file
            content = workflow_file.read_text()
            workflow_pattern = r'type\s+(\w+Workflow)\s+struct'
            
            for match in re.finditer(workflow_pattern, content):
                workflow_name = match.group(1)
                resource_workflows[resource_name].append(workflow_name)
        
        return dict(resource_workflows)
    
    def _extract_resource_from_workflow_filename(self, filename: str) -> str:
        """Extract resource name from workflow filename (GENERIC)"""
        # Remove common suffixes
        name = filename.replace('_workflow', '').replace('_workflows', '')
        
        # Remove operation suffixes
        for suffix in ['_create', '_delete', '_update', '_restore', '_rotate', '_migrate', '_revert', '_refresh']:
            name = name.replace(suffix, '')
        
        # Handle compound names
        if '_' in name:
            parts = name.split('_')
            
            # Common patterns - use first meaningful part
            priority_resources = ['backup', 'pool', 'volume', 'snapshot', 'replication', 
                                'kms', 'hostgroup', 'cluster', 'adc']
            
            for resource in priority_resources:
                if resource in parts:
                    return resource
            
            # Default: use first part
            return parts[0]
        
        return name
    
    def get_actionable_workflows(self) -> List[WorkflowPattern]:
        """Get workflows that need action."""
        if hasattr(self, 'api_analyzer') and self.api_analyzer:
            return self.api_analyzer.workflow_patterns
        else:
            return []