#!/usr/bin/env python3
"""
Smart VSA Control Plane Documentation Generator
Updates existing docs and creates only missing ones with proper grouping.
"""

import sys
from pathlib import Path
from datetime import datetime
import re
sys.path.insert(0, str(Path(__file__).parent))

from api_workflow_analyzer import APIOperationAnalyzer

class SmartDocumentationAnalyzer:
    """Analyzes existing documentation and identifies gaps."""
    
    def __init__(self, repo_root):
        self.repo_root = Path(repo_root)
        # Auto-generated docs go in separate folder to preserve manual designs
        self.auto_gen_dir = repo_root / "doc" / "architecture" / "auto-gen-designs-docs"
        self.manual_designs_dir = repo_root / "doc" / "architecture" / "designs"
        self.existing_docs = {}
        self.analyze_existing_docs()
        
    def analyze_existing_docs(self):
        """Analyze existing documentation coverage ONLY from auto-generated docs.
        
        Manual designs are intentionally ignored - we want to generate all docs
        in auto-gen directory regardless of what exists in manual designs.
        """
        # Only check auto-generated docs for gaps
        all_docs = []
        
        if self.auto_gen_dir.exists():
            # Exclude README.md and other meta files
            all_docs.extend([f for f in self.auto_gen_dir.glob("*.md") 
                           if f.name not in ['README.md', 'INDEX.md', '.gitkeep']])
        
        # Explicitly NOT checking manual_designs_dir - we want to generate all docs
        
        if not all_docs:
            return
            
        for doc_file in all_docs:
            content = doc_file.read_text().lower()
            
            # Determine what this doc covers (only comprehensive coverage, not just references)
            coverage = []
            
            # Only count as comprehensive coverage if it's the main focus
            if 'pool management' in content or 'pool lifecycle' in content:
                coverage.append('pool_comprehensive')
            elif 'pool' in content and len(content.split('pool')) > 5:  # More than just references
                coverage.append('pool_references')
                
            if 'volume management' in content or 'volume lifecycle' in content:
                coverage.append('volume_comprehensive')
            elif 'volume' in content and ('flexcache' in content or len(content.split('volume')) > 10):
                coverage.append('volume_references')
                
            if 'backup' in content and 'vault' not in doc_file.name and ('backup design' in content or 'backup architecture' in content):
                coverage.append('backup_comprehensive')
            if 'backup' in content and 'vault' in doc_file.name and ('backup vault design' in content):
                coverage.append('backup_vault_comprehensive')
            if 'flexcache' in content:
                coverage.append('flexcache_volumes')
            if 'auto-tiering' in content or 'autotiering' in content:
                coverage.append('auto_tiering')
                
            if coverage:
                self.existing_docs[doc_file.name] = {
                    'path': doc_file,
                    'coverage': coverage,
                    'comprehensive': 'comprehensive' in ' '.join(coverage)
                }
    
    def get_coverage_status(self):
        """Get coverage status for main workflows.
        
        Since we only check auto-gen directory, all docs should be generated
        unless they already exist in auto-gen directory.
        """
        workflows = {
            'pool_management': {
                'covered': any('pool_comprehensive' in cov for doc in self.existing_docs.values() for cov in doc['coverage']),
                'comprehensive': any('pool_comprehensive' in cov for doc in self.existing_docs.values() for cov in doc['coverage']),
                'existing_doc': None
            },
            'volume_management': {
                'covered': any('volume_comprehensive' in cov or 'flexcache' in cov for doc in self.existing_docs.values() for cov in doc['coverage']),
                'comprehensive': any('volume_comprehensive' in cov for doc in self.existing_docs.values() for cov in doc['coverage']),
                'existing_doc': '0002-flexcache-volumes.md' if '0002-flexcache-volumes.md' in self.existing_docs else None
            },
            'backup_management': {
                'covered': any('backup_comprehensive' in cov for doc in self.existing_docs.values() for cov in doc['coverage']),
                'comprehensive': any('backup_comprehensive' in cov for doc in self.existing_docs.values() for cov in doc['coverage']),
                'existing_doc': None
            },
            'backup_vault_management': {
                'covered': any('backup_vault_comprehensive' in cov for doc in self.existing_docs.values() for cov in doc['coverage']),
                'comprehensive': any('backup_vault_comprehensive' in cov for doc in self.existing_docs.values() for cov in doc['coverage']),
                'existing_doc': None
            },
            'kms_configuration': {
                'covered': False,
                'comprehensive': False,
                'existing_doc': None
            },
            'volume_replication': {
                'covered': False,
                'comprehensive': False,
                'existing_doc': None
            },
            'host_group_management': {
                'covered': False,
                'comprehensive': False,
                'existing_doc': None
            },
            'active_directory_integration': {
                'covered': False,
                'comprehensive': False,
                'existing_doc': None
            }
        }
        
        return workflows

def create_grouped_workflows(analyzer):
    """Create workflows - GENERIC VERSION - one doc per resource."""
    
    workflows = analyzer.analyze_workflows()
    
    # GENERIC APPROACH: Create one workflow group per discovered resource
    # No hardcoded merging - each resource gets its own documentation
    grouped_workflows = {}
    
    # Create a workflow group for each discovered resource
    for workflow in workflows:
        resource_type = workflow.resource_type.lower()
        workflow_key = f"{resource_type}_management"
        
        # Skip very generic/utility resources that don't need separate docs
        skip_resources = ['count', 'health', 'operation', 'authorize', 'mount', 'release', 
                         'resume', 'reverse', 'stop', 'getmultiple']
        
        if resource_type in skip_resources or resource_type.startswith('getmultiple'):
            continue
        
        # Create new workflow group if it doesn't exist
        if workflow_key not in grouped_workflows:
            # Generate human-readable name
            name = ' '.join(word.capitalize() for word in resource_type.split('_'))
            if not name.endswith('Management'):
                name = f"{name} Management"
            
            # Generate filename
            filename = f"{resource_type.replace('_', '-')}-design.md"
            
            # Create description
            description = f"{name} lifecycle and operations"
            
            grouped_workflows[workflow_key] = {
                'name': name,
                'filename': filename,
                'description': description,
                'main_operations': [],
                'advanced_features': [],
                'foundational': len(workflow.operations) > 5,  # Heuristic: more ops = foundational
                'includes': [f'{resource_type} lifecycle']
            }
        
        # Add operations to this workflow group
        grouped_workflows[workflow_key]['main_operations'].extend(workflow.operations)
    
    return grouped_workflows

def main():
    """Smart documentation generation with gap analysis."""
    
    print("🤖 Smart VSA Control Plane Documentation Generator")
    print("=" * 60)
    
    repo_root = Path(__file__).parent.parent
    doc_analyzer = SmartDocumentationAnalyzer(repo_root)
    api_analyzer = APIOperationAnalyzer(repo_root)
    
    print("\n📋 Analyzing existing documentation coverage...")
    coverage_status = doc_analyzer.get_coverage_status()
    
    print("\n📊 Coverage Analysis:")
    for workflow, status in coverage_status.items():
        status_icon = "✅" if status['comprehensive'] else "⚠️" if status['covered'] else "❌"
        existing = f" (exists: {status['existing_doc']})" if status['existing_doc'] else ""
        print(f"   {status_icon} {workflow.replace('_', ' ').title()}{existing}")
    
    print(f"\n🔍 Creating grouped workflows...")
    grouped_workflows = create_grouped_workflows(api_analyzer)
    
    print(f"\n📝 Documentation Plan:")
    
    actions = []
    for key, workflow in grouped_workflows.items():
        # Get status if it exists, otherwise assume not covered
        status = coverage_status.get(key, {
            'covered': False,
            'comprehensive': False,
            'existing_doc': None
        })
        
        main_ops = len(workflow['main_operations'])
        advanced_ops = len(workflow['advanced_features'])
        
        if not status['covered']:
            actions.append(('CREATE', workflow['name'], workflow['filename'], main_ops + advanced_ops))
        elif not status['comprehensive'] and (main_ops > 0 or advanced_ops > 0):
            actions.append(('ENHANCE', workflow['name'], status['existing_doc'], main_ops + advanced_ops))
        else:
            actions.append(('SKIP', workflow['name'], status['existing_doc'], main_ops + advanced_ops))
    
    # Show plan
    for action, name, filename, ops in actions:
        if action == 'CREATE':
            print(f"   🆕 CREATE: {name} ({ops} operations) → {filename}")
        elif action == 'ENHANCE': 
            print(f"   🔄 ENHANCE: {name} ({ops} operations) → {filename}")
        else:
            print(f"   ✅ SKIP: {name} (already comprehensive) → {filename}")
    
    print(f"\n🎯 Recommendation:")
    create_count = sum(1 for action, _, _, _ in actions if action == 'CREATE')
    enhance_count = sum(1 for action, _, _, _ in actions if action == 'ENHANCE')
    skip_count = sum(1 for action, _, _, _ in actions if action == 'SKIP')
    
    print(f"   • Create {create_count} new comprehensive documents")
    print(f"   • Enhance {enhance_count} existing documents with missing operations")
    print(f"   • Skip {skip_count} already comprehensive documents")
    print(f"   • Total: {create_count + enhance_count} documents to work on")
    
    return actions, grouped_workflows

if __name__ == "__main__":
    try:
        actions, workflows = main()
        # CI/CD friendly exit
        create_count = sum(1 for action, _, _, _ in actions if action == 'CREATE')
        enhance_count = sum(1 for action, _, _, _ in actions if action == 'ENHANCE')
        
        if create_count > 0 or enhance_count > 0:
            print(f"\n✅ Analysis complete: {create_count + enhance_count} documents need work")
        else:
            print(f"\n✅ Analysis complete: All documentation is up to date")
            
    except Exception as e:
        print(f"\n❌ Analysis failed: {e}")
        exit(1)