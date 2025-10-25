#!/usr/bin/env python3
"""
GitHub Copilot Integration for VSA Control Plane Documentation Generation

Uses GitHub Copilot CLI and API to generate intelligent, contextual documentation
by analyzing actual codebase patterns and workflow implementations.
"""

import subprocess
import json
import re
from pathlib import Path
from typing import Optional, Dict, List
from dataclasses import dataclass


@dataclass
class CopilotResponse:
    """Structured response from GitHub Copilot."""
    content: str
    confidence: float
    model_used: str


class GitHubCopilotDocGenerator:
    """
    Leverages GitHub Copilot for intelligent documentation generation.
    
    Features:
    - Contextual architecture descriptions from actual code
    - Accurate Mermaid diagrams generated from workflow implementations
    - ADR-style architectural decision documentation
    - Code-aware troubleshooting guides
    """
    
    def __init__(self, repo_root: Path):
        self.repo_root = Path(repo_root)
        self._verify_gh_copilot_available()
    
    def _verify_gh_copilot_available(self):
        """Check if GitHub Copilot CLI is installed and authenticated."""
        try:
            result = subprocess.run(
                ['gh', 'copilot', '--version'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode != 0:
                print("⚠️  GitHub Copilot CLI not available. Install with: gh extension install github/gh-copilot")
                self.available = False
            else:
                print("✅ GitHub Copilot CLI detected")
                self.available = True
        except (subprocess.TimeoutExpired, FileNotFoundError):
            print("⚠️  GitHub CLI not found. Install from https://cli.github.com/")
            self.available = False
    
    def generate_architecture_description(
        self, 
        workflow_name: str, 
        resource_type: str,
        operations: List[str],
        code_context: str
    ) -> Optional[str]:
        """
        Generate comprehensive architecture description using Copilot.
        
        Args:
            workflow_name: Name of the workflow (e.g., "Volume Management")
            resource_type: Resource type (e.g., "volume", "pool")
            operations: List of operation IDs
            code_context: Relevant code snippets for context
        
        Returns:
            Generated architecture description in markdown format
        """
        if not self.available:
            return None
        
        prompt = self._build_architecture_prompt(
            workflow_name, resource_type, operations, code_context
        )
        
        return self._query_copilot(prompt, query_type="architecture")
    
    def generate_workflow_diagram(self, workflow_file: Path) -> Optional[str]:
        """
        Generate Mermaid sequence diagram from actual workflow code.
        
        Analyzes the Go workflow implementation to create accurate diagrams
        showing activity execution order, error handling, and compensation.
        
        Args:
            workflow_file: Path to workflow .go file
        
        Returns:
            Mermaid diagram syntax as string
        """
        if not self.available or not workflow_file.exists():
            return None
        
        code = workflow_file.read_text()
        
        # Extract key workflow method
        workflow_code = self._extract_workflow_run_method(code)
        
        prompt = f"""
Analyze this Go Temporal workflow code and generate a detailed Mermaid sequence diagram.

Workflow Implementation:
```go
{workflow_code[:4000]}  # Limit context
```

Generate a Mermaid sequence diagram that shows:
1. All ExecuteActivity calls in order
2. Error handling and retry logic
3. Workflow decision points (if/switch statements)
4. External service interactions (GCP, ONTAP, Database)
5. Success and failure paths

Output only valid Mermaid syntax starting with 'sequenceDiagram'.
"""
        
        response = self._query_copilot(prompt, query_type="diagram")
        
        if response:
            return self._extract_mermaid_syntax(response)
        return None
    
    def generate_adr_content(
        self, 
        decision_topic: str,
        code_evidence: List[str],
        existing_docs: List[str]
    ) -> Optional[str]:
        """
        Generate ADR (Architecture Decision Record) content based on code patterns.
        
        Args:
            decision_topic: Topic of the architectural decision
            code_evidence: Code snippets showing the implementation
            existing_docs: Related existing documentation
        
        Returns:
            ADR-formatted content with context, decision, and consequences
        """
        if not self.available:
            return None
        
        prompt = f"""
Generate an Architecture Decision Record (ADR) for the VSA Control Plane.

Decision Topic: {decision_topic}

Code Evidence:
{chr(10).join(code_evidence[:3])}

Related Documentation Context:
{chr(10).join(existing_docs[:2])}

Generate ADR content with these sections:
1. **Context**: What is the problem/requirement?
2. **Decision**: What did we decide and why?
3. **Consequences**: What are the implications (positive and negative)?
4. **Alternatives Considered**: What other options were evaluated?

Focus on technical accuracy based on the code evidence provided.
Format as markdown.
"""
        
        return self._query_copilot(prompt, query_type="adr")
    
    def enhance_api_operation_description(
        self, 
        operation_id: str,
        api_path: str,
        method: str,
        related_code: Optional[str] = None
    ) -> Optional[str]:
        """
        Generate detailed operation descriptions using code context.
        
        Args:
            operation_id: OpenAPI operation ID
            api_path: API endpoint path
            method: HTTP method
            related_code: Related handler/workflow code
        
        Returns:
            Enhanced operation description
        """
        if not self.available:
            return None
        
        prompt = f"""
For the VSA Control Plane API operation:
- Operation: {operation_id}
- Endpoint: {method} {api_path}

{f"Implementation Context:{chr(10)}{related_code[:1500]}" if related_code else ""}

Generate a concise, technical description (2-3 sentences) that explains:
1. What this operation does
2. Key parameters and their purpose
3. Expected behavior and side effects

Focus on storage management context (ONTAP, VSA clusters, volumes, snapshots, etc.).
"""
        
        return self._query_copilot(prompt, query_type="operation")
    
    def generate_troubleshooting_guide(
        self, 
        workflow_name: str,
        error_handling_code: str,
        common_errors: List[str]
    ) -> Optional[str]:
        """
        Generate troubleshooting guide based on error handling code.
        
        Args:
            workflow_name: Name of the workflow
            error_handling_code: Error handling code from workflows
            common_errors: List of common error patterns
        
        Returns:
            Troubleshooting guide in markdown format
        """
        if not self.available:
            return None
        
        prompt = f"""
Generate a troubleshooting guide for the {workflow_name} workflow in VSA Control Plane.

Error Handling Implementation:
```go
{error_handling_code[:2000]}
```

Common Error Patterns:
{chr(10).join(f"- {err}" for err in common_errors[:5])}

Create a troubleshooting guide with:
1. **Common Issues**: List typical failure scenarios
2. **Diagnostic Steps**: How to identify the problem
3. **Resolution**: How to fix each issue
4. **Prevention**: Best practices to avoid the issue

Format as markdown with clear sections.
"""
        
        return self._query_copilot(prompt, query_type="troubleshooting")
    
    def _build_architecture_prompt(
        self, 
        workflow_name: str,
        resource_type: str,
        operations: List[str],
        code_context: str
    ) -> str:
        """Build comprehensive prompt for architecture description."""
        return f"""
Generate a comprehensive architecture description for the VSA Control Plane {workflow_name}.

Resource Type: {resource_type}
Number of Operations: {len(operations)}
Key Operations: {', '.join(operations[:10])}

Code Context:
```go
{code_context[:3000]}
```

Generate architecture documentation with:

1. **System Purpose**: What does this workflow accomplish?
2. **Key Components**: What services/modules are involved?
3. **Architectural Decisions**: 
   - Why was this design chosen?
   - What are the key patterns (e.g., Temporal workflows, database transactions)?
   - How does it integrate with ONTAP and GCP?
4. **Data Flow**: How does data move through the system?
5. **Performance Considerations**: Scalability, timeouts, retry logic
6. **Security Considerations**: Authentication, authorization, encryption

Focus on technical accuracy and operational insights.
Format as markdown with clear sections.
"""
    
    def _query_copilot(self, prompt: str, query_type: str = "general") -> Optional[str]:
        """
        Query GitHub Copilot CLI with prompt.
        
        Args:
            prompt: The prompt to send
            query_type: Type of query for logging
        
        Returns:
            Copilot response or None if failed
        """
        try:
            # Use 'gh copilot suggest' for better responses
            result = subprocess.run(
                ['gh', 'copilot', 'suggest', prompt],
                capture_output=True,
                text=True,
                timeout=30,
                input='\n'  # Auto-accept the first suggestion
            )
            
            if result.returncode == 0:
                response = result.stdout.strip()
                print(f"✅ Generated {query_type} content via Copilot ({len(response)} chars)")
                return response
            else:
                print(f"⚠️  Copilot query failed: {result.stderr}")
                return None
                
        except subprocess.TimeoutExpired:
            print(f"⚠️  Copilot query timed out for {query_type}")
            return None
        except Exception as e:
            print(f"⚠️  Copilot query error: {e}")
            return None
    
    def _extract_workflow_run_method(self, code: str) -> str:
        """Extract the main Run() method from workflow code."""
        # Find the Run method
        match = re.search(
            r'func\s+\([^)]+\)\s+Run\s*\([^)]+\)[^{]*{([^}]+(?:{[^}]*}[^}]*)*)}',
            code,
            re.DOTALL
        )
        
        if match:
            return match.group(0)
        
        # Fallback: return first 2000 chars
        return code[:2000]
    
    def _extract_mermaid_syntax(self, response: str) -> str:
        """Extract Mermaid diagram syntax from Copilot response."""
        # Look for mermaid code block
        match = re.search(
            r'```mermaid\n(.*?)\n```',
            response,
            re.DOTALL
        )
        
        if match:
            return match.group(1).strip()
        
        # Look for sequenceDiagram directly
        match = re.search(
            r'(sequenceDiagram.*?)(?:\n\n|\Z)',
            response,
            re.DOTALL
        )
        
        if match:
            return match.group(1).strip()
        
        # Return as-is if no pattern found
        return response


class CopilotCodeContextExtractor:
    """Extract relevant code context for Copilot prompts."""
    
    def __init__(self, repo_root: Path):
        self.repo_root = Path(repo_root)
    
    def get_workflow_code(self, resource_type: str) -> Optional[str]:
        """
        Find and extract workflow code for a resource type.
        
        Args:
            resource_type: Resource type (e.g., "volume", "pool", "backup")
        
        Returns:
            Workflow code snippet or None
        """
        workflow_dir = self.repo_root / "core" / "orchestrator" / "workflows"
        
        if not workflow_dir.exists():
            return None
        
        # Find matching workflow file
        pattern = f"*{resource_type}*.go"
        matching_files = list(workflow_dir.glob(pattern))
        
        if matching_files:
            # Read first matching file
            code = matching_files[0].read_text()
            # Return relevant portion (first 4000 chars)
            return code[:4000]
        
        return None
    
    def get_activity_code(self, resource_type: str) -> Optional[str]:
        """
        Find and extract activity code for a resource type.
        
        Args:
            resource_type: Resource type
        
        Returns:
            Activity code snippet or None
        """
        activity_dir = self.repo_root / "core" / "orchestrator" / "activities"
        
        if not activity_dir.exists():
            return None
        
        pattern = f"*{resource_type}*.go"
        matching_files = list(activity_dir.glob(pattern))
        
        if matching_files:
            code = matching_files[0].read_text()
            return code[:4000]
        
        return None
    
    def get_model_definitions(self, resource_type: str) -> Optional[str]:
        """
        Extract data model definitions for a resource type.
        
        Args:
            resource_type: Resource type
        
        Returns:
            Model code snippet or None
        """
        datamodel_dir = self.repo_root / "core" / "datamodel"
        
        if not datamodel_dir.exists():
            return None
        
        # Try to find matching model file
        model_file = datamodel_dir / f"{resource_type}.go"
        
        if model_file.exists():
            return model_file.read_text()[:2000]
        
        # Fallback: check models.go
        models_file = datamodel_dir / "models.go"
        if models_file.exists():
            code = models_file.read_text()
            # Extract relevant struct definition
            pattern = rf'type\s+{resource_type.title()}[^{{]*{{[^}}]*}}'
            match = re.search(pattern, code, re.IGNORECASE | re.DOTALL)
            if match:
                return match.group(0)
        
        return None


# Example usage
if __name__ == "__main__":
    repo_root = Path(__file__).parent.parent
    
    # Initialize Copilot integration
    copilot = GitHubCopilotDocGenerator(repo_root)
    context_extractor = CopilotCodeContextExtractor(repo_root)
    
    # Example: Generate architecture description for volume workflow
    workflow_code = context_extractor.get_workflow_code("volume")
    if workflow_code:
        description = copilot.generate_architecture_description(
            workflow_name="Volume Management",
            resource_type="volume",
            operations=["create_volume", "delete_volume", "update_volume"],
            code_context=workflow_code
        )
        
        if description:
            print("Generated Architecture Description:")
            print(description)
    
    # Example: Generate diagram from workflow file
    workflow_files = list((repo_root / "core" / "orchestrator" / "workflows").glob("*volume*.go"))
    if workflow_files:
        diagram = copilot.generate_workflow_diagram(workflow_files[0])
        if diagram:
            print("\nGenerated Mermaid Diagram:")
            print(diagram)
