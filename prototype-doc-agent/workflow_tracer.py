#!/usr/bin/env python3
"""
Workflow Execution Tracer - Generate Detailed Sequence Diagrams

Traces workflow execution flow and generates comprehensive Mermaid sequence diagrams
showing interactions between Client, GoogleProxy, Orchestrator, Workflow, Activities,
ONTAP, and GCP services.
"""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Optional, Set, Tuple
from go_code_analyzer import GoCodeAnalyzer, WorkflowAnalysis, ActivityCall, ActivityDefinition


@dataclass
class SequenceStep:
    """Represents a single step in a sequence diagram"""
    step_type: str  # "activity", "conditional", "loop", "note", "external_call"
    from_component: str
    to_component: str
    action: str
    response: Optional[str] = None
    condition: Optional[str] = None  # For alt/opt blocks
    line_number: int = 0


class WorkflowExecutionTracer:
    """Generate detailed sequence diagrams from workflow analysis"""
    
    def __init__(self, analyzer: GoCodeAnalyzer):
        self.analyzer = analyzer
        
        # Component mappings for sequence diagrams
        self.component_map = {
            'client': 'Client',
            'google_proxy': 'GoogleProxy',
            'orchestrator': 'Orchestrator',
            'workflow': 'Workflow',
            'activities': 'Activities',
            'ontap': 'ONTAP',
            'gcp': 'GCP',
            'database': 'Database',
            'sde': 'SDE'
        }
    
    def generate_sequence_diagram(self, workflow_analysis: WorkflowAnalysis, 
                                  include_api_call: bool = True) -> str:
        """
        Generate comprehensive Mermaid sequence diagram for workflow.
        
        Args:
            workflow_analysis: Analyzed workflow data
            include_api_call: Whether to include initial API call steps
        
        Returns:
            Mermaid sequence diagram string
        """
        steps = self._build_sequence_steps(workflow_analysis)
        
        # Start diagram
        lines = ['```mermaid', 'sequenceDiagram']
        
        # Add participants
        participants = self._determine_participants(steps)
        for participant in participants:
            lines.append(f'    participant {participant}')
        
        lines.append('')
        
        # Add initial API call flow if requested
        if include_api_call:
            lines.extend(self._generate_api_call_flow(workflow_analysis))
            lines.append('')
        
        # Add workflow execution steps
        lines.extend(self._generate_workflow_steps(steps, workflow_analysis))
        
        # Add final completion flow
        if include_api_call:
            lines.extend(self._generate_completion_flow(workflow_analysis))
        
        lines.append('```')
        
        return '\n'.join(lines)
    
    def _build_sequence_steps(self, workflow_analysis: WorkflowAnalysis) -> List[SequenceStep]:
        """Build sequence of execution steps from workflow analysis."""
        steps = []
        
        for call in workflow_analysis.activity_calls:
            # Get activity definition to understand what it does
            activity_def = self.analyzer.get_activity_for_call(call)
            
            # Determine target component based on activity name
            target_component = self._infer_target_component(call.activity_name, activity_def)
            
            # Create step
            step = SequenceStep(
                step_type='activity',
                from_component='Workflow',
                to_component='Activities',
                action=self._format_activity_call(call, activity_def),
                response=self._infer_response(call, activity_def),
                line_number=call.line_number
            )
            
            steps.append(step)
            
            # If activity interacts with external system, add that interaction
            if target_component and target_component not in ['Activities', 'Workflow']:
                external_step = SequenceStep(
                    step_type='external_call',
                    from_component='Activities',
                    to_component=target_component,
                    action=self._infer_external_action(call.activity_name),
                    response=self._infer_external_response(call.activity_name),
                    line_number=call.line_number
                )
                steps.append(external_step)
                
                # Response back to activities
                response_step = SequenceStep(
                    step_type='activity',
                    from_component='Activities',
                    to_component='Workflow',
                    action='',
                    response=self._infer_response(call, activity_def),
                    line_number=call.line_number
                )
                steps.append(response_step)
        
        return steps
    
    def _determine_participants(self, steps: List[SequenceStep]) -> List[str]:
        """Determine which participants to include in diagram."""
        participants_set = set()
        
        # Always include core components
        participants_set.add('Client')
        participants_set.add('GoogleProxy')
        participants_set.add('Orchestrator')
        participants_set.add('Workflow')
        participants_set.add('Activities')
        
        # Add components based on steps
        for step in steps:
            if step.to_component in ['ONTAP', 'GCP', 'Database', 'SDE']:
                participants_set.add(step.to_component)
        
        # Return in logical order
        order = ['Client', 'GoogleProxy', 'Orchestrator', 'Workflow', 'Activities', 
                'ONTAP', 'GCP', 'Database', 'SDE']
        
        return [p for p in order if p in participants_set]
    
    def _infer_target_component(self, activity_name: str, activity_def: Optional[ActivityDefinition]) -> Optional[str]:
        """Infer which external component the activity interacts with."""
        activity_lower = activity_name.lower()
        
        # ONTAP operations
        ontap_keywords = ['snapshot', 'volume', 'snapmirror', 'objectstore', 'cloudtarget', 
                         'endpoint', 'svm', 'aggregate', 'lif', 'ontap']
        if any(kw in activity_lower for kw in ontap_keywords):
            return 'ONTAP'
        
        # GCP operations
        gcp_keywords = ['gcp', 'gcs', 'bucket', 'compute', 'disk', 'instance', 'cloud']
        if any(kw in activity_lower for kw in gcp_keywords):
            return 'GCP'
        
        # Database operations
        db_keywords = ['get', 'create', 'update', 'delete', 'find', 'list', 'save']
        if any(activity_lower.startswith(kw) for kw in db_keywords):
            return 'Database'
        
        # SDE operations
        if 'sde' in activity_lower:
            return 'SDE'
        
        return None
    
    def _format_activity_call(self, call: ActivityCall, activity_def: Optional[ActivityDefinition]) -> str:
        """Format activity call for display in sequence diagram."""
        # Use docstring if available
        if activity_def and activity_def.docstring:
            return activity_def.name
        
        # Otherwise format based on name
        name = call.activity_name.replace('Activity', '')
        # Convert CamelCase to words
        formatted = re.sub(r'([A-Z])', r' \1', name).strip()
        return formatted
    
    def _infer_response(self, call: ActivityCall, activity_def: Optional[ActivityDefinition]) -> str:
        """Infer what the activity returns."""
        if call.output_var:
            # Format output variable name
            if 'error' in call.output_var.lower():
                return "Error or Success"
            elif 'vault' in call.output_var.lower():
                return "Vault Retrieved"
            elif 'volume' in call.output_var.lower():
                return "Volume Retrieved"
            elif 'backup' in call.output_var.lower():
                return "Backup Retrieved"
            elif 'snapshot' in call.output_var.lower():
                return "Snapshot Retrieved"
            else:
                return f"{call.output_var} Retrieved"
        
        # Use activity name to infer
        name_lower = call.activity_name.lower()
        if 'create' in name_lower:
            return "Created Successfully"
        elif 'delete' in name_lower:
            return "Deleted Successfully"
        elif 'update' in name_lower:
            return "Updated Successfully"
        elif 'get' in name_lower or 'find' in name_lower:
            return "Data Retrieved"
        
        return "Operation Complete"
    
    def _infer_external_action(self, activity_name: str) -> str:
        """Infer what action is performed on external system."""
        name_lower = activity_name.lower()
        
        if 'create' in name_lower and 'snapshot' in name_lower:
            return "CreateSnapshot()"
        elif 'delete' in name_lower and 'snapshot' in name_lower:
            return "DeleteSnapshot()"
        elif 'snapmirror' in name_lower:
            if 'initialize' in name_lower:
                return "InitializeSnapMirror()"
            elif 'delete' in name_lower:
                return "DeleteSnapMirror()"
            else:
                return "UpdateSnapMirror()"
        elif 'objectstore' in name_lower or 'object_store' in name_lower:
            if 'create' in name_lower:
                return "CreateObjectStore()"
            elif 'delete' in name_lower:
                return "DeleteObjectStore()"
        elif 'transfer' in name_lower:
            if 'start' in name_lower:
                return "StartBackupTransfer()"
            elif 'poll' in name_lower:
                return "PollTransfer()"
        
        # Generic actions
        if 'create' in name_lower:
            return "Create()"
        elif 'delete' in name_lower:
            return "Delete()"
        elif 'update' in name_lower:
            return "Update()"
        elif 'get' in name_lower:
            return "Get()"
        
        return "ExecuteOperation()"
    
    def _infer_external_response(self, activity_name: str) -> str:
        """Infer response from external system."""
        name_lower = activity_name.lower()
        
        if 'create' in name_lower:
            if 'snapshot' in name_lower:
                return "Snapshot Created"
            elif 'objectstore' in name_lower:
                return "Object Store Created"
            return "Resource Created"
        elif 'delete' in name_lower:
            return "Resource Deleted"
        elif 'snapmirror' in name_lower:
            if 'initialize' in name_lower:
                return "SnapMirror Initialized"
            return "SnapMirror Updated"
        elif 'transfer' in name_lower:
            if 'start' in name_lower:
                return "Transfer Started"
            elif 'poll' in name_lower or 'complete' in name_lower:
                return "Transfer Complete"
        elif 'get' in name_lower:
            return "Data Retrieved"
        
        return "Operation Complete"
    
    def _generate_api_call_flow(self, workflow_analysis: WorkflowAnalysis) -> List[str]:
        """Generate initial API call flow."""
        resource = workflow_analysis.resource_type.title()
        operation = workflow_analysis.workflow_name.replace('_', ' ').title()
        
        lines = [
            f'    Client->>GoogleProxy: POST /{workflow_analysis.resource_type}s',
            f'    GoogleProxy->>Orchestrator: {operation}()',
            f'    Orchestrator->>Workflow: ExecuteWorkflow()',
            '',
            '    Workflow->>Workflow: Setup(ctx, params)',
            '    Workflow->>Activities: UpdateJobStatus(PROCESSING)',
            '    Activities-->>Workflow: Job Status Updated',
        ]
        
        return lines
    
    def _generate_workflow_steps(self, steps: List[SequenceStep], 
                                 workflow_analysis: WorkflowAnalysis) -> List[str]:
        """Generate workflow execution steps with conditional logic."""
        lines = []
        
        # Track conditional blocks
        conditional_depth = 0
        last_conditional = None
        
        for i, step in enumerate(steps):
            # Check if we're entering a conditional block
            if workflow_analysis.conditional_branches:
                for branch in workflow_analysis.conditional_branches:
                    if step.line_number >= branch['line_number'] and last_conditional != branch:
                        # Start conditional block
                        if branch['type'] == 'if':
                            condition = self._simplify_condition(branch['condition'])
                            lines.append(f'    alt {condition}')
                            conditional_depth += 1
                            last_conditional = branch
                        break
            
            # Generate step
            if step.step_type == 'activity':
                if step.from_component == 'Workflow' and step.to_component == 'Activities':
                    lines.append(f'    Workflow->>Activities: {step.action}')
                elif step.from_component == 'Activities' and step.to_component == 'Workflow':
                    if step.response:
                        lines.append(f'    Activities-->>Workflow: {step.response}')
                else:
                    lines.append(f'    {step.from_component}->>{step.to_component}: {step.action}')
                    if step.response:
                        lines.append(f'    {step.to_component}-->>{step.from_component}: {step.response}')
            
            elif step.step_type == 'external_call':
                lines.append(f'    {step.from_component}->>{step.to_component}: {step.action}')
                if step.response:
                    lines.append(f'    {step.to_component}-->>{step.from_component}: {step.response}')
        
        # Close conditional blocks
        while conditional_depth > 0:
            lines.append('    end')
            conditional_depth -= 1
        
        return lines
    
    def _generate_completion_flow(self, workflow_analysis: WorkflowAnalysis) -> List[str]:
        """Generate workflow completion flow."""
        lines = [
            '',
            '    Workflow->>Activities: UpdateJobStatus(DONE)',
            '    Activities-->>Workflow: Job Status Updated',
            '',
            '    Workflow-->>Orchestrator: Workflow Complete',
            '    Orchestrator-->>GoogleProxy: Operation Complete',
            '    GoogleProxy-->>Client: 201 Created',
        ]
        
        return lines
    
    def _simplify_condition(self, condition: str) -> str:
        """Simplify condition for display in diagram."""
        # Remove common prefixes
        condition = condition.replace('params.', '')
        condition = condition.replace('input.', '')
        condition = condition.replace('context.', '')
        
        # Shorten if too long
        if len(condition) > 50:
            condition = condition[:47] + '...'
        
        return condition
    
    def generate_create_workflow_diagram(self, workflow_name: str) -> str:
        """
        Generate sequence diagram specifically for create workflows.
        Includes common patterns like validation, resource creation, state updates.
        """
        # This is a template for create workflows
        return f'''```mermaid
sequenceDiagram
    participant Client
    participant GoogleProxy
    participant Orchestrator
    participant Workflow
    participant Activities
    participant ONTAP
    participant GCP
    
    Client->>GoogleProxy: POST /{workflow_name}
    GoogleProxy->>Orchestrator: Create{workflow_name.title()}()
    Orchestrator->>Workflow: ExecuteWorkflow()
    
    Workflow->>Workflow: Setup(ctx, params)
    Workflow->>Activities: UpdateJobStatus(PROCESSING)
    Activities-->>Workflow: Job Status Updated
    
    Workflow->>Activities: Validate{workflow_name.title()}Params()
    Activities-->>Workflow: Validation Success
    
    Workflow->>Activities: Create{workflow_name.title()}()
    Activities->>GCP: CreateResource()
    GCP-->>Activities: Resource Created
    Activities-->>Workflow: {workflow_name.title()} Created
    
    Workflow->>Activities: UpdateJobStatus(DONE)
    Activities-->>Workflow: Job Status Updated
    
    Workflow-->>Orchestrator: Workflow Complete
    Orchestrator-->>GoogleProxy: {workflow_name.title()} Created
    GoogleProxy-->>Client: 201 Created
```'''
    
    def generate_activity_table(self, workflow_analysis: WorkflowAnalysis) -> str:
        """
        Generate markdown table of activities used in workflow.
        
        Returns:
            Markdown table string
        """
        # Get unique activities
        activity_names = list(dict.fromkeys([call.activity_name for call in workflow_analysis.activity_calls]))
        
        lines = [
            '| Activity | Purpose | Input | Output |',
            '|----------|---------|-------|--------|'
        ]
        
        for activity_name in activity_names:
            # Get activity definition
            activity_def = self.analyzer.activity_definitions.get(activity_name)
            
            # Find corresponding call
            call = next((c for c in workflow_analysis.activity_calls if c.activity_name == activity_name), None)
            
            # Format row
            purpose = activity_def.docstring if activity_def and activity_def.docstring else self._infer_purpose(activity_name)
            input_params = ', '.join(call.input_params[:2]) if call else 'N/A'
            output = call.output_var if call and call.output_var else 'void'
            
            lines.append(f'| `{activity_name}` | {purpose[:50]} | `{input_params[:30]}` | `{output}` |')
        
        return '\n'.join(lines)
    
    def _infer_purpose(self, activity_name: str) -> str:
        """Infer purpose from activity name."""
        name = activity_name.replace('Activity', '')
        # Convert CamelCase to words
        words = re.sub(r'([A-Z])', r' \1', name).strip().split()
        
        # Create purpose based on verb
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


# Example usage
if __name__ == "__main__":
    from pathlib import Path
    
    repo_root = Path(__file__).parent.parent
    analyzer = GoCodeAnalyzer(repo_root)
    tracer = WorkflowExecutionTracer(analyzer)
    
    # Analyze backup workflow
    backup_workflow = repo_root / "core" / "orchestrator" / "workflows" / "backup_workflow.go"
    
    if backup_workflow.exists():
        print("=" * 80)
        print("GENERATING SEQUENCE DIAGRAM FOR BACKUP WORKFLOW")
        print("=" * 80)
        
        # Analyze workflow
        analysis = analyzer.analyze_workflow_file(backup_workflow)
        
        # Load activities
        analyzer.analyze_all_activities("backup")
        
        # Generate sequence diagram
        diagram = tracer.generate_sequence_diagram(analysis)
        
        print(diagram)
        
        print("\n" + "=" * 80)
        print("ACTIVITY TABLE")
        print("=" * 80)
        
        table = tracer.generate_activity_table(analysis)
        print(table)
