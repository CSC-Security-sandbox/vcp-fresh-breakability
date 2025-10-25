#!/usr/bin/env python3
"""
Go Code Analyzer - Deep Analysis of VSA Control Plane Go Code

Extracts detailed information from Go workflow, activity, and model files:
- Activity function signatures and parameters
- Struct definitions with JSONB expansion
- Temporal retry configurations
- Error handling patterns
- Database indexes
"""

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Optional, Set, Tuple
from collections import defaultdict


@dataclass
class ActivityDefinition:
    """Represents a Temporal activity function"""
    name: str
    file_path: str
    parameters: List[Dict[str, str]]  # [{'name': 'ctx', 'type': 'context.Context'}]
    return_types: List[str]
    docstring: str = ""
    line_number: int = 0
    is_exported: bool = True


@dataclass
class ActivityCall:
    """Represents a workflow.ExecuteActivity() call"""
    activity_name: str
    input_params: List[str]
    output_var: str
    line_number: int
    conditional_block: Optional[str] = None  # "if", "else", "switch", etc.


@dataclass
class StructField:
    """Represents a struct field"""
    name: str
    type: str
    json_tag: Optional[str] = None
    gorm_tag: Optional[str] = None
    description: Optional[str] = None


@dataclass
class StructDefinition:
    """Represents a Go struct"""
    name: str
    fields: List[StructField]
    file_path: str
    is_jsonb: bool = False
    embedded_structs: List[str] = field(default_factory=list)


@dataclass
class RetryConfig:
    """Temporal retry configuration"""
    initial_interval: Optional[str] = None
    backoff_coefficient: Optional[float] = None
    maximum_interval: Optional[str] = None
    maximum_attempts: Optional[int] = None
    non_retryable_errors: List[str] = field(default_factory=list)


@dataclass
class WorkflowAnalysis:
    """Complete workflow analysis result"""
    workflow_name: str
    file_path: str
    resource_type: str
    activity_calls: List[ActivityCall]
    retry_config: Optional[RetryConfig] = None
    error_handling: List[str] = field(default_factory=list)
    conditional_branches: List[Dict] = field(default_factory=list)
    timeouts: Dict[str, str] = field(default_factory=dict)


class GoCodeAnalyzer:
    """Deep analyzer for Go code in VSA Control Plane"""
    
    def __init__(self, repo_root: Path):
        self.repo_root = repo_root
        self.workflows_dir = repo_root / "core" / "orchestrator" / "workflows"
        self.activities_dir = repo_root / "core" / "orchestrator" / "activities"
        self.datamodel_dir = repo_root / "core" / "datamodel"
        self.errors_dir = repo_root / "core" / "errors"
        
        # Caches
        self.activity_definitions: Dict[str, ActivityDefinition] = {}
        self.struct_definitions: Dict[str, StructDefinition] = {}
    
    def analyze_workflow_file(self, workflow_file: Path) -> WorkflowAnalysis:
        """
        Analyze a workflow Go file to extract detailed execution information.
        
        Args:
            workflow_file: Path to workflow .go file
        
        Returns:
            WorkflowAnalysis with all extracted details
        """
        if not workflow_file.exists():
            raise FileNotFoundError(f"Workflow file not found: {workflow_file}")
        
        content = workflow_file.read_text()
        workflow_name = workflow_file.stem.replace("_workflow", "").replace("_workflows", "")
        
        analysis = WorkflowAnalysis(
            workflow_name=workflow_name,
            file_path=str(workflow_file),
            resource_type=workflow_name.split("_")[0],
            activity_calls=[]  # Will be populated below
        )
        
        # Extract activity calls
        analysis.activity_calls = self._extract_activity_calls(content)
        
        # Extract retry configuration
        analysis.retry_config = self._extract_retry_config(content)
        
        # Extract error handling patterns
        analysis.error_handling = self._extract_error_handling(content)
        
        # Extract conditional branches
        analysis.conditional_branches = self._extract_conditional_branches(content)
        
        # Extract timeout configurations
        analysis.timeouts = self._extract_timeouts(content)
        
        return analysis
    
    def _extract_activity_calls(self, content: str) -> List[ActivityCall]:
        """
        Extract workflow.ExecuteActivity() calls from workflow code.
        
        Patterns to match:
        - workflow.ExecuteActivity(ctx, activities.SomeActivity, params).Get(ctx, &result)
        - workflow.ExecuteActivity(ctx, backupActivity.ActivityName, params).Get(ctx, &result)
        - err := workflow.ExecuteActivity(ctx, "ActivityName", input).Get(ctx, &output)
        """
        calls = []
        
        # Pattern 1: workflow.ExecuteActivity with Get - handles nested activity references
        pattern1 = r'(?:err\s*[=:]=\s*)?workflow\.ExecuteActivity\s*\(\s*ctx\s*,\s*(?:[\w.]+\.)?(\w+Activity|GetNode)\s*,\s*([^)]*)\)\s*\.Get\s*\(\s*ctx\s*,\s*&(\w+)\s*\)'
        
        for match in re.finditer(pattern1, content, re.MULTILINE):
            activity_name = match.group(1)
            input_params_str = match.group(2).strip()
            output_var = match.group(3)
            line_number = content[:match.start()].count('\n') + 1
            
            # Parse input parameters
            input_params = [p.strip() for p in input_params_str.split(',') if p.strip()]
            
            calls.append(ActivityCall(
                activity_name=activity_name,
                input_params=input_params,
                output_var=output_var,
                line_number=line_number
            ))
        
        # Pattern 2: ExecuteActivity without Get
        pattern2 = r'workflow\.ExecuteActivity\s*\(\s*ctx\s*,\s*(?:[\w.]+\.)?(\w+Activity)\s*,\s*([^)]*)\)'
        
        for match in re.finditer(pattern2, content, re.MULTILINE):
            # Skip if already matched by pattern1 (has .Get)
            if '.Get' in content[match.end():match.end()+20]:
                continue
            
            activity_name = match.group(1)
            input_params_str = match.group(2).strip()
            line_number = content[:match.start()].count('\n') + 1
            
            input_params = [p.strip() for p in input_params_str.split(',') if p.strip()]
            
            calls.append(ActivityCall(
                activity_name=activity_name,
                input_params=input_params,
                output_var="",
                line_number=line_number
            ))
        
        # Pattern 3: ExecuteLocalActivity
        pattern3 = r'workflow\.ExecuteLocalActivity\s*\(\s*ctx\s*,\s*(?:[\w.]+\.)?(\w+Activity)\s*,\s*([^)]*)\)'
        
        for match in re.finditer(pattern3, content, re.MULTILINE):
            activity_name = match.group(1)
            input_params_str = match.group(2).strip()
            line_number = content[:match.start()].count('\n') + 1
            
            input_params = [p.strip() for p in input_params_str.split(',') if p.strip()]
            
            calls.append(ActivityCall(
                activity_name=activity_name,
                input_params=input_params,
                output_var="",
                line_number=line_number
            ))
        
        return calls
    
    def _extract_retry_config(self, content: str) -> Optional[RetryConfig]:
        """Extract Temporal retry policy configuration."""
        retry_config = RetryConfig()
        
        # Look for RetryPolicy struct initialization
        patterns = {
            'initial_interval': r'InitialInterval:\s*(?:time\.)?(\w+)',
            'backoff_coefficient': r'BackoffCoefficient:\s*([\d.]+)',
            'maximum_interval': r'MaximumInterval:\s*(?:time\.)?(\w+)',
            'maximum_attempts': r'MaximumAttempts:\s*(\d+)',
        }
        
        for field, pattern in patterns.items():
            match = re.search(pattern, content)
            if match:
                value = match.group(1)
                if field == 'backoff_coefficient':
                    setattr(retry_config, field, float(value))
                elif field == 'maximum_attempts':
                    setattr(retry_config, field, int(value))
                else:
                    setattr(retry_config, field, value)
        
        # Extract non-retryable error types
        non_retryable_pattern = r'NonRetryableErrorTypes:\s*\[\]string\{([^}]+)\}'
        match = re.search(non_retryable_pattern, content)
        if match:
            errors = [e.strip(' "') for e in match.group(1).split(',')]
            retry_config.non_retryable_errors = errors
        
        # Return None if no config found
        if not any([retry_config.initial_interval, retry_config.maximum_attempts]):
            return None
        
        return retry_config
    
    def _extract_error_handling(self, content: str) -> List[str]:
        """Extract error handling patterns from workflow."""
        error_patterns = []
        
        # Pattern: if err != nil { ... }
        if_err_pattern = r'if\s+err\s*!=\s*nil\s*\{([^}]+)\}'
        
        for match in re.finditer(if_err_pattern, content, re.DOTALL):
            error_block = match.group(1).strip()
            
            # Look for common error handling actions
            if 'return' in error_block:
                error_patterns.append("Return error to caller")
            if 'Revert' in error_block or 'rollback' in error_block.lower():
                error_patterns.append("Rollback/Revert on failure")
            if 'UpdateJobStatus' in error_block:
                error_patterns.append("Update job status to ERROR")
            if 'logger' in error_block.lower() or 'log.' in error_block.lower():
                error_patterns.append("Log error details")
        
        return list(set(error_patterns))
    
    def _extract_conditional_branches(self, content: str) -> List[Dict]:
        """Extract conditional logic that affects workflow execution."""
        branches = []
        
        # Pattern: if condition { activities }
        if_pattern = r'if\s+([^{]+)\s*\{([^}]+)\}'
        
        for match in re.finditer(if_pattern, content, re.DOTALL):
            condition = match.group(1).strip()
            block = match.group(2).strip()
            
            # Only include if it contains activity calls or workflow decisions
            if 'ExecuteActivity' in block or 'ExecuteChildWorkflow' in block:
                branches.append({
                    'type': 'if',
                    'condition': condition,
                    'has_activities': True,
                    'line_number': content[:match.start()].count('\n') + 1
                })
        
        # Pattern: switch/case
        switch_pattern = r'switch\s+([^{]+)\s*\{([^}]+)\}'
        
        for match in re.finditer(switch_pattern, content, re.DOTALL):
            condition = match.group(1).strip()
            cases_block = match.group(2)
            
            # Count cases
            case_count = len(re.findall(r'case\s+', cases_block))
            
            branches.append({
                'type': 'switch',
                'condition': condition,
                'case_count': case_count,
                'line_number': content[:match.start()].count('\n') + 1
            })
        
        return branches
    
    def _extract_timeouts(self, content: str) -> Dict[str, str]:
        """Extract timeout configurations from workflow."""
        timeouts = {}
        
        # Pattern: StartToCloseTimeout: time.Duration
        timeout_pattern = r'(\w*Timeout):\s*(?:time\.)?(\w+)'
        
        for match in re.finditer(timeout_pattern, content):
            timeout_name = match.group(1)
            timeout_value = match.group(2)
            timeouts[timeout_name] = timeout_value
        
        return timeouts
    
    def extract_activity_definitions(self, activity_file: Path) -> List[ActivityDefinition]:
        """
        Extract activity function definitions from activity Go file.
        
        Args:
            activity_file: Path to activity .go file
        
        Returns:
            List of ActivityDefinition objects
        """
        if not activity_file.exists():
            return []
        
        content = activity_file.read_text()
        definitions = []
        
        # Pattern: func (receiver) ActivityName(params) (returns) { ... }
        # Also match: func ActivityName(params) (returns) { ... }
        function_pattern = r'(?:^|\n)func\s+(?:\([^)]+\)\s+)?(\w+Activity)\s*\(([^)]*)\)\s*(?:\(([^)]*)\)|(\w+))\s*\{'
        
        for match in re.finditer(function_pattern, content, re.MULTILINE):
            name = match.group(1)
            params_str = match.group(2)
            returns_str = match.group(3) or match.group(4) or ""
            line_number = content[:match.start()].count('\n') + 1
            
            # Parse parameters
            parameters = self._parse_function_params(params_str)
            
            # Parse return types
            return_types = self._parse_return_types(returns_str)
            
            # Extract docstring (look backwards for comment block)
            docstring = self._extract_docstring(content, match.start())
            
            definitions.append(ActivityDefinition(
                name=name,
                file_path=str(activity_file),
                parameters=parameters,
                return_types=return_types,
                docstring=docstring,
                line_number=line_number,
                is_exported=name[0].isupper()
            ))
        
        return definitions
    
    def _parse_function_params(self, params_str: str) -> List[Dict[str, str]]:
        """Parse function parameters into structured format."""
        if not params_str.strip():
            return []
        
        params = []
        # Split by comma, handling types like map[string]string
        parts = self._smart_split(params_str, ',')
        
        for part in parts:
            part = part.strip()
            if not part:
                continue
            
            # Pattern: name type or just type
            tokens = part.rsplit(maxsplit=1)
            if len(tokens) == 2:
                param_name, param_type = tokens
                params.append({'name': param_name, 'type': param_type})
            elif len(tokens) == 1:
                # Just type (unnamed param)
                params.append({'name': '', 'type': tokens[0]})
        
        return params
    
    def _parse_return_types(self, returns_str: str) -> List[str]:
        """Parse return types from function signature."""
        if not returns_str.strip():
            return []
        
        # Handle multiple return types
        return_types = self._smart_split(returns_str, ',')
        return [rt.strip() for rt in return_types if rt.strip()]
    
    def _smart_split(self, text: str, delimiter: str) -> List[str]:
        """Split text by delimiter, respecting brackets and parentheses."""
        parts = []
        current = []
        depth = 0
        
        for char in text:
            if char in '([{':
                depth += 1
            elif char in ')]}':
                depth -= 1
            
            if char == delimiter and depth == 0:
                parts.append(''.join(current))
                current = []
            else:
                current.append(char)
        
        if current:
            parts.append(''.join(current))
        
        return parts
    
    def _extract_docstring(self, content: str, func_start_pos: int) -> str:
        """Extract docstring comment above function definition."""
        # Look backwards for comment lines
        lines_before = content[:func_start_pos].split('\n')
        docstring_lines = []
        
        # Read backwards until we hit a non-comment line
        for line in reversed(lines_before[-10:]):  # Look at last 10 lines
            stripped = line.strip()
            if stripped.startswith('//'):
                docstring_lines.insert(0, stripped[2:].strip())
            elif stripped:
                break  # Hit non-comment, non-empty line
        
        return ' '.join(docstring_lines)
    
    def extract_struct_definition(self, model_file: Path, struct_name: str) -> Optional[StructDefinition]:
        """
        Extract struct definition including fields and tags.
        
        Args:
            model_file: Path to model .go file
            struct_name: Name of struct to extract
        
        Returns:
            StructDefinition or None if not found
        """
        if not model_file.exists():
            return None
        
        content = model_file.read_text()
        
        # Pattern: type StructName struct { ... }
        struct_pattern = rf'type\s+{struct_name}\s+struct\s*\{{([^}}]+)\}}'
        
        match = re.search(struct_pattern, content, re.DOTALL | re.IGNORECASE)
        if not match:
            return None
        
        struct_body = match.group(1)
        fields = self._parse_struct_fields(struct_body)
        
        # Check if this is a JSONB struct (used as jsonb in another struct)
        is_jsonb = f'type:jsonb' in content and struct_name in content
        
        return StructDefinition(
            name=struct_name,
            fields=fields,
            file_path=str(model_file),
            is_jsonb=is_jsonb
        )
    
    def _parse_struct_fields(self, struct_body: str) -> List[StructField]:
        """Parse struct field definitions."""
        fields = []
        
        # Pattern: FieldName Type `tags`
        field_pattern = r'(\w+)\s+([^\s`]+)(?:\s+`([^`]*)`)?'
        
        for match in re.finditer(field_pattern, struct_body, re.MULTILINE):
            field_name = match.group(1)
            field_type = match.group(2)
            tags = match.group(3) or ""
            
            # Skip embedded structs (no explicit field name with type starting uppercase)
            if field_type[0].isupper() and '.' not in field_type:
                # This might be an embedded struct
                pass
            
            # Parse tags
            json_tag = self._extract_tag(tags, 'json')
            gorm_tag = self._extract_tag(tags, 'gorm')
            
            fields.append(StructField(
                name=field_name,
                type=field_type,
                json_tag=json_tag,
                gorm_tag=gorm_tag
            ))
        
        return fields
    
    def _extract_tag(self, tags: str, tag_name: str) -> Optional[str]:
        """Extract specific tag value from struct tags."""
        pattern = rf'{tag_name}:"([^"]*)"'
        match = re.search(pattern, tags)
        return match.group(1) if match else None
    
    def find_jsonb_attributes(self, model_file: Path, base_struct_name: str) -> Dict[str, StructDefinition]:
        """
        Find and parse JSONB attribute structs referenced by base struct.
        
        Args:
            model_file: Path to models.go
            base_struct_name: Name of base struct (e.g., "Backup")
        
        Returns:
            Dict mapping attribute struct names to their definitions
        """
        jsonb_structs = {}
        
        # First, get the base struct
        base_struct = self.extract_struct_definition(model_file, base_struct_name)
        if not base_struct:
            return jsonb_structs
        
        # Find fields with type:jsonb tag
        for field in base_struct.fields:
            if field.gorm_tag and 'type:jsonb' in field.gorm_tag:
                # This field's type is the name of the JSONB struct
                attr_struct_name = field.type.strip('*')  # Remove pointer if present
                
                # Extract the JSONB struct definition
                attr_struct = self.extract_struct_definition(model_file, attr_struct_name)
                if attr_struct:
                    attr_struct.is_jsonb = True
                    jsonb_structs[attr_struct_name] = attr_struct
        
        return jsonb_structs
    
    def analyze_all_activities(self, resource_type: Optional[str] = None) -> Dict[str, ActivityDefinition]:
        """
        Analyze all activity files and cache results.
        
        Args:
            resource_type: Optional filter for specific resource type
        
        Returns:
            Dict mapping activity names to definitions
        """
        if not self.activities_dir.exists():
            return {}
        
        pattern = f"*{resource_type}*" if resource_type else "*.go"
        activity_files = [f for f in self.activities_dir.glob(pattern) if not f.name.endswith('_test.go')]
        
        for activity_file in activity_files:
            definitions = self.extract_activity_definitions(activity_file)
            for defn in definitions:
                self.activity_definitions[defn.name] = defn
        
        return self.activity_definitions
    
    def get_activity_for_call(self, call: ActivityCall) -> Optional[ActivityDefinition]:
        """
        Get the activity definition for a given activity call.
        
        Args:
            call: ActivityCall from workflow
        
        Returns:
            ActivityDefinition if found, None otherwise
        """
        # Ensure activities are loaded
        if not self.activity_definitions:
            self.analyze_all_activities()
        
        return self.activity_definitions.get(call.activity_name)


# Example usage and testing
if __name__ == "__main__":
    from pathlib import Path
    
    repo_root = Path(__file__).parent.parent
    analyzer = GoCodeAnalyzer(repo_root)
    
    # Test workflow analysis
    backup_workflow = repo_root / "core" / "orchestrator" / "workflows" / "backup_workflow.go"
    
    if backup_workflow.exists():
        print("=" * 80)
        print("ANALYZING BACKUP WORKFLOW")
        print("=" * 80)
        
        analysis = analyzer.analyze_workflow_file(backup_workflow)
        
        print(f"\nWorkflow: {analysis.workflow_name}")
        print(f"Resource Type: {analysis.resource_type}")
        print(f"\nActivity Calls: {len(analysis.activity_calls)}")
        
        for i, call in enumerate(analysis.activity_calls[:10], 1):
            print(f"  {i}. {call.activity_name} (line {call.line_number})")
            print(f"     Input: {', '.join(call.input_params[:2])}...")
            print(f"     Output: {call.output_var}")
        
        if analysis.retry_config:
            print(f"\nRetry Configuration:")
            print(f"  Initial Interval: {analysis.retry_config.initial_interval}")
            print(f"  Backoff Coefficient: {analysis.retry_config.backoff_coefficient}")
            print(f"  Maximum Attempts: {analysis.retry_config.maximum_attempts}")
        
        if analysis.error_handling:
            print(f"\nError Handling Patterns:")
            for pattern in analysis.error_handling:
                print(f"  - {pattern}")
        
        if analysis.conditional_branches:
            print(f"\nConditional Branches: {len(analysis.conditional_branches)}")
            for branch in analysis.conditional_branches[:3]:
                print(f"  - {branch['type']}: {branch.get('condition', 'N/A')[:50]}")
    
    # Test activity extraction
    backup_activities = repo_root / "core" / "orchestrator" / "activities" / "backup_activities.go"
    
    if backup_activities.exists():
        print("\n" + "=" * 80)
        print("ANALYZING BACKUP ACTIVITIES")
        print("=" * 80)
        
        definitions = analyzer.extract_activity_definitions(backup_activities)
        
        print(f"\nFound {len(definitions)} activity definitions:")
        for defn in definitions[:5]:
            print(f"\n  {defn.name}:")
            print(f"    Parameters: {len(defn.parameters)}")
            for param in defn.parameters[:3]:
                print(f"      - {param['name']}: {param['type']}")
            print(f"    Returns: {', '.join(defn.return_types)}")
            if defn.docstring:
                print(f"    Doc: {defn.docstring[:60]}...")
    
    # Test struct extraction
    models_file = repo_root / "core" / "datamodel" / "models.go"
    
    if models_file.exists():
        print("\n" + "=" * 80)
        print("ANALYZING BACKUP STRUCT")
        print("=" * 80)
        
        backup_struct = analyzer.extract_struct_definition(models_file, "Backup")
        
        if backup_struct:
            print(f"\nStruct: {backup_struct.name}")
            print(f"Fields: {len(backup_struct.fields)}")
            
            for field in backup_struct.fields[:10]:
                print(f"  - {field.name}: {field.type}")
                if field.gorm_tag:
                    print(f"    GORM: {field.gorm_tag}")
            
            # Find JSONB attributes
            print("\n" + "=" * 80)
            print("ANALYZING JSONB ATTRIBUTES")
            print("=" * 80)
            
            jsonb_attrs = analyzer.find_jsonb_attributes(models_file, "Backup")
            
            for attr_name, attr_struct in jsonb_attrs.items():
                print(f"\n{attr_name} (JSONB):")
                print(f"  Fields: {len(attr_struct.fields)}")
                for field in attr_struct.fields[:5]:
                    print(f"    - {field.name}: {field.type}")
