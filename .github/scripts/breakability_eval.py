#!/usr/bin/env python3
"""Breakability evaluation corpus validator and scorer (stdlib only, no external deps).

Provides:
  - Corpus schema validation (JSON)
  - Scoring metrics: auto_clear%, review%, fix%, abstain%, false_green, false_block
  - False-green/false-block detection

Run: python3 .github/scripts/breakability_eval.py <corpus.json> [--predict <predictions.json>]
"""
import json
import sys
from typing import Any, Dict, List, Tuple
from enum import Enum


class Label(Enum):
    """Ground-truth label assigned to a corpus case."""
    TRUE_SAFE = "true_safe"
    TRUE_REVIEW = "true_review"
    TRUE_FIX = "true_fix"
    ABSTAIN = "abstain"


class Prediction(Enum):
    """Predicted label (output from policy/merger)."""
    AUTO_CLEAR = "auto_clear"
    REVIEW = "review"
    FIX = "fix"
    ABSTAIN = "abstain"


class CorpusCase:
    """Single evaluation case."""
    
    def __init__(self, case_dict: Dict[str, Any]):
        self.pr_id = case_dict.get("pr_id")
        self.ecosystem = case_dict.get("ecosystem")
        self.package = case_dict.get("package")
        self.from_version = case_dict.get("from_version")
        self.to_version = case_dict.get("to_version")
        self.expected_label = case_dict.get("expected_label")
        self.expected_evidence_class = case_dict.get("expected_evidence_class")
        self.notes = case_dict.get("notes", "")
        
    def validate(self) -> Tuple[bool, str]:
        """Validate case has required fields."""
        required = ["pr_id", "ecosystem", "package", "from_version", "to_version",
                    "expected_label", "expected_evidence_class"]
        missing = [f for f in required if getattr(self, f, None) is None]
        if missing:
            return False, f"Missing required fields: {missing}"
        
        try:
            Label(self.expected_label)
        except ValueError:
            return False, f"Invalid expected_label: {self.expected_label}"
        
        return True, ""


class CorpusValidator:
    """Validates corpus JSON structure and content."""
    
    @staticmethod
    def validate_corpus(data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate corpus structure.
        Returns: (is_valid, errors)
        """
        errors = []
        
        if "cases" not in data:
            errors.append("Missing 'cases' key")
            return False, errors
        
        cases = data["cases"]
        if not isinstance(cases, list):
            errors.append("'cases' must be a list")
            return False, errors
        
        for i, case_dict in enumerate(cases):
            case = CorpusCase(case_dict)
            valid, msg = case.validate()
            if not valid:
                errors.append(f"Case {i}: {msg}")
        
        if errors:
            return False, errors
        
        return True, []


class Scorer:
    """Computes evaluation metrics from predictions vs. corpus."""
    
    def __init__(self, cases: List[CorpusCase]):
        self.cases = cases
    
    def score(self, predictions: Dict[str, str]) -> Dict[str, Any]:
        """
        Score predictions against corpus.
        
        predictions: {pr_id -> predicted_label}
        
        Returns: {
            "metrics": {
                "auto_clear_pct": float,
                "review_pct": float,
                "fix_pct": float,
                "abstain_pct": float,
            },
            "errors": {
                "false_green_count": int,    # true_review/true_fix predicted as auto_clear
                "false_block_count": int,    # true_safe predicted as review/fix
            },
            "per_case": [
                {
                    "pr_id": str,
                    "expected": str,
                    "predicted": str,
                    "error": str or None,
                }
            ]
        }
        """
        metrics = {
            "auto_clear": 0,
            "review": 0,
            "fix": 0,
            "abstain": 0,
        }
        errors = {
            "false_green": 0,
            "false_block": 0,
        }
        per_case = []
        
        for case in self.cases:
            pred_label = predictions.get(case.pr_id, "abstain")
            error = None
            
            # Detect false green: predicted auto_clear but ground truth is true_review/true_fix
            if pred_label == "auto_clear" and case.expected_label in ["true_review", "true_fix"]:
                error = "false_green"
                errors["false_green"] += 1
            
            # Detect false block: predicted review/fix but ground truth is true_safe
            elif pred_label in ["review", "fix"] and case.expected_label == "true_safe":
                error = "false_block"
                errors["false_block"] += 1
            
            # Count prediction distribution
            if pred_label in metrics:
                metrics[pred_label] += 1
            else:
                metrics["abstain"] += 1
                pred_label = "abstain"
            
            per_case.append({
                "pr_id": case.pr_id,
                "expected": case.expected_label,
                "predicted": pred_label,
                "error": error,
            })
        
        total = len(self.cases)
        if total == 0:
            return {"error": "No cases to score"}
        
        result = {
            "metrics": {
                "auto_clear_pct": (metrics["auto_clear"] / total) * 100,
                "review_pct": (metrics["review"] / total) * 100,
                "fix_pct": (metrics["fix"] / total) * 100,
                "abstain_pct": (metrics["abstain"] / total) * 100,
            },
            "errors": {
                "false_green_count": errors["false_green"],
                "false_block_count": errors["false_block"],
            },
            "per_case": per_case,
        }
        
        return result


def load_corpus(filepath: str) -> Tuple[bool, List[CorpusCase], str]:
    """Load and validate corpus from JSON file."""
    try:
        with open(filepath, "r") as f:
            data = json.load(f)
    except Exception as e:
        return False, [], f"Failed to load corpus: {e}"
    
    valid, errors = CorpusValidator.validate_corpus(data)
    if not valid:
        return False, [], f"Corpus validation failed: {'; '.join(errors)}"
    
    cases = [CorpusCase(case_dict) for case_dict in data["cases"]]
    return True, cases, ""


def load_predictions(filepath: str) -> Tuple[bool, Dict[str, str], str]:
    """Load predictions from JSON file."""
    try:
        with open(filepath, "r") as f:
            data = json.load(f)
    except Exception as e:
        return False, {}, f"Failed to load predictions: {e}"
    
    if not isinstance(data, dict):
        return False, {}, "Predictions must be a dict {pr_id -> label}"
    
    return True, data, ""


def main():
    if len(sys.argv) < 2:
        print("Usage: breakability_eval.py <corpus.json> [--predict <predictions.json>]")
        return 1
    
    corpus_file = sys.argv[1]
    valid, cases, msg = load_corpus(corpus_file)
    if not valid:
        print(f"ERROR: {msg}", file=sys.stderr)
        return 1
    
    print(f"Corpus loaded: {len(cases)} cases")
    
    # If predictions provided, score them
    if len(sys.argv) >= 4 and sys.argv[2] == "--predict":
        pred_file = sys.argv[3]
        valid, predictions, msg = load_predictions(pred_file)
        if not valid:
            print(f"ERROR: {msg}", file=sys.stderr)
            return 1
        
        scorer = Scorer(cases)
        result = scorer.score(predictions)
        
        print("\n=== METRICS ===")
        for key, val in result["metrics"].items():
            print(f"  {key}: {val:.1f}%")
        
        print("\n=== ERRORS ===")
        for key, val in result["errors"].items():
            print(f"  {key}: {val}")
        
        print(f"\nFalse-green risk: {result['errors']['false_green_count']} cases")
        print(f"False-block cost: {result['errors']['false_block_count']} cases")
        
        print("\nDetailed results saved to: predictions.eval.json")
        with open("predictions.eval.json", "w") as f:
            json.dump(result, f, indent=2)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
