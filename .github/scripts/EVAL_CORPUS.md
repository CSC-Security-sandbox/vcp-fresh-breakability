# Breakability Evaluation Corpus

Foundation for frozen evaluation/scoring harness enabling policy validation against known ground truth instead of vibes-based tuning.

## Usage

### Validate Corpus

```bash
python3 .github/scripts/breakability_eval.py eval-corpus.seed.json
```

### Score Predictions

Run your policy/merger to generate predictions as JSON, then:

```bash
python3 .github/scripts/breakability_eval.py eval-corpus.seed.json --predict <predictions.json>
```

Output:
- **Metrics**: auto_clear%, review%, fix%, abstain% distribution
- **Errors**: false_green_count (predicted safe but ground truth is review/fix), false_block_count (predicted review/fix but ground truth is safe)
- **Per-case**: Detailed breakdown with error flags

### Run Tests

```bash
python3 .github/scripts/test_breakability_eval.py
```

## Corpus Schema

```json
{
  "cases": [
    {
      "pr_id": "18",
      "ecosystem": "go",
      "package": "github.com/example/api",
      "from_version": "1.0.0",
      "to_version": "2.0.0",
      "expected_label": "true_fix",  // true_safe, true_review, true_fix, abstain
      "expected_evidence_class": "api_break",
      "notes": "Breaking API change"
    }
  ]
}
```

## Seed Corpus

Frozen snapshot from merge plan #122. Each case is conservative ground truth:

- **#18** (true_fix): Breaking API change, build fails, test suite catches it
- **#2** (true_review): Supply-chain signal, requires human verification
- **#8** (true_review): Behavioral changes, not fully covered by probes
- **#28** (true_safe): CVE patch, tests pass, no interface changes
- **#16** (true_review): Test suite exists but not verified clean

## Metrics Explained

- **false_green_count**: Cases predicted as auto_clear but ground truth is true_review/true_fix (highest risk)
- **false_block_count**: Cases predicted as review/fix but ground truth is true_safe (dev friction cost)
- **Review %**: Fraction predicted to require review

Target: minimize false_green at acceptable false_block/review cost.
