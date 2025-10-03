# Test Plan Suites: Generation and Update Process

This document describes the process for generating or updating test plan suite files in this repository, ensuring strict alignment with the canonical test plan template.

## Steps to Generate or Correct Suite Files

1. **Download Source Documents**
   - Download the Test Design Specification (TDS) and Test Plan (TP) documents in Word format from the appropriate source (e.g., Confluence, SharePoint, or other documentation systems).

2. **Upload to Agent Context**
   - Upload the downloaded Word files to the workspace and add them to the context of the Agent (GitHub Copilot or equivalent).

3. **Strict Template Compliance Check**
   - Instruct the Agent to perform a strict compliance check against the canonical test plan template (see `_templates/test-plan-template.md`).
   - The Agent will:
     - Parse the uploaded documents.
     - Map all technical and process content to the correct template sections.
     - Identify and fill any missing sections as per the template.
     - Correct English, formatting, and structure for clarity and consistency.

4. **Suite Folder Creation or Update**
   - The Agent will create a new suite folder (if it does not exist) or update the existing suite folder as needed.
   - The suite file will be named according to the convention: `<feature>-test-suite.md` (e.g., `backup-protection-test-suite.md`).
   - All suite files must reside directly under their respective feature folders (e.g., `backup/`, `crr/`, `cmek-kms/`, etc.).

5. **Review and Finalize**
   - Review the generated or updated suite file for completeness and accuracy.
   - Make any additional manual edits if required.
   - Commit the changes to version control.

## Notes
- Always ensure that the suite files strictly follow the canonical template for consistency across all features.
- If you have questions or need assistance, consult the `_templates/test-plan-template.md` or contact the test documentation owner.
