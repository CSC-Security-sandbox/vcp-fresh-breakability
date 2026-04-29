# Markdown to Word Document Conversion Scripts

This directory contains utility scripts for converting Markdown files to Word documents (.docx).

## Prerequisites

Install pandoc:
```bash
# macOS
brew install pandoc

# Ubuntu/Debian
sudo apt-get install pandoc

# Windows
choco install pandoc
```

## Scripts

### 1. md-to-docx.sh (Two-step conversion via LaTeX)

Converts Markdown → LaTeX → Word document. This approach can provide better formatting control.

**Usage:**
```bash
./scripts/md-to-docx.sh <input.md> [output.docx]
```

**Examples:**
```bash
# Use default output name (replaces .md with .docx)
./scripts/md-to-docx.sh AI_Development_Case_Study.md

# Specify custom output name
./scripts/md-to-docx.sh AI_Development_Case_Study.md Case_Study.docx
```

### 2. md-to-docx-simple.sh (Direct conversion)

Converts Markdown → Word document directly. Faster and simpler.

**Usage:**
```bash
./scripts/md-to-docx-simple.sh <input.md> [output.docx]
```

**Examples:**
```bash
# Use default output name
./scripts/md-to-docx-simple.sh AI_Development_Case_Study_Executive_Brief.md

# Specify custom output name
./scripts/md-to-docx-simple.sh AI_Development_Case_Study.md output.docx
```

## Quick Start

To convert the AI case study documents:

```bash
# Convert the full case study
./scripts/md-to-docx-simple.sh AI_Development_Case_Study.md

# Convert the executive brief
./scripts/md-to-docx-simple.sh AI_Development_Case_Study_Executive_Brief.md
```

## Troubleshooting

**Error: "pandoc: command not found"**
- Install pandoc using the commands in Prerequisites section

**Error: "Permission denied"**
- Make the script executable: `chmod +x scripts/md-to-docx.sh`

**Formatting issues in output**
- Try the two-step conversion script (`md-to-docx.sh`) instead of the simple version
- Or use direct pandoc options for more control

## Advanced Usage

For more control over the conversion, you can use pandoc directly:

```bash
# With custom reference document for styling
pandoc -f markdown -t docx --reference-doc=custom-template.docx -o output.docx input.md

# With table of contents
pandoc -f markdown -t docx --toc -o output.docx input.md

# With specific sections
pandoc -f markdown -t docx --extract-media=./media -o output.docx input.md
```

## Notes

- The scripts automatically clean up intermediate files
- Output file size is displayed after conversion
- Both scripts provide colored output for better readability
- The two-step conversion may produce better formatted documents but takes longer

