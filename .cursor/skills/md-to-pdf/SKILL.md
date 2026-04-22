---
name: md-to-pdf
description: Convert Markdown files to PDF with Mermaid diagram support. Use when the user asks to convert markdown to PDF, generate a PDF from a .md file, or export documentation as PDF.
---

# Markdown to PDF Converter

## Prerequisites

Before converting, verify the required tools are installed:

```bash
command -v pandoc && command -v xelatex || echo "MISSING"
```

If missing, instruct the user to install:

```bash
brew install pandoc
brew install --cask basictex
sudo tlmgr update --self && sudo tlmgr install \
    collection-fontsrecommended bookmark xurl footmisc
```

For Mermaid diagram rendering, `mmdc` or `npx @mermaid-js/mermaid-cli` must be available.

## Conversion Workflow

### Step 1: Identify the input file

Determine the Markdown file to convert. If the user references a file with `@`, use that path. Otherwise ask which file to convert.

### Step 2: Run the conversion script

Use the project's conversion script at `scripts/md-to-pdf.sh`:

```bash
./scripts/md-to-pdf.sh <input.md> [output.pdf]
```

**Options:**

| Flag | Description | Default |
|------|-------------|---------|
| `--toc` | Include table of contents | off |
| `--margin <size>` | Page margin | `1in` |
| `--font-size <pt>` | Font size in points | `11` |

**Examples:**

```bash
# Basic conversion (output alongside input)
./scripts/md-to-pdf.sh doc/architecture/designs/0022-vcp-oci-integration-hld.md

# With table of contents and custom output path
./scripts/md-to-pdf.sh --toc doc/architecture/designs/0022-vcp-oci-integration-hld.md output.pdf

# Custom margins and font size
./scripts/md-to-pdf.sh --margin 0.75in --font-size 10 doc/README.md
```

### Step 3: Report the result

After conversion completes, report the output file path and size to the user.

## What the script does

1. Detects available LaTeX engine (`xelatex` > `lualatex` > `pdflatex`)
2. Applies a Lua filter (`scripts/pandoc-table-wrap.lua`) that auto-distributes column widths evenly for tables without explicit widths, preventing text overflow
3. Injects a LaTeX preamble that reduces font size in table environments (`\small`) for better fit
4. If the Markdown contains ` ```mermaid ` blocks, renders them to PNG via `scripts/export_mermaid_images.py` and replaces the code blocks with image references
5. Converts the processed Markdown to PDF via pandoc with syntax highlighting, colored links, and configurable layout
6. Cleans up temporary files

## Table rendering

Tables are auto-formatted to prevent text overflow in PDF output:

- **Column widths**: The Lua filter (`scripts/pandoc-table-wrap.lua`) distributes column widths equally across all columns that don't have explicit widths set. This forces LaTeX to use paragraph-mode (`p{}`) columns that wrap text.
- **Font size**: Tables use `\small` font via a LaTeX preamble header, reducing the chance of overflow on wide tables.
- If tables still appear too wide, use `--margin 0.75in` or `--font-size 10` to gain extra horizontal space.

## Error handling

| Error | Fix |
|-------|-----|
| `pandoc: not found` | `brew install pandoc` |
| `No LaTeX PDF engine found` | `brew install --cask basictex` then install packages (see Prerequisites) |
| `Mermaid export script not found` | Ensure `scripts/export_mermaid_images.py` exists in the project root |
| LaTeX missing package errors | `sudo tlmgr install <package-name>` |
