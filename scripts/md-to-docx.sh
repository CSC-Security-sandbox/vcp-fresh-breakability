#!/bin/bash

# Markdown to Word Document Converter
# Usage: ./md-to-docx.sh <input.md> [output.docx]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored messages
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Check if pandoc is installed
if ! command -v pandoc &> /dev/null; then
    print_error "pandoc is not installed. Please install it first:"
    echo "  macOS:   brew install pandoc"
    echo "  Ubuntu:  sudo apt-get install pandoc"
    exit 1
fi

# Check if input file is provided
if [ $# -eq 0 ]; then
    print_error "No input file specified"
    echo "Usage: $0 <input.md> [output.docx]"
    echo ""
    echo "Examples:"
    echo "  $0 AI_Development_Case_Study.md"
    echo "  $0 AI_Development_Case_Study.md output.docx"
    exit 1
fi

INPUT_FILE="$1"

# Check if input file exists
if [ ! -f "$INPUT_FILE" ]; then
    print_error "Input file not found: $INPUT_FILE"
    exit 1
fi

# Determine output file name
if [ $# -eq 2 ]; then
    OUTPUT_FILE="$2"
else
    # Remove .md extension and add .docx
    OUTPUT_FILE="${INPUT_FILE%.md}.docx"
fi

# Generate intermediate LaTeX file name
LATEX_FILE="${INPUT_FILE%.md}.tex"

print_info "Converting: $INPUT_FILE → $OUTPUT_FILE"

# Step 1: Convert Markdown to LaTeX
print_info "Step 1/2: Converting Markdown to LaTeX..."
pandoc -f markdown -t latex -o "$LATEX_FILE" "$INPUT_FILE"

if [ $? -eq 0 ]; then
    print_info "LaTeX file created: $LATEX_FILE"
else
    print_error "Failed to convert Markdown to LaTeX"
    exit 1
fi

# Step 2: Convert LaTeX to Word document
print_info "Step 2/2: Converting LaTeX to Word document..."
pandoc -f latex -t docx -o "$OUTPUT_FILE" "$LATEX_FILE"

if [ $? -eq 0 ]; then
    print_info "Word document created: $OUTPUT_FILE"
else
    print_error "Failed to convert LaTeX to Word document"
    # Clean up LaTeX file
    rm -f "$LATEX_FILE"
    exit 1
fi

# Clean up intermediate LaTeX file
print_info "Cleaning up intermediate files..."
rm -f "$LATEX_FILE"

# Display file size
FILE_SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)
print_info "Document size: $FILE_SIZE"

echo ""
echo -e "${GREEN}✓ Conversion completed successfully!${NC}"
echo "Output file: $OUTPUT_FILE"

