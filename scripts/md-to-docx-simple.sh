#!/bin/bash

# Simple Markdown to Word Document Converter (with diagram support)
# Usage:
#   ./md-to-docx-simple.sh <input.md> [output.docx]
#   ./md-to-docx-simple.sh --diagrams-only <input.md> [output_dir]

set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Parse --diagrams-only (-d) flag
DIAGRAMS_ONLY=false
if [ "${1:-}" = "--diagrams-only" ] || [ "${1:-}" = "-d" ]; then
    DIAGRAMS_ONLY=true
    shift
fi

# Check if python3 is installed (needed for diagram export)
if ! command -v python3 &> /dev/null; then
    print_error "python3 is not installed."
    exit 1
fi

# Check if pandoc is installed (only needed when not diagrams-only)
if [ "$DIAGRAMS_ONLY" = false ] && ! command -v pandoc &> /dev/null; then
    print_error "pandoc is not installed. Install it with: brew install pandoc"
    exit 1
fi

# Check arguments
if [ $# -eq 0 ]; then
    if [ "$DIAGRAMS_ONLY" = true ]; then
        print_error "Usage: $0 --diagrams-only <input.md> [output_dir]"
    else
        print_error "Usage: $0 <input.md> [output.docx]"
    fi
    echo "       $0 --diagrams-only <input.md> [output_dir]  # Export only Mermaid diagrams to images"
    exit 1
fi

INPUT_FILE="$1"
if [ "$DIAGRAMS_ONLY" = true ]; then
    # Default output dir: <input_dir>/images/mermaid/<input_stem>
    INPUT_DIR=$(dirname "$INPUT_FILE")
    INPUT_STEM="${INPUT_FILE%.md}"
    INPUT_STEM=$(basename "$INPUT_STEM")
    OUTPUT_DIR="${2:-$INPUT_DIR/images/mermaid/$INPUT_STEM}"
else
    OUTPUT_FILE="${2:-${INPUT_FILE%.md}.docx}"
fi

# Check if input exists
if [ ! -f "$INPUT_FILE" ]; then
    print_error "Input file not found: $INPUT_FILE"
    exit 1
fi

# Get absolute paths
INPUT_FILE_ABS=$(cd "$(dirname "$INPUT_FILE")" && pwd)/$(basename "$INPUT_FILE")
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
EXPORT_SCRIPT="$SCRIPT_DIR/export_mermaid_images.py"

# --- Export only diagrams (no DOCX) ---
if [ "$DIAGRAMS_ONLY" = true ]; then
    if [ ! -f "$EXPORT_SCRIPT" ]; then
        print_error "Mermaid export script not found: $EXPORT_SCRIPT"
        exit 1
    fi
    if ! grep -q '```mermaid' "$INPUT_FILE"; then
        print_warning "No Mermaid diagrams found in: $INPUT_FILE"
        exit 0
    fi
    print_info "Exporting diagrams only: $INPUT_FILE → $OUTPUT_DIR (PNG + SVG)"
    python3 "$EXPORT_SCRIPT" --input "$INPUT_FILE_ABS" --output "$OUTPUT_DIR" --format png,svg
    print_info "Diagrams exported to: $OUTPUT_DIR"
    exit 0
fi

# --- Full conversion (diagrams + DOCX) ---
print_info "Converting: $INPUT_FILE → $OUTPUT_FILE"

# Check if file has mermaid diagrams
if grep -q '```mermaid' "$INPUT_FILE"; then
    print_info "Mermaid diagrams detected, exporting to images..."
    
    # Check if export script exists
    if [ ! -f "$EXPORT_SCRIPT" ]; then
        print_error "Mermaid export script not found: $EXPORT_SCRIPT"
        exit 1
    fi
    
    # Create temporary directory for processing
    TEMP_DIR=$(mktemp -d)
    TEMP_MD="$TEMP_DIR/converted.md"
    IMAGE_DIR="$TEMP_DIR/images"
    
    # Export diagrams to images (PNG for docx embedding, SVG for reuse)
    print_info "Rendering Mermaid diagrams (PNG + SVG)..."
    python3 "$EXPORT_SCRIPT" --input "$INPUT_FILE_ABS" --output "$IMAGE_DIR" --format png,svg 2>&1 | grep -v "^$" || true
    
    # Create modified markdown with image references
    print_info "Replacing diagram code blocks with image references..."
    python3 -c "
import re
import sys
from pathlib import Path

md_file = Path('$INPUT_FILE_ABS')
image_dir = Path('$IMAGE_DIR')
output_file = Path('$TEMP_MD')

content = md_file.read_text()
diagram_count = 0

def replace_mermaid(match):
    global diagram_count
    diagram_count += 1
    # Get the relative path structure
    rel_path = md_file.stem
    img_path = image_dir / rel_path / f'diagram-{diagram_count:02d}.png'
    if img_path.exists():
        return f'![]({img_path})'
    else:
        print(f'Warning: Image not found: {img_path}', file=sys.stderr)
        return match.group(0)

# Replace mermaid code blocks with image references
pattern = r'\`\`\`mermaid\n.*?\n\`\`\`'
new_content = re.sub(pattern, replace_mermaid, content, flags=re.DOTALL)

output_file.write_text(new_content)
print(f'Replaced {diagram_count} diagram(s)')
"
    
    # Convert modified markdown to DOCX
    print_info "Converting to Word document with embedded images..."
    pandoc -f markdown -t docx -o "$OUTPUT_FILE" "$TEMP_MD"
    
    # Clean up temp directory
    rm -rf "$TEMP_DIR"
else
    print_info "No Mermaid diagrams found, doing direct conversion..."
    # Direct conversion without diagram processing
    pandoc -f markdown -t docx -o "$OUTPUT_FILE" "$INPUT_FILE"
fi

if [ $? -eq 0 ]; then
    FILE_SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)
    echo ""
    echo -e "${GREEN}✓ Conversion completed!${NC}"
    echo "Output: $OUTPUT_FILE ($FILE_SIZE)"
else
    print_error "Conversion failed"
    exit 1
fi

