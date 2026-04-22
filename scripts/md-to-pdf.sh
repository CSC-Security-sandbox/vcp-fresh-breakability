#!/bin/bash

# Markdown to PDF Converter (with Mermaid diagram support)
# Usage:
#   ./md-to-pdf.sh <input.md> [output.pdf]
#   ./md-to-pdf.sh --toc <input.md> [output.pdf]    # Include table of contents
#
# Prerequisites:
#   brew install pandoc
#   brew install --cask basictex   # or: brew install --cask mactex
#   # After basictex, add LaTeX packages needed for PDF generation:
#   sudo tlmgr update --self && sudo tlmgr install \
#       collection-fontsrecommended bookmark xurl footmisc
#
# For Mermaid diagram rendering (one of):
#   npm install -g @mermaid-js/mermaid-cli
#   npx @mermaid-js/mermaid-cli (auto, no install needed)
#
# A Chrome/Chromium browser must be available for Mermaid rendering.

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_info()    { echo -e "${GREEN}[INFO]${NC} $1"; }
print_error()   { echo -e "${RED}[ERROR]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }

# Parse flags
TOC=false
MARGIN="1in"
FONT_SIZE="11"

while [[ "${1:-}" == --* ]]; do
    case "$1" in
        --toc)        TOC=true; shift ;;
        --margin)     MARGIN="$2"; shift 2 ;;
        --font-size)  FONT_SIZE="$2"; shift 2 ;;
        *) print_error "Unknown option: $1"; exit 1 ;;
    esac
done

if ! command -v pandoc &> /dev/null; then
    print_error "pandoc is not installed. Install with: brew install pandoc"
    exit 1
fi

if ! command -v python3 &> /dev/null; then
    print_error "python3 is not installed."
    exit 1
fi

detect_pdf_engine() {
    for engine in xelatex lualatex pdflatex; do
        if command -v "$engine" &> /dev/null; then
            echo "$engine"
            return
        fi
    done
    return 1
}

PDF_ENGINE=$(detect_pdf_engine) || {
    print_error "No LaTeX PDF engine found (xelatex, lualatex, or pdflatex)."
    echo ""
    echo "Install one with:"
    echo "  brew install --cask basictex"
    echo "  # Then: sudo tlmgr update --self && sudo tlmgr install collection-fontsrecommended bookmark xurl footmisc"
    echo ""
    echo "Or for the full distribution:"
    echo "  brew install --cask mactex"
    exit 1
}

# Detect Chrome/Chromium for Mermaid rendering
detect_chrome() {
    local candidates=(
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
        "/Applications/Chromium.app/Contents/MacOS/Chromium"
        "/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary"
        "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"
        "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"
    )

    for candidate in "${candidates[@]}"; do
        if [ -x "$candidate" ]; then
            echo "$candidate"
            return
        fi
    done

    # Try PATH-based detection (Linux / custom installs)
    for bin in google-chrome chromium-browser chromium chrome; do
        if command -v "$bin" &> /dev/null; then
            command -v "$bin"
            return
        fi
    done

    return 1
}

if [ $# -eq 0 ]; then
    echo "Usage: $0 [options] <input.md> [output.pdf]"
    echo ""
    echo "Options:"
    echo "  --toc              Include table of contents"
    echo "  --margin <size>    Page margin (default: 1in)"
    echo "  --font-size <pt>   Font size in points (default: 11)"
    exit 1
fi

INPUT_FILE="$1"
OUTPUT_FILE="${2:-${INPUT_FILE%.md}.pdf}"

if [ ! -f "$INPUT_FILE" ]; then
    print_error "Input file not found: $INPUT_FILE"
    exit 1
fi

INPUT_FILE_ABS=$(cd "$(dirname "$INPUT_FILE")" && pwd)/$(basename "$INPUT_FILE")
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXPORT_SCRIPT="$SCRIPT_DIR/export_mermaid_images.py"

print_info "Converting: $INPUT_FILE → $OUTPUT_FILE"
print_info "PDF engine: $PDF_ENGINE"

TEMP_DIR=$(mktemp -d)
trap 'rm -rf "$TEMP_DIR"' EXIT
TEMP_MD="$TEMP_DIR/converted.md"

LATEX_HEADER="$TEMP_DIR/header.tex"
cat > "$LATEX_HEADER" <<'TEXEOF'
\usepackage{etoolbox}
\AtBeginEnvironment{longtable}{\small\sloppy\setlength{\emergencystretch}{2em}}
\AtBeginEnvironment{tabular}{\small\sloppy\setlength{\emergencystretch}{2em}}
TEXEOF

PANDOC_ARGS=(
    -f markdown
    -t pdf
    --pdf-engine="$PDF_ENGINE"
    -V geometry:margin="$MARGIN"
    -V fontsize="${FONT_SIZE}pt"
    -V colorlinks=true
    -V linkcolor=blue
    -V urlcolor=blue
    -V toccolor=black
    --highlight-style=tango
    --include-in-header="$LATEX_HEADER"
)

LUA_FILTER="$SCRIPT_DIR/pandoc-table-wrap.lua"
if [ -f "$LUA_FILTER" ]; then
    PANDOC_ARGS+=(--lua-filter="$LUA_FILTER")
fi

if [ "$TOC" = true ]; then
    PANDOC_ARGS+=(--toc --toc-depth=3)
    print_info "Table of contents enabled"
fi

if grep -q '```mermaid' "$INPUT_FILE"; then
    print_info "Mermaid diagrams detected, exporting to images..."

    if [ ! -f "$EXPORT_SCRIPT" ]; then
        print_error "Mermaid export script not found: $EXPORT_SCRIPT"
        exit 1
    fi

    IMAGE_DIR="$TEMP_DIR/images"
    PUPPETEER_CFG="$TEMP_DIR/puppeteer-config.json"

    # Build puppeteer config with auto-detected Chrome
    PUPPETEER_ARGS=""
    CHROME_PATH=$(detect_chrome 2>/dev/null) || true
    if [ -n "$CHROME_PATH" ]; then
        cat > "$PUPPETEER_CFG" <<PPEOF
{
  "executablePath": "$CHROME_PATH",
  "args": ["--no-sandbox", "--disable-setuid-sandbox"]
}
PPEOF
        PUPPETEER_ARGS="--puppeteer-config $PUPPETEER_CFG"
        print_info "Chrome detected: $CHROME_PATH"
    else
        print_warning "No Chrome/Chromium found. Mermaid will try its bundled browser."
        print_warning "If rendering fails, install Chrome or set PUPPETEER_EXECUTABLE_PATH."
    fi

    print_info "Rendering Mermaid diagrams (PNG)..."
    # shellcheck disable=SC2086
    python3 "$EXPORT_SCRIPT" --input "$INPUT_FILE_ABS" --output "$IMAGE_DIR" --format png $PUPPETEER_ARGS 2>&1 | grep -v "^$" || true

    # Count how many images were actually rendered
    RENDERED_COUNT=0
    if [ -d "$IMAGE_DIR" ]; then
        RENDERED_COUNT=$(find "$IMAGE_DIR" -name "*.png" 2>/dev/null | wc -l | tr -d ' ')
    fi

    print_info "Replacing diagram code blocks with image references..."
    python3 -c "
import re, sys
from pathlib import Path

md_file = Path('$INPUT_FILE_ABS')
image_dir = Path('$IMAGE_DIR')
output_file = Path('$TEMP_MD')

content = md_file.read_text()
diagram_count = 0
replaced = 0

def replace_mermaid(match):
    global diagram_count, replaced
    diagram_count += 1
    rel_path = md_file.stem
    img_path = image_dir / rel_path / f'diagram-{diagram_count:02d}.png'
    if img_path.exists():
        replaced += 1
        return f'![Diagram {diagram_count}]({img_path}){{ width=100% }}'
    else:
        print(f'Warning: Image not found for diagram {diagram_count}, keeping as code block', file=sys.stderr)
        return match.group(0)

pattern = r'\`\`\`mermaid\n.*?\n\`\`\`'
new_content = re.sub(pattern, replace_mermaid, content, flags=re.DOTALL)

# Replace emoji with text equivalents for LaTeX compatibility
emoji_map = {
    '\u2705': '[OK]',       # ✅
    '\u26A0\uFE0F': '[!]',  # ⚠️
    '\u26A0': '[!]',        # ⚠ (without variation selector)
    '\u2192': '->',         # →
    '\u2194': '<->',        # ↔
    '\u2713': '[v]',        # ✓
    '\u2717': '[x]',        # ✗
    '\u25CF': '*',          # ●
}
for emoji, text in emoji_map.items():
    new_content = new_content.replace(emoji, text)

output_file.write_text(new_content)
print(f'Replaced {replaced}/{diagram_count} diagram(s) with images')
"

    if [ "$RENDERED_COUNT" -gt 0 ]; then
        print_info "Successfully rendered $RENDERED_COUNT diagram image(s)"
    else
        print_warning "No diagram images were rendered. Mermaid code blocks will appear as text in the PDF."
    fi

    print_info "Generating PDF with embedded images..."
    pandoc "${PANDOC_ARGS[@]}" -o "$OUTPUT_FILE" "$TEMP_MD"
else
    print_info "No Mermaid diagrams found, doing direct conversion..."

    python3 -c "
from pathlib import Path
content = Path('$INPUT_FILE_ABS').read_text()
emoji_map = {
    '\u2705': '[OK]', '\u26A0\uFE0F': '[!]', '\u26A0': '[!]',
    '\u2192': '->', '\u2194': '<->', '\u2713': '[v]', '\u2717': '[x]', '\u25CF': '*',
}
for emoji, text in emoji_map.items():
    content = content.replace(emoji, text)
Path('$TEMP_MD').write_text(content)
"
    pandoc "${PANDOC_ARGS[@]}" -o "$OUTPUT_FILE" "$TEMP_MD"
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
