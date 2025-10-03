#!/bin/bash

# Documentation Sync Check Script
# This script helps developers identify when documentation needs to be updated

set -e

echo "🔍 Checking documentation sync..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Check if we're in a git repository
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    print_status $RED "❌ Not in a git repository"
    exit 1
fi

# Get the list of changed files
changed_files=$(git diff --name-only HEAD~1 HEAD 2>/dev/null || git diff --name-only --cached)

if [ -z "$changed_files" ]; then
    print_status $YELLOW "⚠️  No changed files detected. Make sure you have staged changes or are comparing with a previous commit."
    exit 0
fi

echo "📁 Changed files:"
echo "$changed_files" | sed 's/^/  - /'

# Check for Go file changes
go_files=$(echo "$changed_files" | grep '\.go$' || true)
doc_files=$(echo "$changed_files" | grep '^doc/' || true)

if [ -n "$go_files" ] && [ -z "$doc_files" ]; then
    print_status $YELLOW "⚠️  Go files were changed but no documentation was updated"
    echo ""
    echo "Please consider updating documentation for:"
    echo ""
    
    # Check for specific types of changes
    if echo "$go_files" | grep -q "workflow"; then
        print_status $BLUE "  📋 Workflow changes detected"
        echo "    - Update doc/workflows/ directory"
        echo "    - Update workflow documentation"
    fi
    
    if echo "$go_files" | grep -q "api\|handler\|endpoint"; then
        print_status $BLUE "  🌐 API changes detected"
        echo "    - Update doc/api/ directory"
        echo "    - Update OpenAPI specifications"
    fi
    
    if echo "$go_files" | grep -q "config\|env\|yaml"; then
        print_status $BLUE "  ⚙️  Configuration changes detected"
        echo "    - Update configuration documentation"
        echo "    - Update environment variable documentation"
    fi
    
    if echo "$go_files" | grep -q "model\|struct\|type"; then
        print_status $BLUE "  📊 Data model changes detected"
        echo "    - Update architecture documentation"
        echo "    - Update data model documentation"
    fi
    
    echo ""
    print_status $YELLOW "💡 Quick commands:"
    echo "  make docs-update    # Get guidance on what to update"
    echo "  make docs-check     # Run documentation checks"
    echo "  make link-check     # Check for broken links"
    echo ""
    print_status $YELLOW "📖 See doc/guides/documentation-updates.md for detailed instructions"
    
    # Don't fail the script, just warn
    exit 0
fi

# Check for broken links
print_status $BLUE "🔗 Checking for broken links..."
if make link-check > /dev/null 2>&1; then
    print_status $GREEN "✅ All documentation links are valid"
else
    print_status $RED "❌ Broken links found in documentation"
    echo ""
    echo "Run 'make link-check' to see the issues:"
    make link-check
    exit 1
fi

# Check for missing documentation
print_status $BLUE "📚 Checking for missing documentation..."

missing_docs=0

# Check for workflow files without documentation
for go_file in $go_files; do
    if echo "$go_file" | grep -q "workflow"; then
        basename=$(basename "$go_file" .go)
        if [ ! -f "doc/workflows/core/${basename}.md" ] && [ ! -f "doc/workflows/background/${basename}.md" ]; then
            print_status $YELLOW "  ⚠️  Missing documentation for workflow: $go_file"
            missing_docs=1
        fi
    fi
done

# Check for API files without documentation
for go_file in $go_files; do
    if echo "$go_file" | grep -q "api\|handler"; then
        if [ ! -f "doc/api/endpoints.md" ]; then
            print_status $YELLOW "  ⚠️  API documentation may need updating: doc/api/endpoints.md"
            missing_docs=1
        fi
    fi
done

if [ $missing_docs -eq 0 ]; then
    print_status $GREEN "✅ Documentation appears to be up to date"
else
    print_status $YELLOW "⚠️  Some documentation may be missing or outdated"
fi

echo ""
print_status $GREEN "🎉 Documentation sync check complete!"

# Provide helpful next steps
echo ""
print_status $BLUE "📋 Next steps:"
echo "  1. Review the suggestions above"
echo "  2. Update documentation as needed"
echo "  3. Run 'make link-check' to verify links"
echo "  4. Test any code examples in documentation"
echo "  5. Commit your changes"

exit 0