#!/bin/bash

files=$(git status -s --porcelain)
if [ -z "$files" ]; then
    echo "✅ No untracked files detected. Working directory is clean."
    exit 0
fi

unknown=$(echo "$files" | grep "\?\?" | awk '{ print $2; }')

echo "❌ Error: Untracked files detected after generating code:"
echo "$unknown" | sed 's/^/  📁 /'
echo ""
echo "⚠️  The working directory contains untracked files that should be committed."
echo "💡 Please review the files above, run 'make generate' if needed, and commit the changes."
exit 1
