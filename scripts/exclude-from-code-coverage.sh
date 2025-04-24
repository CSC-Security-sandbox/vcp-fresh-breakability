#!/bin/sh
# Detect the operating system
if [ "$(uname)" = "Darwin" ]; then
  SED_COMMAND="sed -i ''"
else
  SED_COMMAND="sed -i"
fi

# Process each line in the exclude file
while read -r line
do
  $SED_COMMAND "\%$line%d" "coverage.out"
done < ./exclude-from-code-coverage