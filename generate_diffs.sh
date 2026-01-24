#!/bin/bash

baseFile="/mnt/d/SFU/CMPT 473/a1/nats-server/server/util.go.clean"
mutatedDir="/mnt/d/SFU/CMPT 473/a1/nats-server/mutated"
outputFile="/mnt/d/SFU/CMPT 473/a1/nats-server/all_mutations_diff.txt"

# Clear the output file
> "$outputFile"

echo "Generating diffs between base file and all mutations..."
echo "Base file: $baseFile"
echo "Output file: $outputFile"
echo ""

# Loop through all mutation files sorted numerically
while IFS= read -r -d '' mutantFile; do
    # Skip base.go if it exists
    if [[ "$(basename "$mutantFile")" == "base.go" ]]; then
        continue
    fi
    
    # Extract just the filename without path and extension
    mutantName=$(basename "$mutantFile")
    
    echo "Processing mutation ${mutantName}..."
    
    # Add header for this mutation
    echo "====================================================" >> "$outputFile"
    echo "MUTATION: ${mutantName}" >> "$outputFile"
    echo "====================================================" >> "$outputFile"
    
    # Generate diff and append to output file
    diff -u "$baseFile" "$mutantFile" >> "$outputFile"
    
    # Add separator
    echo "" >> "$outputFile"
    echo "" >> "$outputFile"
done < <(find "$mutatedDir" -name "*.go" -print0 | sort -z -V)

echo ""
echo "All diffs generated and saved to: $outputFile"
echo "Total mutations processed: $(find "$mutatedDir" -name "*.go" | grep -v "base.go" | wc -l)"