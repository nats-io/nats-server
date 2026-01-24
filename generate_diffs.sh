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

# Loop through all mutation files
for i in {00..99}; do
    mutantFile="$mutatedDir/${i}.go"
    
    if [ -f "$mutantFile" ]; then
        echo "Processing mutation ${i}.go..."
        
        # Add header for this mutation
        echo "====================================================" >> "$outputFile"
        echo "MUTATION: ${i}.go" >> "$outputFile"
        echo "====================================================" >> "$outputFile"
        
        # Generate diff and append to output file
        diff -u "$baseFile" "$mutantFile" >> "$outputFile"
        
        # Add separator
        echo "" >> "$outputFile"
        echo "" >> "$outputFile"
    else
        echo "Warning: $mutantFile not found, skipping..."
    fi
done

echo ""
echo "All diffs generated and saved to: $outputFile"
echo "Total mutations processed: $(find "$mutatedDir" -name "*.go" | grep -v "base.go" | wc -l)"