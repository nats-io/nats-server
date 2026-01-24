baseFilePath="/mnt/d/SFU/CMPT 473/a1/nats-server/server/util.go"
cleanFilePath="/mnt/d/SFU/CMPT 473/a1/nats-server/server/util.go.clean"
testsDirPath="/mnt/d/SFU/CMPT 473/a1/nats-server/test"
# tests=("(TestGoodBcryptToken|TestBadBcryptToken|TestGoodBcryptPassword|TestBadBcryptPassword|TestPasswordClientGoodConnect|TestBadPasswordClient|TestBadUserClient|TestNoUserOrPasswordClient|TestAuthClientFailOnEverythingElse|TestAuthClientGoodConnect|TestAuthClientNoConnect|TestAuthClientBadToken|TestNoAuthClient)")
mutantFileRoot="$(pwd)"
resultsDir="$mutantFileRoot/results/"
referenceFile="$resultsDir/referenceOutput"

# Arrays to store results
detectedMutants=()
survivedMutants=()

# # Run our tests and save the result to a reference file for sanity/diffing
# cd "$testsDirPath"
# ./_runTests.sh > "$referenceFile"

index=0
cd "$mutantFileRoot"
# Run all the tests
for file in $(find mutated -name '*.go'); do
    echo Testing for $file ...
    srcPath="$mutantFileRoot/$file"
    testOutputPath="$resultsDir/testoutput/$index"
    testResultsPath="$resultsDir/results/$index"

    # Restore clean file before applying mutant
    cp "$cleanFilePath" "$baseFilePath"
    # Apply the mutant
    cp "$srcPath" "$baseFilePath"

    # Run our tests
    cd "$testsDirPath"
    ./_runTests.sh > "$testOutputPath"

    diffLines=$(diff "$testOutputPath" "$referenceFile" -y --suppress-common-lines | wc -l)

    if (($diffLines > 1)); then
        echo !!! Mutant identified: $file !!!
        detectedMutants+=("$file")
        # Save the diff for later review
        diff "$testOutputPath" "$referenceFile" > "$resultsDir/diff_$index.txt"
    else
        echo $file is not a mutant
        survivedMutants+=("$file")
    fi

    # Increment the index of our test, head back to the root
    index=$((index + 1))
    cd "$mutantFileRoot"
done

echo ""
echo "=================================================="
echo "                MUTATION TESTING SUMMARY"
echo "=================================================="
echo "Total files tested: $index"
echo "Mutants detected (killed by tests): ${#detectedMutants[@]}"
echo "Mutants survived (not caught by tests): ${#survivedMutants[@]}"

if [ ${#detectedMutants[@]} -gt 0 ]; then
    echo ""
    echo "DETECTED MUTANTS (Good - tests caught these):"
    echo "--------------------------------------------"
    for mutant in "${detectedMutants[@]}"; do
        echo "  ✓ $mutant"
    done
fi

if [ ${#survivedMutants[@]} -gt 0 ]; then
    echo ""
    echo "SURVIVED MUTANTS (Bad - tests missed these):"
    echo "-------------------------------------------"
    for mutant in "${survivedMutants[@]}"; do
        echo "  ✗ $mutant"
    done
    echo ""
    echo "WARNING: ${#survivedMutants[@]} mutant(s) survived! These indicate potential test gaps."
fi

# Calculate mutation score
if [ $index -gt 0 ]; then
    mutationScore=$((${#detectedMutants[@]} * 100 / $index))
    echo ""
    echo "MUTATION SCORE: ${mutationScore}% (${#detectedMutants[@]}/$index mutants killed)"
fi

echo ""
echo "Detailed diffs saved in: $resultsDir/diff_*.txt"
echo "=================================================="

# Restore the clean file when done
echo "Restoring clean file..."
cp "$cleanFilePath" "$baseFilePath"