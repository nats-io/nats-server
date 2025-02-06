#!/bin/bash

# Ensure script is run at the root of a git repository
git rev-parse --is-inside-work-tree &>/dev/null || { echo "Not inside a git repository"; exit 1; }

# Find all .go files tracked by git
git ls-files "*.go" | while read -r file; do
    # Skip files that don't have a copyright belonging to "The NATS Authors"
    current_copyright=$(grep -oE "^// Copyright [0-9]{4}(-[0-9]{4})? The NATS Authors" "$file" || echo "")
    [[ -z "$current_copyright" ]] && continue

    # Get the last commit year for the file
    last_year=$(git log --follow --format="%ad" --date=format:%Y -- "$file" | head -1)
    existing_years=$(echo "$current_copyright" | grep -oE "[0-9]{4}(-[0-9]{4})?")

    # Determine the new copyright range
    if [[ "$existing_years" =~ ^([0-9]{4})-([0-9]{4})$ ]]; then
        first_year=${BASH_REMATCH[1]}
        new_copyright="// Copyright $first_year-$last_year The NATS Authors"
    elif [[ "$existing_years" =~ ^([0-9]{4})$ ]]; then
        first_year=${BASH_REMATCH[1]}
        if [[ "$first_year" == "$last_year" ]]; then
            new_copyright="// Copyright $first_year The NATS Authors"
        else
            new_copyright="// Copyright $first_year-$last_year The NATS Authors"
        fi
    else
        continue # If the format is somehow incorrect, skip the file
    fi

    # Update the first line
    if sed --version &>/dev/null; then
        # Linux sed
        sed -i "1s|^// Copyright.*|$new_copyright|" "$file"
    else
        # BSD/macOS sed, needs -i ''
        sed -i '' "1s|^// Copyright.*|$new_copyright|" "$file"
    fi
done
