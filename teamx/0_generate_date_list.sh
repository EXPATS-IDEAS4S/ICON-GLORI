#!/bin/bash

# Script to generate a list of dates with initialization times
# Format: YYYYMMDD_HH

# Configuration
START_DATE="20250424"
END_DATE="20250930"
INITS=("12")  # Initialization times (00 or 12)
OUTPUT_FILE="date_list.txt"

# Function to display usage
usage() {
    echo "Usage: $0 [START_DATE] [END_DATE] [OUTPUT_FILE]"
    echo ""
    echo "Arguments:"
    echo "  START_DATE   Start date in YYYYMMDD format (default: ${START_DATE})"
    echo "  END_DATE     End date in YYYYMMDD format (default: ${END_DATE})"
    echo "  OUTPUT_FILE  Output filename (default: ${OUTPUT_FILE})"
    echo ""
    echo "Example:"
    echo "  $0 20250101 20250930 date_list.txt"
    echo ""
    echo "Initialization times are: ${INITS[@]}"
    exit 1
}

# Parse command line arguments
if [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
    usage
fi

if [ -n "$1" ]; then
    START_DATE="$1"
fi

if [ -n "$2" ]; then
    END_DATE="$2"
fi

if [ -n "$3" ]; then
    OUTPUT_FILE="$3"
fi

# Validate date format
validate_date() {
    local date=$1
    if ! [[ $date =~ ^[0-9]{8}$ ]]; then
        echo "ERROR: Invalid date format '$date'. Must be YYYYMMDD"
        exit 1
    fi
    
    # Check if date is valid using 'date' command
    if ! date -d "${date:0:4}-${date:4:2}-${date:6:2}" &>/dev/null; then
        echo "ERROR: Invalid date '$date'"
        exit 1
    fi
}

echo "=== Date List Generator ==="
echo "Start Date: ${START_DATE}"
echo "End Date:   ${END_DATE}"
echo "Init Times: ${INITS[@]}"
echo "Output:     ${OUTPUT_FILE}"
echo ""

# Validate dates
validate_date "${START_DATE}"
validate_date "${END_DATE}"

# Check if start date is before or equal to end date
if [ "${START_DATE}" -gt "${END_DATE}" ]; then
    echo "ERROR: Start date must be before or equal to end date"
    exit 1
fi

# Clear output file
> "${OUTPUT_FILE}"

# Generate date list
echo "Generating date list..."

CURRENT_DATE="${START_DATE}"
COUNT=0

while [ "${CURRENT_DATE}" -le "${END_DATE}" ]; do
    # Add entries for each initialization time
    for INIT in "${INITS[@]}"; do
        echo "${CURRENT_DATE}_${INIT}" >> "${OUTPUT_FILE}"
        ((COUNT++))
    done
    
    # Increment to next day
    CURRENT_DATE=$(date -d "${CURRENT_DATE:0:4}-${CURRENT_DATE:4:2}-${CURRENT_DATE:6:2} + 1 day" +%Y%m%d)
done

echo "✓ Generated ${COUNT} date entries"
echo "✓ Saved to: ${OUTPUT_FILE}"
echo ""
echo "First 5 entries:"
head -n 5 "${OUTPUT_FILE}"
echo "..."
echo "Last 5 entries:"
tail -n 5 "${OUTPUT_FILE}"
