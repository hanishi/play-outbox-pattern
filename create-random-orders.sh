#!/bin/bash

# Script to create 1000 random orders
# Usage: ./create-random-orders.sh [base_url] [count]

BASE_URL="${1:-http://localhost:9000}"
COUNT="${2:-1000}"
ENDPOINT="$BASE_URL/orders"

# Array of sample customer IDs
CUSTOMER_IDS=(
  "customer-001"
  "customer-002"
  "customer-003"
  "customer-004"
  "customer-005"
  "alice@example.com"
  "bob@example.com"
  "charlie@example.com"
  "david@example.com"
  "eve@example.com"
  "user-$(uuidgen | head -c 8)"
)

# Function to generate random amount between 10.00 and 500.00
generate_random_amount() {
  echo "$(( $RANDOM % 49000 + 1000 )).$(printf "%02d" $(( $RANDOM % 100 )))"
}

# Function to get random customer ID
get_random_customer() {
  local idx=$(( $RANDOM % ${#CUSTOMER_IDS[@]} ))
  echo "${CUSTOMER_IDS[$idx]}"
}

echo "Starting to create $COUNT random orders..."
echo "Target endpoint: $ENDPOINT"
echo "----------------------------------------"

SUCCESS_COUNT=0
FAILURE_COUNT=0
START_TIME=$(date +%s)

for i in $(seq 1 $COUNT); do
  CUSTOMER_ID=$(get_random_customer)
  AMOUNT=$(generate_random_amount)

  # Make the request
  RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$ENDPOINT" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "customerId=$CUSTOMER_ID&totalAmount=$AMOUNT")

  HTTP_CODE=$(echo "$RESPONSE" | tail -n1)

  if [ "$HTTP_CODE" = "200" ]; then
    ((SUCCESS_COUNT++))
    echo -ne "\r[$i/$COUNT] Success: $SUCCESS_COUNT | Failed: $FAILURE_COUNT"
  else
    ((FAILURE_COUNT++))
    echo -ne "\r[$i/$COUNT] Success: $SUCCESS_COUNT | Failed: $FAILURE_COUNT"
    # Uncomment to see error details
     echo -e "\nFailed request: HTTP $HTTP_CODE - Customer: $CUSTOMER_ID, Amount: $AMOUNT"
  fi

  # Optional: Add a small delay to avoid overwhelming the server
   sleep 0.05
done

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo -e "\n----------------------------------------"
echo "Completed!"
echo "Total requests: $COUNT"
echo "Successful: $SUCCESS_COUNT"
echo "Failed: $FAILURE_COUNT"
echo "Duration: ${DURATION}s"
echo "Rate: $(echo "scale=2; $COUNT / $DURATION" | bc) requests/second"
