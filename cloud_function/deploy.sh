#!/bin/bash
#
# Deploy the Email Ingestion Cloud Function
#
# Prerequisites:
# 1. gcloud CLI installed and authenticated
# 2. GCP project configured
# 3. Required APIs enabled (Cloud Functions, Pub/Sub, Gmail, Sheets)
# 4. Service account with appropriate permissions
#
# Usage:
#   ./deploy.sh [PROJECT_ID] [REGION]
#
# Environment variables to set:
#   PARSED_ORDERS_SHEET_ID - Google Sheet ID for parsed orders
#   GMAIL_USER - Gmail user email (or 'me' for service account)

set -e

# Configuration
PROJECT_ID="${1:-$(gcloud config get-value project)}"
REGION="${2:-us-central1}"
FUNCTION_NAME="amazon-email-parser"
PUBSUB_TOPIC="amazon-order-emails"
RUNTIME="python311"
MEMORY="256MB"
TIMEOUT="60s"

# Environment variables for the function
PARSED_ORDERS_SHEET_ID="${PARSED_ORDERS_SHEET_ID:-YOUR_SHEET_ID_HERE}"

echo "=== Amazon Email Parser Cloud Function Deployment ==="
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo "Function: $FUNCTION_NAME"
echo "Topic: $PUBSUB_TOPIC"
echo ""

# Check if project is set
if [ -z "$PROJECT_ID" ] || [ "$PROJECT_ID" == "(unset)" ]; then
    echo "Error: No GCP project set. Run: gcloud config set project YOUR_PROJECT_ID"
    exit 1
fi

# Enable required APIs
echo "Enabling required APIs..."
gcloud services enable cloudfunctions.googleapis.com --project="$PROJECT_ID"
gcloud services enable pubsub.googleapis.com --project="$PROJECT_ID"
gcloud services enable gmail.googleapis.com --project="$PROJECT_ID"
gcloud services enable sheets.googleapis.com --project="$PROJECT_ID"
gcloud services enable cloudbuild.googleapis.com --project="$PROJECT_ID"

# Create Pub/Sub topic if it doesn't exist
echo "Creating Pub/Sub topic..."
if ! gcloud pubsub topics describe "$PUBSUB_TOPIC" --project="$PROJECT_ID" 2>/dev/null; then
    gcloud pubsub topics create "$PUBSUB_TOPIC" --project="$PROJECT_ID"
    echo "Created topic: $PUBSUB_TOPIC"
else
    echo "Topic already exists: $PUBSUB_TOPIC"
fi

# Grant Gmail permission to publish to the topic
echo "Granting Gmail API permission to publish..."
gcloud pubsub topics add-iam-policy-binding "$PUBSUB_TOPIC" \
    --project="$PROJECT_ID" \
    --member="serviceAccount:gmail-api-push@system.gserviceaccount.com" \
    --role="roles/pubsub.publisher" \
    2>/dev/null || echo "Permission may already exist"

# Deploy the Cloud Function
echo ""
echo "Deploying Cloud Function..."
gcloud functions deploy "$FUNCTION_NAME" \
    --project="$PROJECT_ID" \
    --region="$REGION" \
    --runtime="$RUNTIME" \
    --trigger-topic="$PUBSUB_TOPIC" \
    --entry-point="process_gmail_notification" \
    --memory="$MEMORY" \
    --timeout="$TIMEOUT" \
    --set-env-vars="PARSED_ORDERS_SHEET_ID=$PARSED_ORDERS_SHEET_ID,GCP_PROJECT=$PROJECT_ID" \
    --source="."

echo ""
echo "=== Deployment Complete ==="
echo ""
echo "Next steps:"
echo "1. Create the Parsed Orders Google Sheet and note its ID"
echo "2. Share the sheet with the Cloud Function service account"
echo "3. Update PARSED_ORDERS_SHEET_ID environment variable if needed:"
echo "   gcloud functions deploy $FUNCTION_NAME --update-env-vars PARSED_ORDERS_SHEET_ID=YOUR_SHEET_ID"
echo "4. Set up Gmail watch using setup_gmail_watch.py"
echo ""
echo "To view logs:"
echo "   gcloud functions logs read $FUNCTION_NAME --region=$REGION"
