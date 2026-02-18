#!/usr/bin/env python3
"""
Gmail Watch Setup Script

Sets up Gmail push notifications to Pub/Sub for real-time email processing.
Gmail watch expires after 7 days and needs to be renewed.

Usage:
    python setup_gmail_watch.py [--project PROJECT_ID] [--topic TOPIC_NAME]

Prerequisites:
    1. Pub/Sub topic must exist and Gmail API must have publish permission
    2. Run: gcloud pubsub topics add-iam-policy-binding TOPIC_NAME \
             --member="serviceAccount:gmail-api-push@system.gserviceaccount.com" \
             --role="roles/pubsub.publisher"

The script will:
    1. Call Gmail API watch() to set up push notifications
    2. Return the expiration timestamp (when you need to renew)
    3. Store the history ID for tracking

Note: To automate renewal, set up a Cloud Scheduler job to run this script weekly.
"""

import argparse
import json
import logging
import os
from datetime import datetime

from utils import get_credentials_oauth, get_gmail_service

# ============================================================================
# CONFIGURATION
# ============================================================================

DEFAULT_PROJECT_ID = os.environ.get('GCP_PROJECT', 'YOUR_PROJECT_ID')
DEFAULT_TOPIC_NAME = 'amazon-order-emails'

# State file to track watch status
WATCH_STATE_FILE = '.gmail_watch_state.json'

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# WATCH OPERATIONS
# ============================================================================

def setup_gmail_watch(gmail_service, project_id: str, topic_name: str) -> dict:
    """
    Set up Gmail push notifications to Pub/Sub.

    Args:
        gmail_service: Authenticated Gmail API service
        project_id: GCP project ID
        topic_name: Pub/Sub topic name

    Returns:
        dict with historyId and expiration
    """
    topic_path = f'projects/{project_id}/topics/{topic_name}'

    logger.info(f"Setting up Gmail watch to: {topic_path}")

    watch_request = {
        'topicName': topic_path,
        'labelFilterBehavior': 'INCLUDE',
        'labelIds': ['INBOX']  # Watch inbox emails
    }

    try:
        response = gmail_service.users().watch(
            userId='me',
            body=watch_request
        ).execute()

        history_id = response.get('historyId')
        expiration = response.get('expiration')

        # Convert expiration to human-readable
        if expiration:
            exp_dt = datetime.fromtimestamp(int(expiration) / 1000)
            exp_str = exp_dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            exp_str = 'Unknown'

        logger.info(f"Watch set up successfully!")
        logger.info(f"  History ID: {history_id}")
        logger.info(f"  Expires: {exp_str}")

        return {
            'history_id': history_id,
            'expiration': expiration,
            'expiration_readable': exp_str,
            'topic': topic_path,
            'setup_time': datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Failed to set up watch: {e}")
        raise


def stop_gmail_watch(gmail_service) -> bool:
    """
    Stop the current Gmail watch.

    Returns:
        True if successful
    """
    try:
        gmail_service.users().stop(userId='me').execute()
        logger.info("Gmail watch stopped successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to stop watch: {e}")
        return False


def check_watch_status(gmail_service) -> dict:
    """
    Check if a watch is currently active.

    Note: Gmail API doesn't have a direct "get watch status" endpoint.
    We rely on the stored state file.
    """
    if not os.path.exists(WATCH_STATE_FILE):
        return {'active': False, 'message': 'No watch state file found'}

    try:
        with open(WATCH_STATE_FILE, 'r') as f:
            state = json.load(f)

        expiration = state.get('expiration')
        if expiration:
            exp_ts = int(expiration) / 1000
            if datetime.now().timestamp() > exp_ts:
                return {
                    'active': False,
                    'message': 'Watch has expired',
                    'expired_at': state.get('expiration_readable'),
                    'last_state': state
                }

        return {
            'active': True,
            'message': 'Watch appears active',
            'expires': state.get('expiration_readable'),
            'state': state
        }

    except Exception as e:
        return {'active': False, 'message': f'Error reading state: {e}'}


def save_watch_state(state: dict):
    """Save watch state to file."""
    with open(WATCH_STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)
    logger.info(f"Watch state saved to {WATCH_STATE_FILE}")


def load_watch_state() -> dict:
    """Load watch state from file."""
    if os.path.exists(WATCH_STATE_FILE):
        with open(WATCH_STATE_FILE, 'r') as f:
            return json.load(f)
    return {}

# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Set up Gmail push notifications to Pub/Sub',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python setup_gmail_watch.py --project my-project --topic amazon-order-emails
    python setup_gmail_watch.py --status
    python setup_gmail_watch.py --stop
    python setup_gmail_watch.py --renew

Note: Watch expires after 7 days. Set up Cloud Scheduler for automatic renewal.
        """
    )

    parser.add_argument('--project', default=DEFAULT_PROJECT_ID,
                        help='GCP project ID')
    parser.add_argument('--topic', default=DEFAULT_TOPIC_NAME,
                        help='Pub/Sub topic name')
    parser.add_argument('--status', action='store_true',
                        help='Check current watch status')
    parser.add_argument('--stop', action='store_true',
                        help='Stop the current watch')
    parser.add_argument('--renew', action='store_true',
                        help='Renew the watch (stop and restart)')

    args = parser.parse_args()

    # Validate project ID
    if args.project == 'YOUR_PROJECT_ID' and not args.status:
        logger.error("Please specify a project ID:")
        logger.error("  python setup_gmail_watch.py --project YOUR_PROJECT_ID")
        logger.error("  or set GCP_PROJECT environment variable")
        return

    # Authenticate
    logger.info("Authenticating with Google APIs...")
    creds = get_credentials_oauth()
    gmail_service = get_gmail_service(creds)

    # Handle status check
    if args.status:
        logger.info("")
        logger.info("Checking Gmail watch status...")
        status = check_watch_status(gmail_service)

        if status.get('active'):
            logger.info(f"Status: ACTIVE")
            logger.info(f"Expires: {status.get('expires')}")
        else:
            logger.info(f"Status: INACTIVE")
            logger.info(f"Reason: {status.get('message')}")

        return

    # Handle stop
    if args.stop:
        logger.info("")
        logger.info("Stopping Gmail watch...")
        success = stop_gmail_watch(gmail_service)
        if success and os.path.exists(WATCH_STATE_FILE):
            os.remove(WATCH_STATE_FILE)
            logger.info("Removed state file")
        return

    # Handle renew (stop then restart)
    if args.renew:
        logger.info("")
        logger.info("Renewing Gmail watch...")
        stop_gmail_watch(gmail_service)

    # Set up watch
    logger.info("")
    logger.info("=" * 50)
    logger.info("Setting up Gmail Watch")
    logger.info("=" * 50)

    try:
        state = setup_gmail_watch(gmail_service, args.project, args.topic)
        save_watch_state(state)

        logger.info("")
        logger.info("=" * 50)
        logger.info("Setup Complete!")
        logger.info("=" * 50)
        logger.info("")
        logger.info("Next steps:")
        logger.info(f"1. Watch will expire at: {state.get('expiration_readable')}")
        logger.info("2. Set up Cloud Scheduler to run this script weekly:")
        logger.info(f"   python setup_gmail_watch.py --renew --project {args.project} --topic {args.topic}")
        logger.info("")
        logger.info("To check status: python setup_gmail_watch.py --status")
        logger.info("To stop watch:   python setup_gmail_watch.py --stop")

    except Exception as e:
        logger.error("")
        logger.error("Setup Failed!")
        logger.error("")
        logger.error("Common issues:")
        logger.error("1. Pub/Sub topic doesn't exist. Create it:")
        logger.error(f"   gcloud pubsub topics create {args.topic}")
        logger.error("")
        logger.error("2. Gmail API doesn't have permission to publish. Grant it:")
        logger.error(f"   gcloud pubsub topics add-iam-policy-binding {args.topic} \\")
        logger.error(f"     --member='serviceAccount:gmail-api-push@system.gserviceaccount.com' \\")
        logger.error(f"     --role='roles/pubsub.publisher'")
        logger.error("")
        logger.error(f"Error details: {e}")


if __name__ == '__main__':
    main()
