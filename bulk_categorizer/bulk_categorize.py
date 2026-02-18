#!/usr/bin/env python3
"""
Bulk Transaction Categorizer using Claude API.

Processes large batches of transactions that would be too slow for MCP/Claude Desktop.

Usage:
    python bulk_categorize.py                    # Run with defaults
    python bulk_categorize.py --dry-run          # Test without writing
    python bulk_categorize.py --batch-size 50    # Smaller batches
    python bulk_categorize.py --resume           # Resume from last progress
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from typing import Dict, Any, List, Optional

# Add parent directories to path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'mcp_categorizer'))

from bulk_config import (
    BATCH_SIZE,
    MAX_BATCHES,
    BATCH_DELAY,
    RULE_LEARNING_THRESHOLD,
    MIN_PATTERN_LENGTH,
    MAX_CONSECUTIVE_ERRORS,
    PROGRESS_FILE,
)
from api_client import ClaudeCategorizer

# Import from mcp_categorizer
from categorizer import TransactionCategorizer
from sheets_client import SheetsClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(script_dir, 'bulk_categorize.log'))
    ]
)
logger = logging.getLogger(__name__)


class BulkCategorizer:
    """Orchestrates bulk categorization of transactions."""

    def __init__(
        self,
        batch_size: int = BATCH_SIZE,
        max_batches: int = MAX_BATCHES,
        dry_run: bool = False,
        learn_rules: bool = True,
        filter_parent: Optional[str] = None
    ):
        self.batch_size = batch_size
        self.max_batches = max_batches
        self.dry_run = dry_run
        self.learn_rules = learn_rules
        self.filter_parent = filter_parent

        # Initialize clients
        self.sheets = SheetsClient()
        self.claude = ClaudeCategorizer()
        self.categorizer = None  # Lazy loaded

        # Statistics
        self.stats = {
            'started_at': datetime.now().isoformat(),
            'total_processed': 0,
            'rule_categorized': 0,
            'claude_categorized': 0,
            'rules_learned': 0,
            'errors': 0,
            'batches_completed': 0,
        }

        # Track merchant patterns for rule learning
        self.merchant_patterns: Dict[str, Dict[str, int]] = {}

    def _load_categorizer(self) -> TransactionCategorizer:
        """Load the categorizer with current rules."""
        categories = self.sheets.get_categories()
        merchant_rules = self.sheets.get_merchant_rules()
        keyword_rules = self.sheets.get_keywords()

        self.categorizer = TransactionCategorizer(
            categories=categories,
            merchant_rules=merchant_rules,
            keyword_rules=keyword_rules
        )
        return self.categorizer

    def _get_categories(self) -> List[Dict[str, Any]]:
        """Get category list for prompts."""
        return self.sheets.get_categories()

    def _save_progress(self, processed_rows: List[int], seen_rows: List[int] = None, offset: int = 0):
        """Save progress for resume capability."""
        progress = {
            'timestamp': datetime.now().isoformat(),
            'stats': self.stats,
            'processed_rows': processed_rows,
            'seen_rows': seen_rows or processed_rows,
            'offset': offset,
        }
        with open(PROGRESS_FILE, 'w') as f:
            json.dump(progress, f, indent=2)

    def _load_progress(self) -> Optional[Dict[str, Any]]:
        """Load progress from previous run."""
        if not os.path.exists(PROGRESS_FILE):
            return None

        try:
            with open(PROGRESS_FILE, 'r') as f:
                progress = json.load(f)
            logger.info(f"Loaded progress from {progress['timestamp']}")
            return {
                'processed_rows': progress.get('processed_rows', []),
                'seen_rows': progress.get('seen_rows', progress.get('processed_rows', [])),
                'offset': progress.get('offset', 0),
            }
        except Exception as e:
            logger.warning(f"Failed to load progress: {e}")
            return None

    def _track_merchant_pattern(self, categorizations: List[Dict[str, Any]], transactions: List[Dict[str, Any]]):
        """
        Track merchant patterns from Claude's categorizations (without creating rules yet).
        Call _commit_learned_rules() at the end to create rules in batch.
        """
        if not self.learn_rules:
            return

        # Build row -> transaction map
        txn_map = {
            t.get('row_number') or t.get('_row_number'): t
            for t in transactions
        }

        for cat in categorizations:
            row = cat.get('row_number')
            category_id = cat.get('category_id')

            txn = txn_map.get(row)
            if not txn:
                continue

            desc = txn.get('Description', '').lower()
            if not desc:
                continue

            # Extract pattern (first 2-3 words)
            words = desc.split()[:3]
            pattern = ' '.join(words)

            if len(pattern) < MIN_PATTERN_LENGTH:
                continue

            # Track pattern -> category counts
            if pattern not in self.merchant_patterns:
                self.merchant_patterns[pattern] = {}

            self.merchant_patterns[pattern][category_id] = \
                self.merchant_patterns[pattern].get(category_id, 0) + 1

    def _commit_learned_rules(self):
        """
        Create merchant rules from tracked patterns (call once at end of run).
        This batches all rule creation to minimize API calls.
        """
        if not self.learn_rules or self.dry_run:
            return

        # Get existing rules once (using cache)
        existing_rules = {r['merchant_pattern'] for r in self.sheets.get_merchant_rules(use_cache=True)}

        rules_to_add = []
        for pattern, cat_counts in self.merchant_patterns.items():
            top_category = max(cat_counts, key=cat_counts.get)
            count = cat_counts[top_category]

            if count >= RULE_LEARNING_THRESHOLD and pattern not in existing_rules:
                # Check if covered by existing rule
                is_covered = any(pattern in r or r in pattern for r in existing_rules)
                if is_covered:
                    continue

                rules_to_add.append((pattern, top_category, count))
                existing_rules.add(pattern)  # Prevent duplicates in this batch

        # Add rules in batch
        logger.info(f"Adding {len(rules_to_add)} learned merchant rules...")
        for pattern, top_category, count in rules_to_add:
            result = self.sheets.add_merchant_rule(
                merchant_pattern=pattern,
                category_id=top_category,
                confidence=90,
                notes=f"Auto-learned from {count} bulk categorizations",
                skip_duplicate_check=True  # We already checked above
            )

            if result.get('success'):
                logger.info(f"Learned rule: '{pattern}' -> {top_category}")
                self.stats['rules_learned'] += 1

    def run(self, resume: bool = False) -> Dict[str, Any]:
        """
        Run the bulk categorization process.

        Args:
            resume: Whether to resume from previous progress

        Returns:
            Statistics dict
        """
        logger.info("=" * 60)
        logger.info("BULK CATEGORIZATION STARTED")
        logger.info(f"Batch size: {self.batch_size}, Max batches: {self.max_batches}")
        logger.info(f"Dry run: {self.dry_run}, Learn rules: {self.learn_rules}")
        if self.filter_parent:
            logger.info(f"Filter: {self.filter_parent} categories only")
        logger.info("=" * 60)

        # Build category filter if specified
        allowed_category_ids = set()
        if self.filter_parent:
            categories = self._get_categories()
            for cat in categories:
                if cat.get('parent_category', '').lower() == self.filter_parent.lower():
                    allowed_category_ids.add(cat['category_id'])
            logger.info(f"Allowed categories: {allowed_category_ids}")

        # Load categorizer
        self._load_categorizer()
        categories = self._get_categories()
        logger.info(f"Loaded {len(self.categorizer.merchant_rules)} merchant rules")
        logger.info(f"Loaded {len(categories)} categories")

        # Load progress if resuming
        processed_rows = set()  # Rows we've actually categorized
        seen_rows = set()       # Rows we've seen (for pagination with filters)
        current_offset = 0
        if resume:
            prev_progress = self._load_progress()
            if prev_progress:
                processed_rows = set(prev_progress.get('processed_rows', []))
                seen_rows = set(prev_progress.get('seen_rows', []))
                current_offset = prev_progress.get('offset', 0)
                logger.info(f"Resuming with {len(processed_rows)} processed, {len(seen_rows)} seen, offset {current_offset}")

        consecutive_errors = 0
        batch_num = 0

        # Determine if we need offset-based pagination
        # Only needed when filtering (filtered-out rows stay uncategorized)
        use_offset_pagination = bool(allowed_category_ids)

        while batch_num < self.max_batches:
            batch_num += 1
            logger.info(f"\n--- Batch {batch_num}/{self.max_batches} ---")

            # Get uncategorized transactions
            # When not filtering: always offset=0 (categorized rows disappear from results)
            # When filtering: use offset to advance past rows we've seen but skipped
            fetch_offset = current_offset if use_offset_pagination else 0

            try:
                transactions = self.sheets.get_uncategorized_transactions(
                    limit=self.batch_size,
                    offset=fetch_offset
                )
            except Exception as e:
                if '429' in str(e) or 'Quota exceeded' in str(e):
                    logger.warning("Google Sheets rate limit hit, waiting 60 seconds...")
                    time.sleep(60)
                    transactions = self.sheets.get_uncategorized_transactions(
                        limit=self.batch_size,
                        offset=fetch_offset
                    )
                else:
                    raise

            if not transactions:
                logger.info("No more uncategorized transactions!")
                break

            # Track all rows we've seen for this batch
            batch_row_numbers = [
                t.get('row_number') or t.get('_row_number')
                for t in transactions
            ]

            # Filter out already seen rows (for resume or when using offset pagination)
            if seen_rows and use_offset_pagination:
                transactions = [
                    t for t in transactions
                    if (t.get('row_number') or t.get('_row_number')) not in seen_rows
                ]

            if not transactions:
                # All transactions in this batch were already seen, advance offset
                current_offset += self.batch_size
                logger.info(f"All fetched transactions already seen, advancing to offset {current_offset}")
                continue

            logger.info(f"Processing {len(transactions)} transactions (fetch_offset: {fetch_offset})")

            # Mark these rows as seen (for filtering mode)
            if use_offset_pagination:
                for row_num in batch_row_numbers:
                    if row_num:
                        seen_rows.add(row_num)

                # Advance offset for next batch (only in filtering mode)
                current_offset += self.batch_size

            # Step 1: Apply deterministic rules first
            results = self.categorizer.categorize_batch(transactions)

            auto_categorized = results['auto_categorized']
            needs_claude = results['needs_claude']

            # Apply parent category filter if specified
            if allowed_category_ids:
                # Filter rule-matched to only allowed categories
                auto_categorized = [
                    t for t in auto_categorized
                    if t['_categorization']['category_id'] in allowed_category_ids
                ]
                # For needs_claude, use keyword heuristics to filter likely matches
                food_keywords = [
                    'food', 'grocery', 'grocer', 'market', 'cafe', 'coffee',
                    'restaurant', 'pizza', 'burger', 'taco', 'sushi', 'thai',
                    'chinese', 'mexican', 'italian', 'doordash', 'ubereats',
                    'grubhub', 'instacart', 'whole foods', 'trader joe',
                    'safeway', 'kroger', 'publix', 'aldi', 'costco', 'target',
                    'walmart', 'bakery', 'deli', 'bar', 'brew', 'wine', 'liquor',
                    'dining', 'eat', 'kitchen', 'grill', 'bistro', 'diner'
                ]
                needs_claude = [
                    t for t in needs_claude
                    if any(kw in t.get('Description', '').lower() for kw in food_keywords)
                ]
                logger.info(f"  After filter - Rules: {len(auto_categorized)}, Claude: {len(needs_claude)}")

            logger.info(f"  Rules matched: {len(auto_categorized)}")
            logger.info(f"  Needs Claude: {len(needs_claude)}")

            # Write rule-based categorizations
            if auto_categorized and not self.dry_run:
                updates = []
                for t in auto_categorized:
                    cat_info = t['_categorization']
                    updates.append({
                        'row_number': t.get('_row_number'),
                        'category_id': cat_info['category_id'],
                        'source': cat_info['source'],
                        'confidence': cat_info['confidence'],
                        'needs_review': cat_info.get('needs_review', False),
                        'review_reason': cat_info.get('review_reason', '')
                    })
                self.sheets.write_categories(updates)
                self.stats['rule_categorized'] += len(updates)

            # Track processed rows
            for t in auto_categorized:
                processed_rows.add(t.get('_row_number'))

            # Step 2: Send remaining to Claude API
            if needs_claude:
                try:
                    categorizations = self.claude.categorize_batch(
                        transactions=needs_claude,
                        categories=categories
                    )

                    logger.info(f"  Claude categorized: {len(categorizations)}")

                    # Write Claude categorizations
                    if categorizations and not self.dry_run:
                        self.sheets.write_categories(categorizations)
                        self.stats['claude_categorized'] += len(categorizations)

                        # Track patterns for rule learning (rules created at end)
                        self._track_merchant_pattern(categorizations, needs_claude)

                    # Track processed rows
                    for c in categorizations:
                        processed_rows.add(c.get('row_number'))

                    consecutive_errors = 0

                except Exception as e:
                    logger.error(f"Error calling Claude API: {e}")
                    self.stats['errors'] += 1
                    consecutive_errors += 1

                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                        logger.error(f"Too many consecutive errors ({consecutive_errors}), stopping")
                        break

            self.stats['total_processed'] += len(transactions)
            self.stats['batches_completed'] = batch_num

            # Save progress
            self._save_progress(list(processed_rows), list(seen_rows), current_offset)

            # Delay between batches
            if batch_num < self.max_batches:
                time.sleep(BATCH_DELAY)

        # Commit learned rules in batch (reduces API calls)
        self._commit_learned_rules()

        # Final summary
        self.stats['finished_at'] = datetime.now().isoformat()
        self.stats['api_usage'] = self.claude.get_usage_stats()

        logger.info("\n" + "=" * 60)
        logger.info("BULK CATEGORIZATION COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Total processed: {self.stats['total_processed']}")
        logger.info(f"Rule categorized: {self.stats['rule_categorized']}")
        logger.info(f"Claude categorized: {self.stats['claude_categorized']}")
        logger.info(f"Rules learned: {self.stats['rules_learned']}")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info(f"API cost: ${self.stats['api_usage']['estimated_cost_usd']:.4f}")

        return self.stats


def main():
    parser = argparse.ArgumentParser(
        description='Bulk categorize transactions using Claude API'
    )
    parser.add_argument(
        '--batch-size', '-b',
        type=int,
        default=BATCH_SIZE,
        help=f'Transactions per batch (default: {BATCH_SIZE})'
    )
    parser.add_argument(
        '--max-batches', '-m',
        type=int,
        default=MAX_BATCHES,
        help=f'Maximum batches to process (default: {MAX_BATCHES})'
    )
    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help='Test without writing to sheets'
    )
    parser.add_argument(
        '--no-learn-rules',
        action='store_true',
        help='Disable automatic merchant rule learning'
    )
    parser.add_argument(
        '--resume', '-r',
        action='store_true',
        help='Resume from previous progress'
    )
    parser.add_argument(
        '--filter-parent', '-f',
        type=str,
        help='Only process categories under this parent (e.g., "Food")'
    )

    args = parser.parse_args()

    # Check for API key - try keychain first, then env var
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        # Try macOS Keychain
        import subprocess
        try:
            result = subprocess.run(
                ['security', 'find-generic-password', '-s', os.environ.get('KEYCHAIN_SERVICE_NAME', 'budget-categorizer-api-key'), '-w'],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                api_key = result.stdout.strip()
                os.environ['ANTHROPIC_API_KEY'] = api_key  # Set for downstream use
                print("✓ API key loaded from macOS Keychain")
        except Exception:
            pass

    if not api_key:
        print("ERROR: No API key found")
        print("Options:")
        print("  1. Set ANTHROPIC_API_KEY environment variable")
        print("  2. Store in macOS Keychain: security add-generic-password -a \"$USER\" -s \"budget-categorizer-api-key\" -w \"sk-ant-...\"")
        sys.exit(1)

    # Run categorizer
    categorizer = BulkCategorizer(
        batch_size=args.batch_size,
        max_batches=args.max_batches,
        dry_run=args.dry_run,
        learn_rules=not args.no_learn_rules,
        filter_parent=args.filter_parent
    )

    try:
        stats = categorizer.run(resume=args.resume)

        # Save final stats
        stats_file = os.path.join(script_dir, 'last_run_stats.json')
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2)

        print(f"\nStats saved to: {stats_file}")

    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
