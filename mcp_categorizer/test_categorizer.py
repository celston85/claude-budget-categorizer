#!/usr/bin/env python3
"""
Test script for the transaction categorizer.

Run this to verify the categorizer logic works before starting the MCP server.
"""

import sys
import os
import logging

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from categorizer import TransactionCategorizer, test_categorization
from config import COL_DESCRIPTION, COL_AMOUNT

logging.basicConfig(level=logging.INFO, format='%(message)s')


def test_with_real_config():
    """Test categorizer with actual config from Google Sheets."""
    from sheets_client import get_sheets_client
    
    print("=" * 60)
    print("Testing Categorizer with Real Config")
    print("=" * 60)
    print()
    
    try:
        client = get_sheets_client()
        
        # Load config
        print("Loading categories...")
        categories = client.get_categories()
        print(f"  Loaded {len(categories)} categories")
        
        print("Loading merchant rules...")
        merchant_rules = client.get_merchant_rules()
        print(f"  Loaded {len(merchant_rules)} merchant rules")
        
        print("Loading keywords...")
        keywords = client.get_keywords()
        print(f"  Loaded {len(keywords)} keyword rules")
        print()
        
        if not categories:
            print("ERROR: No categories found. Run setup_config_sheet.py first.")
            return False
        
        # Create categorizer
        categorizer = TransactionCategorizer(categories, merchant_rules, keywords)
        
        # Test transactions
        test_transactions = [
            {COL_DESCRIPTION: 'WHOLE FOODS MARKET #10234', COL_AMOUNT: '-$87.43', '_row_number': 1},
            {COL_DESCRIPTION: 'SHELL OIL 57442135', COL_AMOUNT: '-$45.00', '_row_number': 2},
            {COL_DESCRIPTION: 'STARBUCKS STORE 12345', COL_AMOUNT: '-$6.75', '_row_number': 3},
            {COL_DESCRIPTION: 'Amazon Basics Battery Charger', COL_AMOUNT: '-$24.99', '_row_number': 4},
            {COL_DESCRIPTION: 'TARGET', COL_AMOUNT: '-$123.45', '_row_number': 5},
            {COL_DESCRIPTION: 'NETFLIX.COM', COL_AMOUNT: '-$15.99', '_row_number': 6},
            {COL_DESCRIPTION: 'Random Store XYZ', COL_AMOUNT: '-$10.00', '_row_number': 7},
            {COL_DESCRIPTION: 'HOME DEPOT #1234', COL_AMOUNT: '-$234.56', '_row_number': 8},
            {COL_DESCRIPTION: 'UBER TRIP', COL_AMOUNT: '-$18.50', '_row_number': 9},
            {COL_DESCRIPTION: 'COSTCO WHSE #123', COL_AMOUNT: '-$456.78', '_row_number': 10},
        ]
        
        print("Testing categorization...")
        print()
        
        results = categorizer.categorize_batch(test_transactions)
        
        print("=== AUTO-CATEGORIZED ===")
        for trans in results['auto_categorized']:
            cat = trans['_categorization']
            print(f"  [{cat['source']:15}] {trans[COL_DESCRIPTION][:35]:35} → {cat['category_id']}")
        print()
        
        print("=== NEEDS CLAUDE ===")
        for trans in results['needs_claude']:
            cat = trans['_categorization']
            reason = cat.get('review_reason', 'No rules matched')
            print(f"  {trans[COL_DESCRIPTION][:45]:45} | {reason}")
        print()
        
        print("=== SUMMARY ===")
        print(f"  Auto-categorized: {len(results['auto_categorized'])}")
        print(f"  Needs Claude:     {len(results['needs_claude'])}")
        print(f"  Needs Review:     {len(results['needs_review'])}")
        print()
        
        return True
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_sheets_client():
    """Test Google Sheets connectivity."""
    from sheets_client import get_sheets_client
    
    print("=" * 60)
    print("Testing Sheets Client")
    print("=" * 60)
    print()
    
    try:
        client = get_sheets_client()
        
        # Test ensure columns
        print("Ensuring categorization columns exist...")
        client.ensure_categorization_columns()
        print("  OK")
        print()
        
        # Test get uncategorized
        print("Getting uncategorized transactions (limit 5)...")
        transactions = client.get_uncategorized_transactions(limit=5)
        print(f"  Found {len(transactions)} uncategorized transactions")
        
        if transactions:
            print("  Sample:")
            for t in transactions[:3]:
                print(f"    Row {t.get('_row_number')}: {t.get('Description', '')[:50]}")
        print()
        
        # Test stats
        print("Getting categorization stats...")
        stats = client.get_categorization_stats()
        print(f"  Total:         {stats['total']}")
        print(f"  Categorized:   {stats['categorized']}")
        print(f"  Uncategorized: {stats['uncategorized']}")
        print(f"  Progress:      {stats['percent_complete']}%")
        print()
        
        return True
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print()
    print("=" * 60)
    print("MCP Transaction Categorizer - Test Suite")
    print("=" * 60)
    print()
    
    # Test 1: Built-in categorizer test
    print("TEST 1: Categorizer Logic (built-in data)")
    print("-" * 40)
    test_categorization()
    print()
    
    # Check if config sheet is set up
    from config import BUDGET_CONFIG_SHEET_ID
    
    if not BUDGET_CONFIG_SHEET_ID:
        print()
        print("NOTE: BUDGET_CONFIG_SHEET_ID not set.")
        print("Run setup_config_sheet.py to create the config sheet,")
        print("then set the environment variable and re-run tests.")
        print()
        return
    
    # Test 2: Real config from sheets
    print()
    print("TEST 2: Categorizer with Real Config")
    print("-" * 40)
    test_with_real_config()
    
    # Test 3: Sheets client
    print()
    print("TEST 3: Sheets Client")
    print("-" * 40)
    test_sheets_client()
    
    print()
    print("=" * 60)
    print("All tests completed!")
    print("=" * 60)


if __name__ == '__main__':
    main()
