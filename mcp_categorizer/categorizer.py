#!/usr/bin/env python3
"""
Transaction categorization logic with layered determinism.

Priority order:
1. Merchant rules (100% deterministic)
2. Keyword rules (deterministic)
3. Claude fallback (non-deterministic but constrained)
"""

import re
import logging
from typing import List, Dict, Any, Optional, Tuple

from config import (
    CONFIDENCE_HIGH,
    CONFIDENCE_MEDIUM,
    CONFIDENCE_LOW,
    AMBIGUOUS_MERCHANTS,
    COL_DESCRIPTION,
    COL_AMOUNT,
)

logger = logging.getLogger(__name__)


class TransactionCategorizer:
    """
    Categorizes transactions using a layered approach:
    1. Merchant rules (deterministic)
    2. Keyword rules (deterministic)  
    3. Returns remaining for Claude to handle
    """
    
    def __init__(
        self,
        categories: List[Dict[str, Any]],
        merchant_rules: List[Dict[str, Any]],
        keyword_rules: List[Dict[str, Any]]
    ):
        """
        Initialize with configuration from sheets.

        Args:
            categories: List of category dicts
            merchant_rules: List of merchant rule dicts
            keyword_rules: List of keyword rule dicts
        """
        self.categories = {c['category_id']: c for c in categories}
        self.category_ids = set(self.categories.keys())

        # Sort merchant rules by pattern length (longest first) for deterministic matching
        # This ensures "whole foods market" matches before "whole" or "foods"
        self.merchant_rules = sorted(
            merchant_rules,
            key=lambda r: len(r.get('merchant_pattern', '')),
            reverse=True
        )

        # Sort keyword rules by priority (highest first), then by length for tie-breaking
        self.keyword_rules = sorted(
            keyword_rules,
            key=lambda r: (r.get('priority', 0), len(r.get('keyword', ''))),
            reverse=True
        )

        logger.info(
            f"Initialized categorizer with {len(self.categories)} categories, "
            f"{len(self.merchant_rules)} merchant rules (sorted by specificity), "
            f"{len(self.keyword_rules)} keyword rules (sorted by priority)"
        )
    
    def validate_category(self, category_id: str) -> bool:
        """Check if a category_id is valid."""
        return category_id in self.category_ids
    
    def get_category_taxonomy(self) -> List[Dict[str, Any]]:
        """
        Get the full category taxonomy for Claude.
        
        Returns:
            List of categories with hierarchy information
        """
        # Group by parent
        by_parent = {}
        for cat in self.categories.values():
            parent = cat.get('parent_category', '') or 'Other'
            if parent not in by_parent:
                by_parent[parent] = []
            by_parent[parent].append(cat)
        
        return {
            'categories': list(self.categories.values()),
            'by_parent': by_parent,
            'valid_ids': list(self.category_ids)
        }
    
    def apply_merchant_rules(
        self, 
        description: str
    ) -> Optional[Dict[str, Any]]:
        """
        Try to match a transaction description against merchant rules.
        
        Args:
            description: Transaction description
            
        Returns:
            Match result or None if no match
        """
        desc_lower = description.lower()
        
        for rule in self.merchant_rules:
            pattern = rule['merchant_pattern']
            
            # Check if pattern matches (substring match)
            if pattern in desc_lower:
                category_id = rule['category_id']
                
                # Validate category exists
                if not self.validate_category(category_id):
                    logger.warning(
                        f"Merchant rule '{pattern}' references invalid "
                        f"category '{category_id}'"
                    )
                    continue
                
                return {
                    'category_id': category_id,
                    'category_name': self.categories[category_id]['category_name'],
                    'source': 'merchant_rule',
                    'confidence': rule['confidence'],
                    'matched_pattern': pattern,
                    'needs_review': rule['confidence'] < CONFIDENCE_HIGH,
                    'review_reason': f"Merchant rule confidence: {rule['confidence']}" 
                        if rule['confidence'] < CONFIDENCE_HIGH else None
                }
        
        return None
    
    def apply_keyword_rules(
        self, 
        description: str
    ) -> Optional[Dict[str, Any]]:
        """
        Try to match transaction description against keyword rules.
        
        Args:
            description: Transaction description
            
        Returns:
            Match result or None if no match
        """
        desc_lower = description.lower()
        
        matches = []
        for rule in self.keyword_rules:
            keyword = rule['keyword']
            
            # Word boundary match (not substring)
            if re.search(rf'\b{re.escape(keyword)}\b', desc_lower):
                if self.validate_category(rule['category_id']):
                    matches.append(rule)
        
        if not matches:
            return None
        
        # Sort by priority (higher = better)
        matches.sort(key=lambda x: x['priority'], reverse=True)
        best = matches[0]
        
        return {
            'category_id': best['category_id'],
            'category_name': self.categories[best['category_id']]['category_name'],
            'source': 'keyword',
            'confidence': min(70, 50 + best['priority']),  # Cap at 70
            'matched_keyword': best['keyword'],
            'needs_review': True,  # Always review keyword matches
            'review_reason': f"Keyword match: '{best['keyword']}'"
        }
    
    def is_ambiguous_merchant(self, description: str) -> Tuple[bool, Optional[str]]:
        """
        Check if the transaction is from an ambiguous merchant.
        
        Returns:
            (is_ambiguous, merchant_name)
        """
        desc_lower = description.lower()
        
        for merchant in AMBIGUOUS_MERCHANTS:
            if merchant in desc_lower:
                return True, merchant
        
        return False, None
    
    def categorize_transaction(
        self, 
        transaction: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Attempt to categorize a single transaction.
        
        Args:
            transaction: Dict with Description, Amount, etc.
            
        Returns:
            Categorization result with:
            - category_id (None if needs Claude)
            - category_name
            - source
            - confidence
            - needs_review
            - review_reason
            - needs_claude (True if rules couldn't handle it)
        """
        description = transaction.get(COL_DESCRIPTION, '')
        
        if not description:
            return {
                'category_id': None,
                'source': None,
                'needs_claude': True,
                'needs_review': True,
                'review_reason': 'Empty description'
            }
        
        # Step 1: Try merchant rules (most deterministic)
        merchant_result = self.apply_merchant_rules(description)
        if merchant_result and merchant_result['confidence'] >= CONFIDENCE_HIGH:
            merchant_result['needs_claude'] = False
            return merchant_result
        
        # Step 2: Check if ambiguous merchant (Amazon, Target, Costco, Walmart)
        # These are sent to Claude for categorization, but flagged for review
        # Note: Parsed Amazon items have product names, not "AMAZON MARKETPLACE",
        # so they won't hit this check and will go through normal rules.
        is_ambiguous, merchant = self.is_ambiguous_merchant(description)
        if is_ambiguous:
            return {
                'category_id': None,
                'source': None,
                'needs_claude': True,  # Let Claude try to categorize
                'needs_review': True,  # Flag for review since it's ambiguous
                'review_reason': f"Ambiguous merchant: {merchant}",
                'merchant_rule_hint': merchant_result  # Pass along if we have one
            }
        
        # Step 3: Try keyword rules
        keyword_result = self.apply_keyword_rules(description)
        if keyword_result:
            keyword_result['needs_claude'] = False
            return keyword_result
        
        # Step 4: Return for Claude to handle
        return {
            'category_id': None,
            'source': None,
            'needs_claude': True,
            'needs_review': False,
            'review_reason': None,
            'merchant_rule_hint': merchant_result  # Pass along low-confidence match
        }
    
    def categorize_batch(
        self,
        transactions: List[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Categorize a batch of transactions.

        Separates into:
        - auto_categorized: Handled by rules, ready to write
        - needs_claude: Need Claude's judgment (unknown merchants + ambiguous)
        - needs_review: Have a category but should be reviewed

        Args:
            transactions: List of transaction dicts

        Returns:
            Dict with categorized lists
        """
        auto_categorized = []
        needs_claude = []
        needs_review = []

        for trans in transactions:
            result = self.categorize_transaction(trans)

            # Attach result to transaction
            trans_with_result = {
                **trans,
                '_categorization': result
            }

            if result.get('needs_claude'):
                # Unknown or ambiguous - needs Claude's judgment
                needs_claude.append(trans_with_result)
            elif result.get('needs_review'):
                needs_review.append(trans_with_result)
                auto_categorized.append(trans_with_result)
            elif result.get('category_id'):
                # Successfully categorized by rules
                auto_categorized.append(trans_with_result)

        logger.info(
            f"Batch results: {len(auto_categorized)} auto-categorized, "
            f"{len(needs_claude)} need Claude, "
            f"{len(needs_review)} need review"
        )

        return {
            'auto_categorized': auto_categorized,
            'needs_claude': needs_claude,
            'needs_review': needs_review,
        }
    
    def format_for_claude(
        self, 
        transactions: List[Dict[str, Any]]
    ) -> str:
        """
        Format transactions for Claude to categorize.
        
        Args:
            transactions: List of transactions needing Claude's judgment
            
        Returns:
            Formatted string for Claude
        """
        lines = []
        
        for i, trans in enumerate(transactions, 1):
            desc = trans.get(COL_DESCRIPTION, 'Unknown')
            amount = trans.get(COL_AMOUNT, '')
            row_num = trans.get('_row_number', '?')
            
            # Include hint if available
            hint = ''
            cat_info = trans.get('_categorization', {})
            if cat_info.get('review_reason'):
                hint = f" (Note: {cat_info['review_reason']})"
            
            lines.append(f"{i}. [Row {row_num}] {desc} | {amount}{hint}")
        
        return '\n'.join(lines)


def test_categorization():
    """Test the categorizer with sample data."""
    # Sample categories
    categories = [
        {'category_id': 'groceries', 'category_name': 'Groceries', 'parent_category': 'Food', 'description': ''},
        {'category_id': 'restaurants', 'category_name': 'Restaurants', 'parent_category': 'Food', 'description': ''},
        {'category_id': 'electronics', 'category_name': 'Electronics', 'parent_category': 'Shopping', 'description': ''},
        {'category_id': 'gas', 'category_name': 'Gas & Fuel', 'parent_category': 'Transportation', 'description': ''},
    ]
    
    # Sample merchant rules
    merchant_rules = [
        {'merchant_pattern': 'whole foods', 'category_id': 'groceries', 'confidence': 100, 'notes': ''},
        {'merchant_pattern': 'trader joe', 'category_id': 'groceries', 'confidence': 100, 'notes': ''},
        {'merchant_pattern': 'shell', 'category_id': 'gas', 'confidence': 100, 'notes': ''},
        {'merchant_pattern': 'chevron', 'category_id': 'gas', 'confidence': 100, 'notes': ''},
    ]
    
    # Sample keyword rules
    keyword_rules = [
        {'keyword': 'battery', 'category_id': 'electronics', 'priority': 10},
        {'keyword': 'charger', 'category_id': 'electronics', 'priority': 10},
    ]
    
    categorizer = TransactionCategorizer(categories, merchant_rules, keyword_rules)
    
    # Test transactions
    test_transactions = [
        {COL_DESCRIPTION: 'WHOLE FOODS MARKET #123', COL_AMOUNT: '-$45.67', '_row_number': 1},
        {COL_DESCRIPTION: 'SHELL OIL 123456', COL_AMOUNT: '-$52.00', '_row_number': 2},
        {COL_DESCRIPTION: 'Amazon Basics Battery Pack', COL_AMOUNT: '-$12.99', '_row_number': 3},
        {COL_DESCRIPTION: 'TARGET', COL_AMOUNT: '-$34.56', '_row_number': 4},  # Ambiguous
        {COL_DESCRIPTION: 'Random Store ABC', COL_AMOUNT: '-$10.00', '_row_number': 5},  # Unknown
    ]
    
    results = categorizer.categorize_batch(test_transactions)
    
    print("\n=== Auto-Categorized ===")
    for trans in results['auto_categorized']:
        cat = trans['_categorization']
        print(f"  {trans[COL_DESCRIPTION][:30]}: {cat['category_id']} ({cat['source']}, {cat['confidence']}%)")
    
    print("\n=== Needs Claude ===")
    for trans in results['needs_claude']:
        cat = trans['_categorization']
        print(f"  {trans[COL_DESCRIPTION][:30]}: {cat.get('review_reason', 'No rules matched')}")
    
    print("\n=== Needs Review ===")
    for trans in results['needs_review']:
        cat = trans['_categorization']
        print(f"  {trans[COL_DESCRIPTION][:30]}: {cat['review_reason']}")


if __name__ == '__main__':
    test_categorization()
