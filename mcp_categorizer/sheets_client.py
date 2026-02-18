#!/usr/bin/env python3
"""
Google Sheets client for MCP Transaction Categorizer.

Wraps the existing utils.py authentication and adds categorization-specific operations.
"""

import sys
import os
import logging
import time
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import (
    get_credentials_oauth,
    get_sheets_service,
    column_index_to_letter,
)

from config import (
    BUDGET_CONFIG_SHEET_ID,
    PROCESSED_TRANSACTIONS_SHEET_ID,
    PROCESSED_TRANSACTIONS_SHEET_NAME,
    CONFIG_CATEGORIES_TAB,
    CONFIG_MERCHANT_RULES_TAB,
    CONFIG_KEYWORDS_TAB,
    CATEGORIZATION_COLUMNS,
    COL_CLAUDE_CATEGORY,
    COL_CATEGORY_SOURCE,
    COL_CATEGORY_CONFIDENCE,
    COL_CATEGORIZED_AT,
    COL_CATEGORIZED_BY,
    COL_NEEDS_REVIEW,
    COL_REVIEW_REASON,
    COL_PREVIOUS_CATEGORY,
    COL_DESCRIPTION,
    COL_AMOUNT,
    COL_DATE,
    COL_ACCOUNT,
)

logger = logging.getLogger(__name__)


class SheetsClient:
    """Client for all Google Sheets operations."""

    HEADERS_CACHE_TTL = 300  # 5 minutes

    def __init__(self):
        self._creds = None
        self._service = None
        self._headers_cache = {}
        self._headers_cache_time = {}
        self._merchant_rules_cache = None
        self._categories_cache = None
        self._columns_verified = False  # Track if categorization columns exist
    
    def _get_service(self):
        """Get authenticated Sheets service (lazy initialization)."""
        if self._service is None:
            # Use absolute paths for credentials (parent directory of mcp_categorizer)
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            credentials_file = os.path.join(project_root, 'credentials.json')
            token_file = os.path.join(project_root, 'token.json')
            
            self._creds = get_credentials_oauth(
                credentials_file=credentials_file,
                token_file=token_file
            )
            self._service = get_sheets_service(self._creds)
        return self._service
    
    # ========================================================================
    # CONFIG SHEET OPERATIONS
    # ========================================================================
    
    def get_categories(self) -> List[Dict[str, Any]]:
        """
        Get all categories from config sheet.
        
        Returns:
            List of category dicts with keys: category_id, category_name, 
            parent_category, description
        """
        service = self._get_service()
        
        try:
            result = service.spreadsheets().values().get(
                spreadsheetId=BUDGET_CONFIG_SHEET_ID,
                range=f"'{CONFIG_CATEGORIES_TAB}'!A:E"
            ).execute()

            values = result.get('values', [])
            if not values or len(values) < 2:
                logger.warning("No categories found in config sheet")
                return []

            headers = values[0]
            categories = []

            # Build header index map for flexible column ordering
            header_map = {h.strip().lower(): i for i, h in enumerate(headers)}

            for row in values[1:]:
                if not row or not row[0]:
                    continue

                padded_row = row + [''] * (len(headers) - len(row))

                # Get values by header name (flexible ordering)
                cat_id_idx = header_map.get('category_id', 0)
                cat_name_idx = header_map.get('category_name', 2)
                parent_idx = header_map.get('parent_category', 1)
                desc_idx = header_map.get('description', 3)
                budget_idx = header_map.get('monthly_budget')

                # Parse monthly_budget as float
                monthly_budget = None
                if budget_idx is not None and budget_idx < len(padded_row):
                    budget_str = padded_row[budget_idx].strip().replace('$', '').replace(',', '')
                    if budget_str:
                        try:
                            monthly_budget = float(budget_str)
                        except ValueError:
                            pass

                categories.append({
                    'category_id': padded_row[cat_id_idx].strip() if cat_id_idx < len(padded_row) else '',
                    'category_name': padded_row[cat_name_idx].strip() if cat_name_idx < len(padded_row) else '',
                    'parent_category': padded_row[parent_idx].strip() if parent_idx < len(padded_row) else '',
                    'description': padded_row[desc_idx].strip() if desc_idx < len(padded_row) else '',
                    'monthly_budget': monthly_budget,
                })
            
            logger.info(f"Loaded {len(categories)} categories")
            return categories
            
        except Exception as e:
            logger.error(f"Error loading categories: {e}")
            return []
    
    def get_merchant_rules(self, use_cache: bool = False) -> List[Dict[str, Any]]:
        """
        Get all merchant rules from config sheet.

        Args:
            use_cache: If True, return cached rules if available (reduces API calls)

        Returns:
            List of rule dicts with keys: merchant_pattern, category_id,
            confidence, notes
        """
        if use_cache and self._merchant_rules_cache is not None:
            return self._merchant_rules_cache

        service = self._get_service()

        try:
            result = service.spreadsheets().values().get(
                spreadsheetId=BUDGET_CONFIG_SHEET_ID,
                range=f"'{CONFIG_MERCHANT_RULES_TAB}'!A:D"
            ).execute()

            values = result.get('values', [])
            if not values or len(values) < 2:
                logger.warning("No merchant rules found in config sheet")
                return []

            rules = []

            for row in values[1:]:
                if not row or not row[0]:
                    continue

                padded_row = row + [''] * (4 - len(row))

                # Parse confidence as int, default to 100
                try:
                    confidence = int(padded_row[2]) if padded_row[2] else 100
                except ValueError:
                    confidence = 100

                rules.append({
                    'merchant_pattern': padded_row[0].strip().lower(),
                    'category_id': padded_row[1].strip(),
                    'confidence': confidence,
                    'notes': padded_row[3].strip(),
                })

            logger.info(f"Loaded {len(rules)} merchant rules")
            self._merchant_rules_cache = rules
            return rules

        except Exception as e:
            logger.error(f"Error loading merchant rules: {e}")
            return []

    def invalidate_merchant_rules_cache(self):
        """Clear the merchant rules cache."""
        self._merchant_rules_cache = None
    
    def get_keywords(self) -> List[Dict[str, Any]]:
        """
        Get all keyword rules from config sheet.
        
        Returns:
            List of keyword dicts with keys: keyword, category_id, priority
        """
        service = self._get_service()
        
        try:
            result = service.spreadsheets().values().get(
                spreadsheetId=BUDGET_CONFIG_SHEET_ID,
                range=f"'{CONFIG_KEYWORDS_TAB}'!A:C"
            ).execute()
            
            values = result.get('values', [])
            if not values or len(values) < 2:
                logger.info("No keyword rules found in config sheet")
                return []
            
            keywords = []
            
            for row in values[1:]:
                if not row or not row[0]:
                    continue
                    
                padded_row = row + [''] * (3 - len(row))
                
                try:
                    priority = int(padded_row[2]) if padded_row[2] else 10
                except ValueError:
                    priority = 10
                
                keywords.append({
                    'keyword': padded_row[0].strip().lower(),
                    'category_id': padded_row[1].strip(),
                    'priority': priority,
                })
            
            logger.info(f"Loaded {len(keywords)} keyword rules")
            return keywords
            
        except Exception as e:
            logger.error(f"Error loading keywords: {e}")
            return []
    
    # ========================================================================
    # TRANSACTION SHEET OPERATIONS
    # ========================================================================
    
    def _get_headers(self, force_refresh: bool = False) -> Tuple[List[str], Dict[str, int]]:
        """
        Get headers from processed transactions sheet.
        
        Returns:
            (headers list, column index mapping)
        """
        cache_key = 'processed_transactions'
        
        cache_age = time.time() - self._headers_cache_time.get(cache_key, 0)
        if not force_refresh and cache_key in self._headers_cache and cache_age < self.HEADERS_CACHE_TTL:
            return self._headers_cache[cache_key]
        
        service = self._get_service()
        
        result = service.spreadsheets().values().get(
            spreadsheetId=PROCESSED_TRANSACTIONS_SHEET_ID,
            range=f"'{PROCESSED_TRANSACTIONS_SHEET_NAME}'!1:1"
        ).execute()
        
        headers = result.get('values', [[]])[0]
        col_indices = {h: i for i, h in enumerate(headers)}
        
        self._headers_cache[cache_key] = (headers, col_indices)
        self._headers_cache_time[cache_key] = time.time()
        return headers, col_indices
    
    def ensure_categorization_columns(self, force_check: bool = False):
        """
        Ensure all categorization columns exist in the processed transactions sheet.
        Adds missing columns to the end.

        Args:
            force_check: If True, always check. If False, skip if already verified this session.
        """
        # Skip if already verified this session (reduces API calls)
        if self._columns_verified and not force_check:
            return

        service = self._get_service()
        headers, col_indices = self._get_headers(force_refresh=force_check)

        missing_columns = [col for col in CATEGORIZATION_COLUMNS if col not in col_indices]

        if not missing_columns:
            logger.info("All categorization columns already exist")
            self._columns_verified = True
            return

        # Add missing columns
        new_headers = headers + missing_columns

        # Write the new header row
        service.spreadsheets().values().update(
            spreadsheetId=PROCESSED_TRANSACTIONS_SHEET_ID,
            range=f"'{PROCESSED_TRANSACTIONS_SHEET_NAME}'!1:1",
            valueInputOption='RAW',
            body={'values': [new_headers]}
        ).execute()

        logger.info(f"Added categorization columns: {missing_columns}")

        # Refresh cache
        self._get_headers(force_refresh=True)
        self._columns_verified = True
    
    def get_uncategorized_transactions(
        self, 
        limit: int = 50, 
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Get transactions that don't have a claude_category yet.
        
        Args:
            limit: Max number of transactions to return
            offset: Number of transactions to skip (for pagination)
            
        Returns:
            List of transaction dicts with row_number for writing back
        """
        service = self._get_service()
        headers, col_indices = self._get_headers()
        
        # Ensure categorization columns exist
        if COL_CLAUDE_CATEGORY not in col_indices:
            self.ensure_categorization_columns()
            headers, col_indices = self._get_headers(force_refresh=True)
        
        # Read all data
        result = service.spreadsheets().values().get(
            spreadsheetId=PROCESSED_TRANSACTIONS_SHEET_ID,
            range=f"'{PROCESSED_TRANSACTIONS_SHEET_NAME}'!A:Z"
        ).execute()
        
        values = result.get('values', [])
        if len(values) < 2:
            return []
        
        # Find uncategorized rows
        uncategorized = []
        claude_cat_idx = col_indices.get(COL_CLAUDE_CATEGORY)
        category_source_idx = col_indices.get(COL_CATEGORY_SOURCE)
        
        for row_num, row in enumerate(values[1:], start=2):  # Start at 2 (1-indexed, skip header)
            padded_row = row + [''] * (len(headers) - len(row))
            
            # Skip if already categorized
            if claude_cat_idx is not None and claude_cat_idx < len(padded_row):
                if padded_row[claude_cat_idx].strip():
                    continue
            
            # Skip if manually categorized (protect manual overrides)
            if category_source_idx is not None and category_source_idx < len(padded_row):
                if padded_row[category_source_idx].strip().lower() == 'manual':
                    continue
            
            # Build transaction dict
            trans = {'_row_number': row_num}
            for header in headers:
                idx = col_indices.get(header)
                if idx is not None and idx < len(padded_row):
                    trans[header] = padded_row[idx]
                else:
                    trans[header] = ''
            
            uncategorized.append(trans)
        
        # Apply offset and limit
        uncategorized = uncategorized[offset:offset + limit]
        
        logger.info(f"Found {len(uncategorized)} uncategorized transactions")
        return uncategorized
    
    def write_categories(
        self, 
        updates: List[Dict[str, Any]],
        batch_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Write category updates back to the sheet.
        
        Args:
            updates: List of dicts with keys:
                - row_number: The row to update
                - category_id: The category to assign
                - source: 'merchant_rule' | 'keyword' | 'claude'
                - confidence: 0-100
                - needs_review: bool
                - review_reason: str (optional)
            batch_id: Optional batch identifier for undo capability
                
        Returns:
            Dict with success count, error count, errors list
        """
        service = self._get_service()
        headers, col_indices = self._get_headers()
        
        # Ensure columns exist
        self.ensure_categorization_columns()
        headers, col_indices = self._get_headers(force_refresh=True)
        
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        batch_id = batch_id or f"batch_{timestamp.replace(' ', '_').replace(':', '-')}"
        
        # Build batch update data
        batch_data = []
        errors = []
        
        for update in updates:
            row_num = update.get('row_number')
            if not row_num:
                errors.append({'error': 'Missing row_number', 'update': update})
                continue
            
            try:
                # Build cell updates for this row
                cells = [
                    (COL_CLAUDE_CATEGORY, update.get('category_id', '')),
                    (COL_CATEGORY_SOURCE, update.get('source', 'claude')),
                    (COL_CATEGORY_CONFIDENCE, str(update.get('confidence', ''))),
                    (COL_CATEGORIZED_AT, timestamp),
                    (COL_CATEGORIZED_BY, f"mcp_v1:{batch_id}"),
                    (COL_NEEDS_REVIEW, 'TRUE' if update.get('needs_review') else ''),
                    (COL_REVIEW_REASON, update.get('review_reason', '')),
                ]
                
                # Store previous category for undo
                if COL_PREVIOUS_CATEGORY in col_indices:
                    # Read current value first
                    prev_idx = col_indices[COL_CLAUDE_CATEGORY]
                    prev_col_letter = column_index_to_letter(prev_idx)
                    # We'll batch this separately for efficiency
                
                for col_name, value in cells:
                    if col_name not in col_indices:
                        continue
                    
                    col_letter = column_index_to_letter(col_indices[col_name])
                    batch_data.append({
                        'range': f"'{PROCESSED_TRANSACTIONS_SHEET_NAME}'!{col_letter}{row_num}",
                        'values': [[value]]
                    })
                    
            except Exception as e:
                errors.append({'error': str(e), 'row': row_num})
        
        # Execute batch update
        if batch_data:
            try:
                service.spreadsheets().values().batchUpdate(
                    spreadsheetId=PROCESSED_TRANSACTIONS_SHEET_ID,
                    body={
                        'valueInputOption': 'RAW',
                        'data': batch_data
                    }
                ).execute()
            except Exception as e:
                logger.error(f"Batch update failed: {e}")
                return {
                    'success_count': 0,
                    'error_count': len(updates),
                    'errors': [{'error': str(e)}]
                }
        
        success_count = len(updates) - len(errors)
        logger.info(f"Wrote {success_count} categories, {len(errors)} errors")
        
        return {
            'success_count': success_count,
            'error_count': len(errors),
            'errors': errors,
            'batch_id': batch_id
        }
    
    def get_categorization_stats(self) -> Dict[str, Any]:
        """
        Get statistics about categorization progress.
        
        Returns:
            Dict with counts and breakdowns
        """
        service = self._get_service()
        headers, col_indices = self._get_headers()
        
        result = service.spreadsheets().values().get(
            spreadsheetId=PROCESSED_TRANSACTIONS_SHEET_ID,
            range=f"'{PROCESSED_TRANSACTIONS_SHEET_NAME}'!A:Z"
        ).execute()
        
        values = result.get('values', [])
        if len(values) < 2:
            return {'total': 0, 'categorized': 0, 'uncategorized': 0}
        
        total = len(values) - 1  # Exclude header
        
        claude_cat_idx = col_indices.get(COL_CLAUDE_CATEGORY)
        source_idx = col_indices.get(COL_CATEGORY_SOURCE)
        review_idx = col_indices.get(COL_NEEDS_REVIEW)
        
        categorized = 0
        uncategorized = 0
        by_source = {'merchant_rule': 0, 'keyword': 0, 'claude': 0, 'manual': 0}
        needs_review = 0
        
        for row in values[1:]:
            padded_row = row + [''] * (len(headers) - len(row))
            
            # Check if categorized
            if claude_cat_idx and claude_cat_idx < len(padded_row):
                if padded_row[claude_cat_idx].strip():
                    categorized += 1
                    
                    # Count by source
                    if source_idx and source_idx < len(padded_row):
                        source = padded_row[source_idx].strip().lower()
                        if source in by_source:
                            by_source[source] += 1
                else:
                    uncategorized += 1
            else:
                uncategorized += 1
            
            # Count needs review
            if review_idx and review_idx < len(padded_row):
                if padded_row[review_idx].strip().upper() == 'TRUE':
                    needs_review += 1
        
        return {
            'total': total,
            'categorized': categorized,
            'uncategorized': uncategorized,
            'percent_complete': round(categorized / total * 100, 1) if total > 0 else 0,
            'by_source': by_source,
            'needs_review': needs_review,
        }
    
    def flag_for_review(
        self, 
        row_number: int, 
        reason: str
    ) -> bool:
        """
        Flag a transaction for human review.
        
        Args:
            row_number: The row to flag
            reason: Why it needs review
            
        Returns:
            True if successful
        """
        service = self._get_service()
        headers, col_indices = self._get_headers()
        
        if COL_NEEDS_REVIEW not in col_indices or COL_REVIEW_REASON not in col_indices:
            self.ensure_categorization_columns()
            headers, col_indices = self._get_headers(force_refresh=True)
        
        review_col = column_index_to_letter(col_indices[COL_NEEDS_REVIEW])
        reason_col = column_index_to_letter(col_indices[COL_REVIEW_REASON])
        
        try:
            service.spreadsheets().values().batchUpdate(
                spreadsheetId=PROCESSED_TRANSACTIONS_SHEET_ID,
                body={
                    'valueInputOption': 'RAW',
                    'data': [
                        {
                            'range': f"'{PROCESSED_TRANSACTIONS_SHEET_NAME}'!{review_col}{row_number}",
                            'values': [['TRUE']]
                        },
                        {
                            'range': f"'{PROCESSED_TRANSACTIONS_SHEET_NAME}'!{reason_col}{row_number}",
                            'values': [[reason]]
                        }
                    ]
                }
            ).execute()
            return True
            
        except Exception as e:
            logger.error(f"Failed to flag row {row_number}: {e}")
            return False
    
    # ========================================================================
    # CONFIG SHEET WRITE OPERATIONS
    # ========================================================================
    
    def add_merchant_rule(
        self,
        merchant_pattern: str,
        category_id: str,
        confidence: int = 100,
        notes: str = '',
        skip_duplicate_check: bool = False
    ) -> Dict[str, Any]:
        """
        Add a new merchant rule to the Budget Config sheet.

        Args:
            merchant_pattern: Text pattern to match (will be lowercased)
            category_id: Category to assign
            confidence: 0-100 confidence level
            notes: Optional notes about the rule
            skip_duplicate_check: If True, skip API call to check for duplicates (for bulk ops)

        Returns:
            Dict with success status and details
        """
        service = self._get_service()

        # Normalize the pattern
        merchant_pattern = merchant_pattern.strip().lower()

        # Validate minimum pattern length to prevent false positives
        MIN_PATTERN_LENGTH = 4
        if len(merchant_pattern) < MIN_PATTERN_LENGTH:
            return {
                'success': False,
                'error': f"Pattern '{merchant_pattern}' too short (min {MIN_PATTERN_LENGTH} chars). "
                         f"Short patterns cause false positives (e.g., 'cal' matching 'CalDigit').",
                'suggestion': 'Use a more specific pattern like the full merchant name'
            }

        # Check if rule already exists (use cache to reduce API calls)
        if not skip_duplicate_check:
            existing_rules = self.get_merchant_rules(use_cache=True)
            for rule in existing_rules:
                if rule['merchant_pattern'] == merchant_pattern:
                    return {
                        'success': False,
                        'error': f"Rule for '{merchant_pattern}' already exists",
                        'existing_rule': rule
                    }

        # Append new rule
        new_row = [[merchant_pattern, category_id, confidence, notes]]

        try:
            service.spreadsheets().values().append(
                spreadsheetId=BUDGET_CONFIG_SHEET_ID,
                range=f"'{CONFIG_MERCHANT_RULES_TAB}'!A:D",
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body={'values': new_row}
            ).execute()

            logger.info(f"Added merchant rule: {merchant_pattern} -> {category_id}")

            # Update cache if it exists
            if self._merchant_rules_cache is not None:
                self._merchant_rules_cache.append({
                    'merchant_pattern': merchant_pattern,
                    'category_id': category_id,
                    'confidence': confidence,
                    'notes': notes,
                })

            return {
                'success': True,
                'merchant_pattern': merchant_pattern,
                'category_id': category_id,
                'confidence': confidence,
                'notes': notes
            }

        except Exception as e:
            logger.error(f"Failed to add merchant rule: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def add_keyword(
        self,
        keyword: str,
        category_id: str,
        priority: int = 10
    ) -> Dict[str, Any]:
        """
        Add a new keyword rule to the Budget Config sheet.
        
        Args:
            keyword: Word to match (will be lowercased)
            category_id: Category to suggest
            priority: Higher = stronger signal (1-100)
            
        Returns:
            Dict with success status and details
        """
        service = self._get_service()
        
        # Normalize the keyword
        keyword = keyword.strip().lower()
        
        # Check if keyword already exists
        existing_keywords = self.get_keywords()
        for kw in existing_keywords:
            if kw['keyword'] == keyword:
                return {
                    'success': False,
                    'error': f"Keyword '{keyword}' already exists",
                    'existing_keyword': kw
                }
        
        # Append new keyword
        new_row = [[keyword, category_id, priority]]
        
        try:
            service.spreadsheets().values().append(
                spreadsheetId=BUDGET_CONFIG_SHEET_ID,
                range=f"'{CONFIG_KEYWORDS_TAB}'!A:C",
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body={'values': new_row}
            ).execute()
            
            logger.info(f"Added keyword: {keyword} -> {category_id}")
            
            return {
                'success': True,
                'keyword': keyword,
                'category_id': category_id,
                'priority': priority
            }
            
        except Exception as e:
            logger.error(f"Failed to add keyword: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    # ========================================================================
    # SHARED HELPERS
    # ========================================================================

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string in common formats."""
        if not date_str:
            return None
        for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%m/%d/%y'):
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        return None

    def _parse_amount(self, amount_str: str) -> Optional[float]:
        """Parse amount string, handling $, commas, and (parentheses) as negative."""
        if not amount_str:
            return None
        s = str(amount_str).strip()
        negative = False
        if s.startswith('(') and s.endswith(')'):
            negative = True
            s = s[1:-1]
        s = s.replace('$', '').replace(',', '')
        try:
            val = float(s)
            return -val if negative else val
        except ValueError:
            return None

    def _read_all_rows(self) -> Tuple[List[List[str]], List[str], Dict[str, int]]:
        """
        Read all data from Processed Transactions in a single API call.
        Returns (all_values, headers, col_indices).
        """
        service = self._get_service()
        result = service.spreadsheets().values().get(
            spreadsheetId=PROCESSED_TRANSACTIONS_SHEET_ID,
            range=f"'{PROCESSED_TRANSACTIONS_SHEET_NAME}'!A:ZZ"
        ).execute()
        values = result.get('values', [])
        if not values:
            return [], [], {}
        headers = values[0]
        col_indices = {h: i for i, h in enumerate(headers)}
        return values, headers, col_indices

    def _apply_filters(
        self,
        values: List[List[str]],
        headers: List[str],
        col_indices: Dict[str, int],
        filters: Dict[str, Any]
    ) -> List[Tuple[int, List[str]]]:
        """
        Apply filters to data rows. Returns list of (row_number, padded_row).
        All filters are AND-ed together.
        """
        results = []
        num_headers = len(headers)

        # Pre-resolve column indices for filter fields
        cat_idx = col_indices.get(COL_CLAUDE_CATEGORY)
        desc_idx = col_indices.get(COL_DESCRIPTION)
        date_idx = col_indices.get(COL_DATE)
        source_idx = col_indices.get(COL_CATEGORY_SOURCE)
        review_idx = col_indices.get(COL_NEEDS_REVIEW)
        amount_idx = col_indices.get(COL_AMOUNT)
        account_idx = col_indices.get(COL_ACCOUNT)

        # Pre-parse filter values
        f_category = filters.get('category')
        f_desc = filters.get('description_pattern', '').lower() if filters.get('description_pattern') else None
        f_date_from = self._parse_date(filters.get('date_from', ''))
        f_date_to = self._parse_date(filters.get('date_to', ''))
        f_source = filters.get('source')
        f_needs_review = filters.get('needs_review')
        f_amount_min = filters.get('amount_min')
        f_amount_max = filters.get('amount_max')
        f_uncategorized = filters.get('uncategorized_only', False)
        f_account = filters.get('account')

        for row_num, row in enumerate(values[1:], start=2):
            padded = row + [''] * (num_headers - len(row))

            # category filter
            if f_category is not None:
                val = padded[cat_idx].strip() if cat_idx is not None else ''
                if val != f_category:
                    continue

            # uncategorized_only filter
            if f_uncategorized:
                val = padded[cat_idx].strip() if cat_idx is not None else ''
                if val:
                    continue

            # description_pattern filter
            if f_desc is not None:
                val = padded[desc_idx].lower() if desc_idx is not None else ''
                if f_desc not in val:
                    continue

            # date_from / date_to filters
            if f_date_from or f_date_to:
                raw = padded[date_idx] if date_idx is not None else ''
                row_date = self._parse_date(raw)
                if not row_date:
                    continue
                if f_date_from and row_date < f_date_from:
                    continue
                if f_date_to and row_date > f_date_to:
                    continue

            # source filter
            if f_source is not None:
                val = padded[source_idx].strip() if source_idx is not None else ''
                if val != f_source:
                    continue

            # needs_review filter
            if f_needs_review is not None:
                val = padded[review_idx].strip().upper() if review_idx is not None else ''
                if f_needs_review and val != 'TRUE':
                    continue
                if not f_needs_review and val == 'TRUE':
                    continue

            # amount_min / amount_max filters
            if f_amount_min is not None or f_amount_max is not None:
                raw = padded[amount_idx] if amount_idx is not None else ''
                amt = self._parse_amount(raw)
                if amt is None:
                    continue
                if f_amount_min is not None and amt < f_amount_min:
                    continue
                if f_amount_max is not None and amt > f_amount_max:
                    continue

            # account filter
            if f_account is not None:
                val = padded[account_idx].strip().lower() if account_idx is not None else ''
                if f_account.lower() not in val:
                    continue

            results.append((row_num, padded))

        return results

    # ========================================================================
    # QUERY & UPDATE OPERATIONS
    # ========================================================================

    def query_transactions(
        self,
        filters: Dict[str, Any],
        limit: int = 50,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Search and filter transactions with flexible criteria.
        Returns paginated results with optional category summary.
        """
        values, headers, col_indices = self._read_all_rows()
        if not values:
            return {'total_matching': 0, 'offset': offset, 'limit': limit,
                    'has_more': False, 'transactions': []}

        # Ensure categorization columns exist
        if COL_CLAUDE_CATEGORY not in col_indices:
            self.ensure_categorization_columns()
            values, headers, col_indices = self._read_all_rows()

        matching = self._apply_filters(values, headers, col_indices, filters)
        total_matching = len(matching)

        # Aggregate: category summary + total amount (before pagination)
        category_summary = {}
        total_amount = 0.0
        cat_idx = col_indices.get(COL_CLAUDE_CATEGORY)
        amount_idx = col_indices.get(COL_AMOUNT)
        for _, padded in matching:
            # Amount
            amt = self._parse_amount(padded[amount_idx]) if amount_idx is not None else None
            if amt is not None:
                total_amount += amt
            # Category breakdown (only when not filtering by single category)
            if not filters.get('category'):
                cat = padded[cat_idx].strip() if cat_idx is not None else ''
                cat_key = cat if cat else '(uncategorized)'
                category_summary[cat_key] = category_summary.get(cat_key, 0) + 1

        # Paginate
        page = matching[offset:offset + limit]

        # Build compact transaction dicts (4 fields)
        transactions = []
        for row_num, padded in page:
            transactions.append({
                'row_number': row_num,
                'Date': padded[col_indices.get(COL_DATE, 0)] if COL_DATE in col_indices else '',
                'Description': padded[col_indices.get(COL_DESCRIPTION, 0)] if COL_DESCRIPTION in col_indices else '',
                'Amount': padded[col_indices.get(COL_AMOUNT, 0)] if COL_AMOUNT in col_indices else '',
                'claude_category': padded[col_indices.get(COL_CLAUDE_CATEGORY, 0)] if COL_CLAUDE_CATEGORY in col_indices else '',
            })

        result = {
            'total_matching': total_matching,
            'total_amount': round(total_amount, 2),
            'offset': offset,
            'limit': limit,
            'has_more': (offset + limit) < total_matching,
            'transactions': transactions,
        }
        if category_summary:
            result['category_summary'] = category_summary

        return result

    def bulk_update_category(
        self,
        filters: Dict[str, Any],
        new_category_id: str,
        dry_run: bool = True
    ) -> Dict[str, Any]:
        """
        Change category for all transactions matching filters.
        Saves previous_category for undo.
        """
        values, headers, col_indices = self._read_all_rows()
        if not values:
            return {'success': True, 'updated': 0, 'dry_run': dry_run}

        if COL_CLAUDE_CATEGORY not in col_indices:
            self.ensure_categorization_columns()
            values, headers, col_indices = self._read_all_rows()

        matching = self._apply_filters(values, headers, col_indices, filters)

        if dry_run:
            preview = []
            for row_num, padded in matching[:20]:
                preview.append({
                    'row_number': row_num,
                    'Description': padded[col_indices.get(COL_DESCRIPTION, 0)] if COL_DESCRIPTION in col_indices else '',
                    'Amount': padded[col_indices.get(COL_AMOUNT, 0)] if COL_AMOUNT in col_indices else '',
                    'current_category': padded[col_indices.get(COL_CLAUDE_CATEGORY, 0)] if COL_CLAUDE_CATEGORY in col_indices else '',
                })
            return {
                'success': True,
                'updated': len(matching),
                'new_category_id': new_category_id,
                'dry_run': True,
                'preview': preview,
            }

        # Build batch updates
        service = self._get_service()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        batch_data = []

        cat_col = column_index_to_letter(col_indices[COL_CLAUDE_CATEGORY])
        source_col = column_index_to_letter(col_indices[COL_CATEGORY_SOURCE])
        at_col = column_index_to_letter(col_indices[COL_CATEGORIZED_AT])
        by_col = column_index_to_letter(col_indices[COL_CATEGORIZED_BY])
        prev_col = column_index_to_letter(col_indices[COL_PREVIOUS_CATEGORY]) if COL_PREVIOUS_CATEGORY in col_indices else None
        review_col = column_index_to_letter(col_indices[COL_NEEDS_REVIEW]) if COL_NEEDS_REVIEW in col_indices else None
        reason_col = column_index_to_letter(col_indices[COL_REVIEW_REASON]) if COL_REVIEW_REASON in col_indices else None
        sheet = f"'{PROCESSED_TRANSACTIONS_SHEET_NAME}'"

        for row_num, padded in matching:
            current_cat = padded[col_indices[COL_CLAUDE_CATEGORY]].strip()

            # Save previous category
            if prev_col:
                batch_data.append({'range': f"{sheet}!{prev_col}{row_num}", 'values': [[current_cat]]})

            batch_data.append({'range': f"{sheet}!{cat_col}{row_num}", 'values': [[new_category_id]]})
            batch_data.append({'range': f"{sheet}!{source_col}{row_num}", 'values': [['claude']]})
            batch_data.append({'range': f"{sheet}!{at_col}{row_num}", 'values': [[timestamp]]})
            batch_data.append({'range': f"{sheet}!{by_col}{row_num}", 'values': [['mcp_bulk_update']]})
            if review_col:
                batch_data.append({'range': f"{sheet}!{review_col}{row_num}", 'values': [['']]})
            if reason_col:
                batch_data.append({'range': f"{sheet}!{reason_col}{row_num}", 'values': [['']]})

        if batch_data:
            service.spreadsheets().values().batchUpdate(
                spreadsheetId=PROCESSED_TRANSACTIONS_SHEET_ID,
                body={'valueInputOption': 'RAW', 'data': batch_data}
            ).execute()

        return {
            'success': True,
            'updated': len(matching),
            'new_category_id': new_category_id,
            'dry_run': False,
        }

    def reset_categories(
        self,
        filters: Dict[str, Any],
        dry_run: bool = True
    ) -> Dict[str, Any]:
        """
        Clear categories for matching transactions so they can be re-categorized.
        Saves previous_category for reference.
        """
        values, headers, col_indices = self._read_all_rows()
        if not values:
            return {'success': True, 'reset': 0, 'dry_run': dry_run}

        if COL_CLAUDE_CATEGORY not in col_indices:
            self.ensure_categorization_columns()
            values, headers, col_indices = self._read_all_rows()

        matching = self._apply_filters(values, headers, col_indices, filters)

        if dry_run:
            preview = []
            for row_num, padded in matching[:20]:
                preview.append({
                    'row_number': row_num,
                    'Description': padded[col_indices.get(COL_DESCRIPTION, 0)] if COL_DESCRIPTION in col_indices else '',
                    'current_category': padded[col_indices.get(COL_CLAUDE_CATEGORY, 0)] if COL_CLAUDE_CATEGORY in col_indices else '',
                    'source': padded[col_indices.get(COL_CATEGORY_SOURCE, 0)] if COL_CATEGORY_SOURCE in col_indices else '',
                })
            return {
                'success': True,
                'reset': len(matching),
                'dry_run': True,
                'preview': preview,
                'suggestion': 'Run batch_apply_all_rules after reset to re-categorize',
            }

        service = self._get_service()
        batch_data = []
        sheet = f"'{PROCESSED_TRANSACTIONS_SHEET_NAME}'"

        # Columns to clear
        clear_cols = {}
        for col_name in [COL_CLAUDE_CATEGORY, COL_CATEGORY_SOURCE, COL_CATEGORY_CONFIDENCE,
                         COL_CATEGORIZED_AT, COL_CATEGORIZED_BY, COL_NEEDS_REVIEW, COL_REVIEW_REASON]:
            if col_name in col_indices:
                clear_cols[col_name] = column_index_to_letter(col_indices[col_name])

        prev_col = column_index_to_letter(col_indices[COL_PREVIOUS_CATEGORY]) if COL_PREVIOUS_CATEGORY in col_indices else None

        for row_num, padded in matching:
            current_cat = padded[col_indices[COL_CLAUDE_CATEGORY]].strip() if COL_CLAUDE_CATEGORY in col_indices else ''

            # Save previous category
            if prev_col and current_cat:
                batch_data.append({'range': f"{sheet}!{prev_col}{row_num}", 'values': [[current_cat]]})

            # Clear all categorization columns
            for col_name, col_letter in clear_cols.items():
                batch_data.append({'range': f"{sheet}!{col_letter}{row_num}", 'values': [['']]})

        if batch_data:
            service.spreadsheets().values().batchUpdate(
                spreadsheetId=PROCESSED_TRANSACTIONS_SHEET_ID,
                body={'valueInputOption': 'RAW', 'data': batch_data}
            ).execute()

        return {
            'success': True,
            'reset': len(matching),
            'dry_run': False,
            'suggestion': 'Run batch_apply_all_rules to re-categorize these transactions',
        }

    def get_spending_summary(
        self,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        category_taxonomy: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Get pre-aggregated spending data grouped by category.
        Returns hierarchical breakdown with budget comparison.
        """
        values, headers, col_indices = self._read_all_rows()
        if not values:
            return {'period': {}, 'total_income': 0, 'total_expenses': 0,
                    'net': 0, 'by_parent_category': {}, 'uncategorized': {'total': 0, 'count': 0}}

        if COL_CLAUDE_CATEGORY not in col_indices:
            self.ensure_categorization_columns()
            values, headers, col_indices = self._read_all_rows()

        filters = {}
        if date_from:
            filters['date_from'] = date_from
        if date_to:
            filters['date_to'] = date_to

        matching = self._apply_filters(values, headers, col_indices, filters)

        # Build category -> parent / budget lookup from taxonomy
        cat_parent = {}
        cat_budget = {}
        parent_budget = {}
        if category_taxonomy:
            for cat in category_taxonomy:
                cid = cat.get('category_id', '')
                cat_parent[cid] = cat.get('parent_category', 'Other')
                if cat.get('monthly_budget') is not None:
                    cat_budget[cid] = cat['monthly_budget']

        # Aggregate
        cat_idx = col_indices.get(COL_CLAUDE_CATEGORY)
        amount_idx = col_indices.get(COL_AMOUNT)

        total_income = 0.0
        total_expenses = 0.0
        by_category = {}  # category_id -> {total, count}
        uncategorized_total = 0.0
        uncategorized_count = 0

        for _, padded in matching:
            cat = padded[cat_idx].strip() if cat_idx is not None else ''
            amt = self._parse_amount(padded[amount_idx] if amount_idx is not None else '')
            if amt is None:
                continue

            if amt > 0:
                total_income += amt
            else:
                total_expenses += amt

            if not cat:
                uncategorized_total += amt
                uncategorized_count += 1
            else:
                if cat not in by_category:
                    by_category[cat] = {'total': 0.0, 'count': 0}
                by_category[cat]['total'] += amt
                by_category[cat]['count'] += 1

        # Group by parent category
        by_parent = {}
        for cat_id, data in by_category.items():
            parent = cat_parent.get(cat_id, 'Other')
            if parent not in by_parent:
                by_parent[parent] = {'total': 0.0, 'budget': None, 'categories': {}}
            by_parent[parent]['total'] += data['total']
            by_parent[parent]['categories'][cat_id] = {
                'total': round(data['total'], 2),
                'budget': cat_budget.get(cat_id),
                'count': data['count'],
            }

        # Sum parent budgets from child budgets
        for parent_name, parent_data in by_parent.items():
            parent_total_budget = 0.0
            has_budget = False
            for cat_id, cat_data in parent_data['categories'].items():
                if cat_data.get('budget') is not None:
                    parent_total_budget += cat_data['budget']
                    has_budget = True
            if has_budget:
                parent_data['budget'] = round(parent_total_budget, 2)
            parent_data['total'] = round(parent_data['total'], 2)

        return {
            'period': {'date_from': date_from, 'date_to': date_to},
            'total_income': round(total_income, 2),
            'total_expenses': round(total_expenses, 2),
            'net': round(total_income + total_expenses, 2),
            'by_parent_category': by_parent,
            'uncategorized': {
                'total': round(uncategorized_total, 2),
                'count': uncategorized_count,
            },
        }

    # ========================================================================
    # CATEGORY MIGRATION
    # ========================================================================

    def migrate_category(
        self,
        old_category_id: str,
        new_category_id: str
    ) -> Dict[str, Any]:
        """
        Migrate all transactions from one category to another.
        
        Args:
            old_category_id: Category ID to migrate from
            new_category_id: Category ID to migrate to
            
        Returns:
            Dict with success status, count of migrated transactions
        """
        service = self._get_service()
        headers, col_indices = self._get_headers()
        
        if COL_CLAUDE_CATEGORY not in col_indices:
            return {
                'success': False,
                'error': f"Column '{COL_CLAUDE_CATEGORY}' not found in sheet",
                'migrated': 0
            }
        
        category_col_idx = col_indices[COL_CLAUDE_CATEGORY]
        category_col_letter = column_index_to_letter(category_col_idx)
        
        # Get all data
        try:
            result = service.spreadsheets().values().get(
                spreadsheetId=PROCESSED_TRANSACTIONS_SHEET_ID,
                range=f"'{PROCESSED_TRANSACTIONS_SHEET_NAME}'!A:ZZ"
            ).execute()
            
            rows = result.get('values', [])
            if len(rows) < 2:
                return {
                    'success': True,
                    'migrated': 0,
                    'message': 'No data rows found'
                }
            
            # Find rows to update
            updates = []
            for row_idx, row in enumerate(rows[1:], start=2):  # Skip header, 1-indexed
                # Pad row if needed
                while len(row) <= category_col_idx:
                    row.append('')
                
                if row[category_col_idx] == old_category_id:
                    updates.append({
                        'range': f"'{PROCESSED_TRANSACTIONS_SHEET_NAME}'!{category_col_letter}{row_idx}",
                        'values': [[new_category_id]]
                    })
            
            if not updates:
                return {
                    'success': True,
                    'migrated': 0,
                    'message': f"No transactions found with category '{old_category_id}'"
                }
            
            # Batch update
            service.spreadsheets().values().batchUpdate(
                spreadsheetId=PROCESSED_TRANSACTIONS_SHEET_ID,
                body={
                    'valueInputOption': 'RAW',
                    'data': updates
                }
            ).execute()
            
            logger.info(f"Migrated {len(updates)} transactions: {old_category_id} -> {new_category_id}")
            
            return {
                'success': True,
                'migrated': len(updates),
                'old_category_id': old_category_id,
                'new_category_id': new_category_id
            }
            
        except Exception as e:
            logger.error(f"Failed to migrate categories: {e}")
            return {
                'success': False,
                'error': str(e),
                'migrated': 0
            }


# Singleton instance
_client = None

def get_sheets_client() -> SheetsClient:
    """Get the singleton SheetsClient instance."""
    global _client
    if _client is None:
        _client = SheetsClient()
    return _client
