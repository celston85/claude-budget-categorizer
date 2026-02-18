#!/usr/bin/env python3
"""
Setup script to create the Budget Config Google Sheet.

Creates a new sheet with:
- Categories tab: Budget categories with hierarchy
- Merchant Rules tab: Deterministic merchant → category mappings
- Keywords tab: Keyword-based category suggestions

Run once to create the config sheet, then customize as needed.
"""

import sys
import os
import logging

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import get_credentials_oauth, get_sheets_service

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# DEFAULT CATEGORIES
# ============================================================================

DEFAULT_CATEGORIES = [
    # Food & Dining
    ('groceries', 'Groceries', 'Food & Dining', 'Supermarkets, grocery stores'),
    ('restaurants', 'Restaurants', 'Food & Dining', 'Dining out, takeout, delivery'),
    ('coffee', 'Coffee & Cafes', 'Food & Dining', 'Coffee shops, cafes'),
    ('alcohol', 'Alcohol & Bars', 'Food & Dining', 'Bars, liquor stores, breweries'),
    ('fast_food', 'Fast Food', 'Food & Dining', 'Quick service restaurants'),
    
    # Shopping
    ('electronics', 'Electronics', 'Shopping', 'Gadgets, tech, accessories'),
    ('clothing', 'Clothing & Apparel', 'Shopping', 'Clothes, shoes, accessories'),
    ('household', 'Household Supplies', 'Shopping', 'Cleaning, paper goods, home basics'),
    ('home_improvement', 'Home Improvement', 'Shopping', 'Hardware, tools, repairs'),
    ('furniture', 'Furniture & Decor', 'Shopping', 'Furniture, decorations'),
    ('books_media', 'Books & Media', 'Shopping', 'Books, magazines, digital media'),
    ('office', 'Office Supplies', 'Shopping', 'Office and work supplies'),
    ('general_shopping', 'General Shopping', 'Shopping', 'Other retail purchases'),
    
    # Health & Personal Care
    ('health', 'Health & Medical', 'Health & Personal', 'Medical, dental, vision, pharmacy'),
    ('personal_care', 'Personal Care', 'Health & Personal', 'Toiletries, cosmetics, grooming'),
    ('fitness', 'Fitness & Sports', 'Health & Personal', 'Gym, sports equipment, classes'),
    
    # Transportation
    ('gas', 'Gas & Fuel', 'Transportation', 'Gas stations, EV charging'),
    ('auto_maintenance', 'Auto Maintenance', 'Transportation', 'Car repairs, oil changes, car wash'),
    ('parking', 'Parking', 'Transportation', 'Parking fees, meters'),
    ('public_transit', 'Public Transit', 'Transportation', 'Bus, train, subway'),
    ('rideshare', 'Rideshare & Taxi', 'Transportation', 'Uber, Lyft, taxi'),
    ('auto_insurance', 'Auto Insurance', 'Transportation', 'Car insurance premiums'),
    
    # Home & Utilities
    ('rent_mortgage', 'Rent/Mortgage', 'Home & Utilities', 'Housing payment'),
    ('utilities', 'Utilities', 'Home & Utilities', 'Electric, gas, water, trash'),
    ('internet_phone', 'Internet & Phone', 'Home & Utilities', 'Internet, cell phone, landline'),
    ('home_insurance', 'Home Insurance', 'Home & Utilities', 'Renters or homeowners insurance'),
    
    # Entertainment & Recreation
    ('entertainment', 'Entertainment', 'Entertainment', 'Movies, concerts, events'),
    ('streaming', 'Streaming Services', 'Entertainment', 'Netflix, Spotify, etc.'),
    ('games', 'Games & Gaming', 'Entertainment', 'Video games, board games'),
    ('hobbies', 'Hobbies', 'Entertainment', 'Hobby supplies and activities'),
    
    # Travel
    ('airfare', 'Airfare', 'Travel', 'Flights'),
    ('lodging', 'Lodging', 'Travel', 'Hotels, Airbnb'),
    ('travel_other', 'Travel - Other', 'Travel', 'Other travel expenses'),
    
    # Family & Kids
    ('kids', 'Kids & Family', 'Family', 'Childcare, kids activities, school'),
    ('pets', 'Pets', 'Family', 'Pet food, vet, supplies'),
    ('gifts', 'Gifts', 'Family', 'Gifts for others'),
    
    # Financial
    ('investments', 'Investments', 'Financial', 'Investment contributions'),
    ('fees', 'Bank Fees', 'Financial', 'ATM fees, service charges'),
    ('taxes', 'Taxes', 'Financial', 'Tax payments'),
    
    # Income (positive amounts)
    ('salary', 'Salary', 'Income', 'Regular paycheck'),
    ('refund', 'Refund', 'Income', 'Refunds and returns'),
    ('income_other', 'Other Income', 'Income', 'Miscellaneous income'),
    
    # Other
    ('subscriptions', 'Subscriptions', 'Other', 'Recurring subscriptions'),
    ('charity', 'Charity & Donations', 'Other', 'Charitable donations'),
    ('uncategorized', 'Uncategorized', 'Other', 'Needs manual categorization'),
]

# ============================================================================
# DEFAULT MERCHANT RULES
# ============================================================================

DEFAULT_MERCHANT_RULES = [
    # Groceries (100% confidence)
    ('whole foods', 'groceries', 100, 'Always groceries'),
    ('trader joe', 'groceries', 100, 'Always groceries'),
    ('safeway', 'groceries', 100, 'Always groceries'),
    ('kroger', 'groceries', 100, 'Always groceries'),
    ('publix', 'groceries', 100, 'Always groceries'),
    ('aldi', 'groceries', 100, 'Always groceries'),
    ('sprouts', 'groceries', 100, 'Always groceries'),
    ('heb', 'groceries', 100, 'Always groceries'),
    ('wegmans', 'groceries', 100, 'Always groceries'),
    ('food lion', 'groceries', 100, 'Always groceries'),
    
    # Gas stations (100% confidence)
    ('shell', 'gas', 100, 'Gas station'),
    ('chevron', 'gas', 100, 'Gas station'),
    ('exxon', 'gas', 100, 'Gas station'),
    ('mobil', 'gas', 100, 'Gas station'),
    ('bp ', 'gas', 100, 'Gas station'),
    ('arco', 'gas', 100, 'Gas station'),
    ('76 ', 'gas', 100, 'Gas station'),
    ('costco gas', 'gas', 100, 'Costco gas station'),
    ('sams club fuel', 'gas', 100, 'Sams Club gas'),
    
    # Coffee (100% confidence)
    ('starbucks', 'coffee', 100, 'Coffee shop'),
    ('dunkin', 'coffee', 100, 'Coffee shop'),
    ('peets', 'coffee', 100, 'Coffee shop'),
    ('blue bottle', 'coffee', 100, 'Coffee shop'),
    
    # Fast food (100% confidence)
    ('mcdonald', 'fast_food', 100, 'Fast food'),
    ('burger king', 'fast_food', 100, 'Fast food'),
    ('wendy', 'fast_food', 100, 'Fast food'),
    ('taco bell', 'fast_food', 100, 'Fast food'),
    ('chick-fil-a', 'fast_food', 100, 'Fast food'),
    ('chipotle', 'fast_food', 100, 'Fast food'),
    ('subway', 'fast_food', 100, 'Fast food'),
    
    # Streaming (100% confidence)
    ('netflix', 'streaming', 100, 'Streaming service'),
    ('spotify', 'streaming', 100, 'Streaming service'),
    ('hulu', 'streaming', 100, 'Streaming service'),
    ('disney+', 'streaming', 100, 'Streaming service'),
    ('apple music', 'streaming', 100, 'Streaming service'),
    ('hbo max', 'streaming', 100, 'Streaming service'),
    ('prime video', 'streaming', 100, 'Streaming service'),
    ('youtube premium', 'streaming', 100, 'Streaming service'),
    
    # Rideshare (100% confidence)
    ('uber ', 'rideshare', 100, 'Rideshare'),
    ('lyft', 'rideshare', 100, 'Rideshare'),
    
    # Pharmacy (health)
    ('cvs', 'health', 80, 'Usually pharmacy/health'),
    ('walgreens', 'health', 80, 'Usually pharmacy/health'),
    ('rite aid', 'health', 80, 'Usually pharmacy/health'),
    
    # Home improvement (100% confidence)
    ('home depot', 'home_improvement', 100, 'Home improvement store'),
    ('lowes', 'home_improvement', 100, 'Home improvement store'),
    ('ace hardware', 'home_improvement', 100, 'Hardware store'),
    
    # Electronics
    ('best buy', 'electronics', 100, 'Electronics store'),
    ('apple store', 'electronics', 100, 'Apple store'),
    ('micro center', 'electronics', 100, 'Electronics store'),
    
    # Utilities
    ('comcast', 'internet_phone', 100, 'Internet/Cable'),
    ('xfinity', 'internet_phone', 100, 'Internet/Cable'),
    ('at&t', 'internet_phone', 100, 'Phone/Internet'),
    ('verizon', 'internet_phone', 100, 'Phone/Internet'),
    ('t-mobile', 'internet_phone', 100, 'Phone'),
    
    # Amazon patterns (low confidence - needs item detail)
    ('amazon basics', 'household', 60, 'Usually household items'),
    
    # Refunds
    ('refund', 'refund', 90, 'Usually a refund'),
    ('return', 'refund', 80, 'Usually a return'),
]

# ============================================================================
# DEFAULT KEYWORDS
# ============================================================================

DEFAULT_KEYWORDS = [
    # Electronics
    ('battery', 'electronics', 20),
    ('charger', 'electronics', 20),
    ('cable', 'electronics', 15),
    ('adapter', 'electronics', 15),
    ('usb', 'electronics', 15),
    ('hdmi', 'electronics', 20),
    ('phone case', 'electronics', 15),
    ('screen protector', 'electronics', 15),
    
    # Household
    ('paper towel', 'household', 20),
    ('toilet paper', 'household', 20),
    ('detergent', 'household', 20),
    ('soap', 'household', 15),
    ('cleaning', 'household', 15),
    ('trash bag', 'household', 20),
    ('light bulb', 'household', 15),
    
    # Personal care
    ('shampoo', 'personal_care', 20),
    ('toothpaste', 'personal_care', 20),
    ('deodorant', 'personal_care', 20),
    ('razor', 'personal_care', 15),
    ('lotion', 'personal_care', 15),
    
    # Office
    ('pen', 'office', 10),
    ('notebook', 'office', 15),
    ('printer', 'office', 15),
    ('paper', 'office', 10),
    ('stapler', 'office', 15),
    
    # Pets
    ('dog food', 'pets', 25),
    ('cat food', 'pets', 25),
    ('pet', 'pets', 10),
    ('treats', 'pets', 10),
    
    # Kids
    ('toy', 'kids', 15),
    ('diaper', 'kids', 25),
    ('baby', 'kids', 15),
]


def create_config_sheet():
    """Create the Budget Config Google Sheet."""
    logger.info("Creating Budget Config Sheet...")
    
    creds = get_credentials_oauth()
    service = get_sheets_service(creds)
    
    # Create new spreadsheet
    spreadsheet = {
        'properties': {
            'title': 'Budget Config'
        },
        'sheets': [
            {'properties': {'title': 'Categories'}},
            {'properties': {'title': 'Merchant Rules'}},
            {'properties': {'title': 'Keywords'}},
        ]
    }
    
    result = service.spreadsheets().create(body=spreadsheet).execute()
    sheet_id = result['spreadsheetId']
    sheet_url = result['spreadsheetUrl']
    
    logger.info(f"Created sheet: {sheet_url}")
    
    # Populate Categories tab
    logger.info("Populating Categories...")
    categories_data = [
        ['category_id', 'category_name', 'parent_category', 'description']
    ] + [list(c) for c in DEFAULT_CATEGORIES]
    
    service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range="'Categories'!A1",
        valueInputOption='RAW',
        body={'values': categories_data}
    ).execute()
    
    # Populate Merchant Rules tab
    logger.info("Populating Merchant Rules...")
    rules_data = [
        ['merchant_pattern', 'category_id', 'confidence', 'notes']
    ] + [list(r) for r in DEFAULT_MERCHANT_RULES]
    
    service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range="'Merchant Rules'!A1",
        valueInputOption='RAW',
        body={'values': rules_data}
    ).execute()
    
    # Populate Keywords tab
    logger.info("Populating Keywords...")
    keywords_data = [
        ['keyword', 'category_id', 'priority']
    ] + [list(k) for k in DEFAULT_KEYWORDS]
    
    service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range="'Keywords'!A1",
        valueInputOption='RAW',
        body={'values': keywords_data}
    ).execute()
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("Budget Config Sheet Created Successfully!")
    logger.info("=" * 60)
    logger.info("")
    logger.info(f"Sheet ID: {sheet_id}")
    logger.info(f"Sheet URL: {sheet_url}")
    logger.info("")
    logger.info("Next steps:")
    logger.info("1. Open the sheet and customize categories/rules as needed")
    logger.info("2. Set the environment variable:")
    logger.info(f"   export BUDGET_CONFIG_SHEET_ID='{sheet_id}'")
    logger.info("3. Or update config.py with the sheet ID")
    logger.info("")
    
    return sheet_id


if __name__ == '__main__':
    create_config_sheet()
