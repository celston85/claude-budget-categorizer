#!/usr/bin/env python3
"""
Dashboard Generator for Budget Categorizer.

Generates a standalone HTML dashboard file with embedded CSS, JS, and data.
No external dependencies on load — all data is baked into the HTML.
"""

import json
import os
from typing import Dict, Any, List


def generate_dashboard_html(
    summary_data: Dict[str, Any],
    transactions: List[Dict[str, Any]],
    output_path: str = None
) -> str:
    """
    Generate interactive HTML budget dashboard.

    Args:
        summary_data: Output from SheetsClient.get_spending_summary()
        transactions: List of transaction dicts with row-level data
        output_path: Where to write the file (default: ~/claude_budget/dashboard.html)

    Returns:
        Path to the generated HTML file
    """
    if output_path is None:
        output_path = os.path.expanduser('~/claude_budget/dashboard.html')

    # Bake data into JSON for the template
    summary_json = json.dumps(summary_data, indent=2)
    transactions_json = json.dumps(transactions)

    period = summary_data.get('period', {})
    date_from = period.get('date_from', '')
    date_to = period.get('date_to', '')
    period_label = f"{date_from} to {date_to}" if date_from and date_to else "All Time"

    html = _TEMPLATE.replace('{{SUMMARY_DATA}}', summary_json)
    html = html.replace('{{TRANSACTIONS_DATA}}', transactions_json)
    html = html.replace('{{PERIOD_LABEL}}', period_label)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(html)

    return output_path


_TEMPLATE = r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Budget Dashboard</title>
<style>
  :root {
    --bg: #f5f5f7;
    --card-bg: #ffffff;
    --text: #1d1d1f;
    --text-secondary: #6e6e73;
    --border: #e5e5ea;
    --accent: #0071e3;
    --green: #34c759;
    --red: #ff3b30;
    --orange: #ff9500;
    --shadow: 0 1px 3px rgba(0,0,0,0.08), 0 1px 2px rgba(0,0,0,0.04);
    --shadow-lg: 0 4px 12px rgba(0,0,0,0.08), 0 2px 4px rgba(0,0,0,0.04);
    --radius: 12px;
    --radius-sm: 8px;
    --font: -apple-system, BlinkMacSystemFont, 'SF Pro Text', 'Inter', 'Segoe UI', sans-serif;
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: var(--font);
    background: var(--bg);
    color: var(--text);
    line-height: 1.5;
    -webkit-font-smoothing: antialiased;
  }
  .container { max-width: 960px; margin: 0 auto; padding: 32px 24px 64px; }
  header {
    display: flex; justify-content: space-between; align-items: baseline;
    margin-bottom: 28px;
  }
  header h1 { font-size: 28px; font-weight: 700; letter-spacing: -0.5px; }
  header .period { font-size: 15px; color: var(--text-secondary); }

  /* Summary cards */
  .cards {
    display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px;
    margin-bottom: 32px;
  }
  .card {
    background: var(--card-bg); border-radius: var(--radius); padding: 20px;
    box-shadow: var(--shadow);
  }
  .card .label { font-size: 12px; font-weight: 600; color: var(--text-secondary); text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px; }
  .card .value { font-size: 24px; font-weight: 700; letter-spacing: -0.5px; }
  .card .value.income { color: var(--green); }
  .card .value.expense { color: var(--text); }
  .card .value.net-positive { color: var(--green); }
  .card .value.net-negative { color: var(--red); }
  .card .sub { font-size: 12px; color: var(--text-secondary); margin-top: 4px; }

  /* Parent category groups */
  .parent-group {
    background: var(--card-bg); border-radius: var(--radius);
    box-shadow: var(--shadow); margin-bottom: 12px; overflow: hidden;
  }
  .parent-header {
    display: flex; justify-content: space-between; align-items: center;
    padding: 16px 20px; cursor: pointer; user-select: none;
    border-bottom: 1px solid transparent;
    transition: background 0.15s;
  }
  .parent-header:hover { background: #fafafa; }
  .parent-group.open .parent-header { border-bottom-color: var(--border); }
  .parent-header .left { display: flex; align-items: center; gap: 10px; }
  .parent-header .chevron {
    width: 20px; height: 20px; transition: transform 0.2s;
    color: var(--text-secondary);
  }
  .parent-group.open .parent-header .chevron { transform: rotate(90deg); }
  .parent-header .name { font-size: 16px; font-weight: 600; }
  .parent-header .right { display: flex; align-items: center; gap: 20px; }
  .parent-header .total { font-size: 16px; font-weight: 600; }
  .parent-header .budget-bar-container {
    width: 100px; height: 6px; background: var(--border); border-radius: 3px; overflow: hidden;
  }
  .parent-header .budget-bar {
    height: 100%; border-radius: 3px; transition: width 0.3s;
  }

  /* Category rows */
  .categories { display: none; }
  .parent-group.open .categories { display: block; }
  .cat-row {
    display: flex; justify-content: space-between; align-items: center;
    padding: 12px 20px 12px 50px; cursor: pointer;
    border-bottom: 1px solid var(--border);
    transition: background 0.1s;
  }
  .cat-row:last-child { border-bottom: none; }
  .cat-row:hover { background: #fafafa; }
  .cat-row .cat-left { display: flex; align-items: center; gap: 8px; }
  .cat-row .cat-chevron {
    width: 16px; height: 16px; transition: transform 0.2s;
    color: var(--text-secondary);
  }
  .cat-row.expanded .cat-chevron { transform: rotate(90deg); }
  .cat-row .cat-name { font-size: 14px; }
  .cat-row .cat-count { font-size: 12px; color: var(--text-secondary); }
  .cat-row .cat-right { display: flex; align-items: center; gap: 16px; font-size: 14px; }
  .cat-row .cat-total { font-weight: 600; min-width: 80px; text-align: right; }
  .cat-row .cat-budget { color: var(--text-secondary); font-size: 12px; min-width: 90px; text-align: right; }
  .cat-row .cat-pct { color: var(--text-secondary); font-size: 12px; min-width: 50px; text-align: right; }

  /* Transaction drill-down */
  .txn-table-wrapper {
    display: none; padding: 0 20px 12px 50px;
    border-bottom: 1px solid var(--border);
  }
  .txn-table-wrapper.visible { display: block; }
  table.txn-table {
    width: 100%; border-collapse: collapse; font-size: 13px;
  }
  .txn-table th {
    text-align: left; padding: 8px 10px; font-weight: 600; font-size: 11px;
    text-transform: uppercase; letter-spacing: 0.5px; color: var(--text-secondary);
    border-bottom: 1px solid var(--border); cursor: pointer; user-select: none;
    white-space: nowrap;
  }
  .txn-table th:hover { color: var(--accent); }
  .txn-table th .sort-arrow { margin-left: 4px; font-size: 10px; }
  .txn-table td { padding: 6px 10px; border-bottom: 1px solid #f0f0f0; }
  .txn-table td.amount { text-align: right; font-variant-numeric: tabular-nums; }
  .txn-table td.date { white-space: nowrap; color: var(--text-secondary); }
  .txn-table td.account { color: var(--text-secondary); font-size: 12px; }

  /* Uncategorized section */
  .uncategorized {
    background: var(--card-bg); border-radius: var(--radius);
    box-shadow: var(--shadow); padding: 16px 20px; margin-bottom: 12px;
    display: flex; justify-content: space-between; align-items: center;
  }
  .uncategorized .label { font-size: 14px; color: var(--text-secondary); }
  .uncategorized .value { font-size: 14px; font-weight: 600; }

  footer {
    text-align: center; padding: 24px; font-size: 12px; color: var(--text-secondary);
  }

  @media (max-width: 700px) {
    .cards { grid-template-columns: repeat(2, 1fr); }
    .cat-row { padding-left: 30px; }
    .txn-table-wrapper { padding-left: 30px; }
  }
</style>
</head>
<body>
<div class="container">
  <header>
    <h1>Budget Dashboard</h1>
    <span class="period">{{PERIOD_LABEL}}</span>
  </header>
  <div id="cards" class="cards"></div>
  <div id="groups"></div>
  <div id="uncategorized"></div>
</div>
<footer>Generated by Claude Budget Categorizer</footer>

<script>
const SUMMARY = {{SUMMARY_DATA}};
const TRANSACTIONS = {{TRANSACTIONS_DATA}};

function fmt(n) {
  if (n == null) return '—';
  const abs = Math.abs(n);
  const s = abs.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
  return (n < 0 ? '-' : '') + '$' + s;
}
function pct(used, budget) {
  if (!budget) return '';
  return Math.round((Math.abs(used) / budget) * 100) + '%';
}
function barColor(used, budget) {
  if (!budget) return 'var(--accent)';
  const ratio = Math.abs(used) / budget;
  if (ratio > 1) return 'var(--red)';
  if (ratio > 0.85) return 'var(--orange)';
  return 'var(--green)';
}

// Render summary cards
(function() {
  const c = document.getElementById('cards');
  const totalExpAbs = Math.abs(SUMMARY.total_expenses);
  // Find total budget across all parents
  let totalBudget = 0;
  for (const p of Object.values(SUMMARY.by_parent_category || {})) {
    if (p.budget) totalBudget += p.budget;
  }
  const utilization = totalBudget > 0 ? Math.round((totalExpAbs / totalBudget) * 100) + '%' : '—';

  c.innerHTML = `
    <div class="card"><div class="label">Income</div><div class="value income">${fmt(SUMMARY.total_income)}</div></div>
    <div class="card"><div class="label">Expenses</div><div class="value expense">${fmt(SUMMARY.total_expenses)}</div></div>
    <div class="card"><div class="label">Net</div><div class="value ${SUMMARY.net >= 0 ? 'net-positive' : 'net-negative'}">${fmt(SUMMARY.net)}</div></div>
    <div class="card"><div class="label">Budget Used</div><div class="value">${utilization}</div>
      ${totalBudget > 0 ? `<div class="sub">${fmt(-totalExpAbs)} of ${fmt(totalBudget)}</div>` : ''}
    </div>
  `;
})();

// Index transactions by category
const txnsByCategory = {};
for (const t of TRANSACTIONS) {
  const cat = t.claude_category || '(uncategorized)';
  if (!txnsByCategory[cat]) txnsByCategory[cat] = [];
  txnsByCategory[cat].push(t);
}

// Render parent groups
(function() {
  const container = document.getElementById('groups');
  const parents = SUMMARY.by_parent_category || {};
  const totalExp = Math.abs(SUMMARY.total_expenses) || 1;

  // Sort parents by absolute total descending
  const sorted = Object.entries(parents).sort((a, b) => Math.abs(b[1].total) - Math.abs(a[1].total));

  for (const [parentName, parentData] of sorted) {
    const group = document.createElement('div');
    group.className = 'parent-group';

    const budgetPct = parentData.budget ? Math.min(100, Math.round((Math.abs(parentData.total) / parentData.budget) * 100)) : 0;
    const bc = barColor(parentData.total, parentData.budget);

    let catHTML = '';
    // Sort categories by absolute total descending
    const catEntries = Object.entries(parentData.categories || {}).sort((a, b) => Math.abs(b[1].total) - Math.abs(a[1].total));

    for (const [catId, catData] of catEntries) {
      const catPctOfTotal = Math.round((Math.abs(catData.total) / totalExp) * 100);
      const budgetStr = catData.budget != null ? `${fmt(-Math.abs(catData.total))} / ${fmt(catData.budget)}` : '';
      const catTxns = txnsByCategory[catId] || [];

      // Build transaction table
      let txnHTML = '';
      if (catTxns.length > 0) {
        txnHTML = `<div class="txn-table-wrapper" id="txn-${catId}">
          <table class="txn-table">
            <thead><tr>
              <th data-col="Date">Date <span class="sort-arrow"></span></th>
              <th data-col="Description">Description <span class="sort-arrow"></span></th>
              <th data-col="Amount">Amount <span class="sort-arrow"></span></th>
              <th data-col="Account">Account <span class="sort-arrow"></span></th>
            </tr></thead>
            <tbody>${catTxns.map(t => `<tr>
              <td class="date">${t.Date || ''}</td>
              <td>${t.Description || ''}</td>
              <td class="amount">${t.Amount || ''}</td>
              <td class="account">${t.Account || ''}</td>
            </tr>`).join('')}</tbody>
          </table>
        </div>`;
      }

      catHTML += `
        <div class="cat-row" data-cat="${catId}">
          <div class="cat-left">
            <svg class="cat-chevron" viewBox="0 0 20 20" fill="currentColor"><path d="M7.21 14.77a.75.75 0 01.02-1.06L11.168 10 7.23 6.29a.75.75 0 111.04-1.08l4.5 4.25a.75.75 0 010 1.08l-4.5 4.25a.75.75 0 01-1.06-.02z"/></svg>
            <span class="cat-name">${catId}</span>
            <span class="cat-count">(${catData.count})</span>
          </div>
          <div class="cat-right">
            <span class="cat-budget">${budgetStr}</span>
            <span class="cat-pct">${catPctOfTotal}%</span>
            <span class="cat-total">${fmt(catData.total)}</span>
          </div>
        </div>
        ${txnHTML}`;
    }

    group.innerHTML = `
      <div class="parent-header">
        <div class="left">
          <svg class="chevron" viewBox="0 0 20 20" fill="currentColor"><path d="M7.21 14.77a.75.75 0 01.02-1.06L11.168 10 7.23 6.29a.75.75 0 111.04-1.08l4.5 4.25a.75.75 0 010 1.08l-4.5 4.25a.75.75 0 01-1.06-.02z"/></svg>
          <span class="name">${parentName}</span>
        </div>
        <div class="right">
          ${parentData.budget ? `<div class="budget-bar-container"><div class="budget-bar" style="width:${budgetPct}%;background:${bc}"></div></div>` : ''}
          <span class="total">${fmt(parentData.total)}</span>
        </div>
      </div>
      <div class="categories">${catHTML}</div>`;

    // Toggle parent group
    group.querySelector('.parent-header').addEventListener('click', () => {
      group.classList.toggle('open');
    });

    // Toggle category drill-down
    group.querySelectorAll('.cat-row').forEach(row => {
      row.addEventListener('click', () => {
        const catId = row.dataset.cat;
        const wrapper = document.getElementById('txn-' + catId);
        if (wrapper) {
          row.classList.toggle('expanded');
          wrapper.classList.toggle('visible');
        }
      });
    });

    // Sortable columns
    group.querySelectorAll('.txn-table th').forEach(th => {
      th.addEventListener('click', (e) => {
        e.stopPropagation();
        const col = th.dataset.col;
        const table = th.closest('table');
        const tbody = table.querySelector('tbody');
        const rows = Array.from(tbody.querySelectorAll('tr'));
        const colIdx = Array.from(th.parentNode.children).indexOf(th);
        const asc = th.dataset.sort !== 'asc';
        th.dataset.sort = asc ? 'asc' : 'desc';

        // Clear other sort indicators
        th.parentNode.querySelectorAll('th').forEach(h => {
          if (h !== th) h.dataset.sort = '';
          h.querySelector('.sort-arrow').textContent = '';
        });
        th.querySelector('.sort-arrow').textContent = asc ? ' ↑' : ' ↓';

        rows.sort((a, b) => {
          let va = a.children[colIdx].textContent.trim();
          let vb = b.children[colIdx].textContent.trim();
          // Try numeric sort for Amount
          if (col === 'Amount') {
            const na = parseFloat(va.replace(/[$,]/g, '')) || 0;
            const nb = parseFloat(vb.replace(/[$,]/g, '')) || 0;
            return asc ? na - nb : nb - na;
          }
          return asc ? va.localeCompare(vb) : vb.localeCompare(va);
        });
        rows.forEach(r => tbody.appendChild(r));
      });
    });

    container.appendChild(group);
  }
})();

// Uncategorized section
(function() {
  const u = SUMMARY.uncategorized || {};
  if (u.count > 0) {
    document.getElementById('uncategorized').innerHTML = `
      <div class="uncategorized">
        <span class="label">Uncategorized (${u.count} transactions)</span>
        <span class="value">${fmt(u.total)}</span>
      </div>`;
  }
})();
</script>
</body>
</html>'''
