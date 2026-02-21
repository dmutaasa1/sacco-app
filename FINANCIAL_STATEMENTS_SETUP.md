# Financial Statements Package - Setup Instructions

## Files Delivered

1. **financial_statements_routes.js** — Backend routes for app.js
2. **financial_statements.ejs** — Frontend page for views/
3. **create_financial_statements.py** — Python script (already saved to /home/claude/)

## Installation Steps

### 1. Install Python Dependencies

```bash
pip install openpyxl --break-system-packages
```

### 2. Add Routes to app.js

Copy the contents of `financial_statements_routes.js` into your `app.js` file, placing it in the **REPORTS** section or before the cron jobs section.

### 3. Add Frontend Page

Copy `financial_statements.ejs` to your `views/` directory.

### 4. Add Download Route (if not already present)

Add this route to app.js if you don't have a generic download handler:

```javascript
// Download file from outputs
app.get('/download/:filename', checkAuth, (req, res) => {
  const filepath = path.join('/mnt/user-data/outputs', req.params.filename);
  res.download(filepath);
});
```

### 5. Add to Sidebar Navigation

In `views/partials/sidebar.ejs`, add this menu item:

```html
<li>
    <a href="/financial_statements">
        <i class="sidebar-item-icon fa fa-file-text"></i>
        <span class="nav-label">Financial Statements</span>
    </a>
</li>
```

## What It Generates

The system creates a **single Excel workbook** with **3 sheets**:

### Sheet 1: Income Statement
- **Revenue sections:**
  - Interest on Loans
  - Processing Fees
  - Penalties
  - Membership Fees
  - Other Income
- **Expense sections:**
  - Operating Expenses
  - Administrative Expenses
  - Loan Loss Provisions
- **Bottom line:** Net Income (auto-calculated)

### Sheet 2: Balance Sheet
- **Assets:**
  - Cash and Cash Equivalents (from liquidity_funds table)
  - Loans Receivable (outstanding loan balances)
  - Accrued Interest Receivable
- **Liabilities:**
  - Member Savings Deposits (net of withdrawals)
  - Accrued Expenses
- **Equity:**
  - Share Capital
  - Retained Earnings
- **Verification:** Total Assets = Liabilities + Equity

### Sheet 3: Cash Flow Statement
- **Operating Activities:**
  - Net Income
  - Loan Repayments Received
  - Interest and Fees Collected
- **Investing Activities:**
  - Loans Disbursed (outflow)
- **Financing Activities:**
  - Member Savings Deposits (inflow)
  - Member Savings Withdrawals (outflow)
- **Net Change in Cash** (auto-calculated)
- **Closing Cash Balance** (opening + net change)

## Data Sources

The routes pull data from:
- `transactions` table — for all income, expenses, and cash flows
- `loans` table — for outstanding balances and disbursements
- `liquidity_funds` table — for cash balances

## Formula Examples

All totals are calculated with Excel formulas:
```
Total Income       = SUM(B7:B11)
Total Assets       = SUM(B7:B9)
Net Income         = Total Income - Total Expenses
Net Cash Change    = Operating + Investing + Financing
Closing Cash       = Opening Cash + Net Change
```

## Customization

### Add More Income Categories

In the route, add more queries like:
```javascript
const [newIncome] = await db.execute(`
  SELECT COALESCE(SUM(Amount), 0) AS total
  FROM transactions
  WHERE transaction_type = 'New Category'
    AND Debit_Credit = 'Debit'
    AND tran_date BETWEEN ? AND ?
`, [start_date, end_date]);

income_items.push({ 
  category: 'New Income Source', 
  amount: parseFloat(newIncome[0].total) 
});
```

### Add Expense Tracking

If you track expenses in a separate table, query them:
```javascript
const [expenses] = await db.execute(`
  SELECT category, SUM(amount) AS total
  FROM expenses
  WHERE expense_date BETWEEN ? AND ?
  GROUP BY category
`, [start_date, end_date]);

expense_items = expenses.map(e => ({
  category: e.category,
  amount: parseFloat(e.total)
}));
```

## Features

✅ Professional Excel formatting with color-coded sections
✅ All totals calculated with Excel formulas (not hardcoded)
✅ Formula recalculation using LibreOffice
✅ Date range selection (YTD, Quarter, Month, Custom)
✅ Three complete financial statements in one workbook
✅ Ready for download and external reporting

## Testing

1. Go to `/financial_statements`
2. Select date range (defaults to YTD)
3. Click "Generate Excel Package"
4. Download opens automatically
5. Open in Excel/LibreOffice — all formulas should calculate correctly

## Troubleshooting

**"Failed to generate statements"**
- Check Python dependencies: `pip list | grep openpyxl`
- Verify script location: `ls /home/claude/create_financial_statements.py`

**Empty or zero values**
- Verify transactions exist in the date range
- Check transaction_type values match the queries
- Confirm Debit_Credit logic is correct for your setup

**Formula errors in Excel**
- Check recalc.py ran successfully (look for JSON output in logs)
- Verify no #REF! or #DIV/0! errors
- Ensure cell references match data placement

## Next Steps

Consider adding:
- Comparative statements (current vs prior period)
- Budget vs actual analysis
- Ratio analysis (ROA, ROE, Debt-to-Equity)
- Monthly/quarterly trend charts
- PDF export option
