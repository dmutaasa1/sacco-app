// ==================== FINANCIAL STATEMENTS ROUTES ====================

// GET: Financial statements page
app.get('/financial_statements', checkAuth, asyncHandler(async (req, res) => {
  res.render('financial_statements', {
    currentPage: 'reports',
    user: req.session.user
  });
}));

// POST: Generate financial statements
app.post('/financial_statements/generate', checkAuth, asyncHandler(async (req, res) => {
  const { start_date, end_date, sacco_name = 'St. Kizito SACCO' } = req.body;
  const db = dbConfig;

  if (!start_date || !end_date) {
    return res.status(400).json({ error: 'Start date and end date are required' });
  }

  // ── INCOME STATEMENT DATA ────────────────────────────────────────

  // Interest income from loans
  const [interestIncome] = await db.execute(`
    SELECT COALESCE(SUM(Amount), 0) AS total
    FROM transactions
    WHERE transaction_type = 'Loan Interest Payment'
      AND Debit_Credit = 'Debit'
      AND tran_date BETWEEN ? AND ?
  `, [start_date, end_date]);

  // Processing fees income
  const [processingFees] = await db.execute(`
    SELECT COALESCE(SUM(Amount), 0) AS total
    FROM transactions
    WHERE transaction_type = 'Processing Fee'
      AND Debit_Credit = 'Debit'
      AND tran_date BETWEEN ? AND ?
  `, [start_date, end_date]);

  // Penalty income
  const [penaltyIncome] = await db.execute(`
    SELECT COALESCE(SUM(Amount), 0) AS total
    FROM transactions
    WHERE transaction_type = 'Penalty'
      AND Debit_Credit = 'Debit'
      AND tran_date BETWEEN ? AND ?
  `, [start_date, end_date]);

  // Membership fees
  const [membershipFees] = await db.execute(`
    SELECT COALESCE(SUM(Amount), 0) AS total
    FROM transactions
    WHERE transaction_type = 'Membership Fee'
      AND Debit_Credit = 'Debit'
      AND tran_date BETWEEN ? AND ?
  `, [start_date, end_date]);

  // Other income
  const [otherIncome] = await db.execute(`
    SELECT COALESCE(SUM(Amount), 0) AS total
    FROM transactions
    WHERE transaction_type IN ('Other Income', 'Donation', 'Grant')
      AND Debit_Credit = 'Debit'
      AND tran_date BETWEEN ? AND ?
  `, [start_date, end_date]);

  const income_items = [
    { category: 'Interest on Loans', amount: parseFloat(interestIncome[0].total) },
    { category: 'Processing Fees', amount: parseFloat(processingFees[0].total) },
    { category: 'Penalties', amount: parseFloat(penaltyIncome[0].total) },
    { category: 'Membership Fees', amount: parseFloat(membershipFees[0].total) },
    { category: 'Other Income', amount: parseFloat(otherIncome[0].total) }
  ];

  // Expenses (example - adjust based on your expense tracking)
  const [operatingExpenses] = await db.execute(`
    SELECT COALESCE(SUM(Amount), 0) AS total
    FROM transactions
    WHERE transaction_type IN ('Operating Expense', 'Administrative Expense', 'Salary', 'Rent', 'Utilities')
      AND Debit_Credit = 'Debit'
      AND tran_date BETWEEN ? AND ?
  `, [start_date, end_date]);

  const expense_items = [
    { category: 'Operating Expenses', amount: parseFloat(operatingExpenses[0].total) },
    { category: 'Administrative Expenses', amount: 0 },
    { category: 'Loan Loss Provisions', amount: 0 }
  ];

  // ── BALANCE SHEET DATA ───────────────────────────────────────────

  // Cash and cash equivalents (from liquidity funds)
  const [cashBalance] = await db.execute(`
    SELECT COALESCE(SUM(amount), 0) AS total
    FROM liquidity_funds
    WHERE as_at_date = (SELECT MAX(as_at_date) FROM liquidity_funds WHERE as_at_date <= ?)
  `, [end_date]);

  // Loans receivable (outstanding loan balances)
  const [loansReceivable] = await db.execute(`
    SELECT COALESCE(SUM(balance), 0) AS total
    FROM loans
    WHERE status = 'Active'
      AND disbursement_date <= ?
  `, [end_date]);

  // Total savings (member deposits)
  const [memberSavings] = await db.execute(`
    SELECT COALESCE(SUM(Amount), 0) AS total
    FROM transactions
    WHERE transaction_type = 'Savings Deposit'
      AND Debit_Credit = 'Credit'
      AND tran_date <= ?
  `, [end_date]);

  // Savings withdrawals
  const [savingsWithdrawals] = await db.execute(`
    SELECT COALESCE(SUM(Amount), 0) AS total
    FROM transactions
    WHERE transaction_type = 'Savings Withdrawal'
      AND Debit_Credit = 'Debit'
      AND tran_date <= ?
  `, [end_date]);

  const netSavings = parseFloat(memberSavings[0].total) - parseFloat(savingsWithdrawals[0].total);

  // Retained earnings (cumulative net income)
  const [retainedEarnings] = await db.execute(`
    SELECT 
      COALESCE(SUM(CASE WHEN Debit_Credit='Debit' THEN Amount ELSE 0 END), 0) -
      COALESCE(SUM(CASE WHEN Debit_Credit='Credit' AND transaction_type NOT IN ('Loan Disbursement') THEN Amount ELSE 0 END), 0) AS total
    FROM transactions
    WHERE tran_date <= ?
  `, [end_date]);

  const asset_items = [
    { category: 'Cash and Cash Equivalents', amount: parseFloat(cashBalance[0].total) },
    { category: 'Loans Receivable', amount: parseFloat(loansReceivable[0].total) },
    { category: 'Accrued Interest Receivable', amount: 0 }
  ];

  const liability_items = [
    { category: 'Member Savings Deposits', amount: netSavings },
    { category: 'Accrued Expenses', amount: 0 }
  ];

  const equity_items = [
    { category: 'Share Capital', amount: 0 },
    { category: 'Retained Earnings', amount: parseFloat(retainedEarnings[0].total) }
  ];

  // ── CASH FLOW DATA ───────────────────────────────────────────────

  // Cash from operations (net income + adjustments)
  const totalIncome = income_items.reduce((s, i) => s + i.amount, 0);
  const totalExpenses = expense_items.reduce((s, i) => s + i.amount, 0);
  const netIncome = totalIncome - totalExpenses;

  // Loan disbursements (cash outflow)
  const [loanDisbursements] = await db.execute(`
    SELECT COALESCE(SUM(net_disbursement), SUM(loan_amount), 0) AS total
    FROM loans
    WHERE disbursement_date BETWEEN ? AND ?
  `, [start_date, end_date]);

  // Loan repayments received (cash inflow)
  const [loanRepayments] = await db.execute(`
    SELECT COALESCE(SUM(Amount), 0) AS total
    FROM transactions
    WHERE transaction_type IN ('Loan Payment', 'Loan Principal Payment')
      AND Debit_Credit = 'Debit'
      AND tran_date BETWEEN ? AND ?
  `, [start_date, end_date]);

  // Member savings deposits (cash inflow)
  const [savingsDeposits] = await db.execute(`
    SELECT COALESCE(SUM(Amount), 0) AS total
    FROM transactions
    WHERE transaction_type = 'Savings Deposit'
      AND Debit_Credit = 'Credit'
      AND tran_date BETWEEN ? AND ?
  `, [start_date, end_date]);

  // Member savings withdrawals (cash outflow)
  const [savingsWithdrawn] = await db.execute(`
    SELECT COALESCE(SUM(Amount), 0) AS total
    FROM transactions
    WHERE transaction_type = 'Savings Withdrawal'
      AND Debit_Credit = 'Debit'
      AND tran_date BETWEEN ? AND ?
  `, [start_date, end_date]);

  const operating_cashflow = [
    { category: 'Net Income', amount: netIncome },
    { category: 'Loan Repayments Received', amount: parseFloat(loanRepayments[0].total) },
    { category: 'Interest and Fees Collected', amount: parseFloat(interestIncome[0].total) + parseFloat(processingFees[0].total) }
  ];

  const investing_cashflow = [
    { category: 'Loans Disbursed', amount: -parseFloat(loanDisbursements[0].total) }
  ];

  const financing_cashflow = [
    { category: 'Member Savings Deposits', amount: parseFloat(savingsDeposits[0].total) },
    { category: 'Member Savings Withdrawals', amount: -parseFloat(savingsWithdrawn[0].total) }
  ];

  // Opening cash balance
  const [openingCash] = await db.execute(`
    SELECT COALESCE(SUM(amount), 0) AS total
    FROM liquidity_funds
    WHERE as_at_date = (SELECT MAX(as_at_date) FROM liquidity_funds WHERE as_at_date < ?)
  `, [start_date]);

  // ── GENERATE EXCEL ───────────────────────────────────────────────

  const statementData = {
    sacco_name,
    start_date,
    end_date,
    income_items,
    expense_items,
    asset_items,
    liability_items,
    equity_items,
    operating_cashflow,
    investing_cashflow,
    financing_cashflow,
    opening_cash: parseFloat(openingCash[0].total)
  };

  const { spawn } = require('child_process');
  const python = spawn('python3', ['/home/claude/create_financial_statements.py']);

  python.stdin.write(JSON.stringify(statementData));
  python.stdin.end();

  let output = '';
  let error = '';

  python.stdout.on('data', (data) => { output += data.toString(); });
  python.stderr.on('data', (data) => { error += data.toString(); });

  python.on('close', async (code) => {
    if (code !== 0) {
      console.error('Python script error:', error);
      return res.status(500).json({ error: 'Failed to generate statements', details: error });
    }

    // Recalculate formulas
    const { execSync } = require('child_process');
    try {
      execSync('python3 /mnt/skills/public/xlsx/scripts/recalc.py /mnt/user-data/outputs/financial_statements.xlsx', { timeout: 30000 });
    } catch (e) {
      console.error('Recalc error:', e.message);
    }

    res.json({
      success: true,
      message: 'Financial statements generated successfully',
      filename: 'financial_statements.xlsx'
    });
  });
}));
