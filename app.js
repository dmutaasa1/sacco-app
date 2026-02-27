const express = require('express');
const mysql = require('mysql2/promise');
const moment = require('moment');
const session = require('express-session');
const bcrypt = require('bcrypt');
const multer = require('multer');
const cron = require('node-cron');
const crypto = require('crypto');

const app = express();
app.set('trust proxy', 1); 

// Middleware Configuration
app.set('view engine', 'ejs');
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(express.static('public'));
app.use(express.static('partials'));

// Session configuration

const MySQLStore = require('express-mysql-session')(session);

const sessionStore = new MySQLStore({
  host: process.env.MYSQLHOST,
  user: process.env.MYSQLUSER,
  password: process.env.MYSQLPASSWORD,
  database: process.env.MYSQLDATABASE,  
  clearExpired: true,
  checkExpirationInterval: 300000, 
  expiration: 300000, 
});

app.use(session({
  key: 'session_cookie_name',
  secret: process.env.SESSION_SECRET,
  store: sessionStore,
  resave: false,
  saveUninitialized: false,
  cookie: {
    secure: process.env.NODE_ENV === 'production', // true on Railway (HTTPS)
    httpOnly: true,
    sameSite: 'lax',
    maxAge: 86400000 // 1 day
  }
}));


app.set('trust proxy', 1);



// MySQL connection pool



const dbConfig = mysql.createPool({
  host: process.env.MYSQLHOST ,
  user: process.env.MYSQLUSER ,
  password: process.env.MYSQLPASSWORD ,
  database: process.env.MYSQLDATABASE ,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  enableKeepAlive: true,
  keepAliveInitialDelay: 0
});

// Multer configuration
const storage = multer.memoryStorage();
const upload = multer({ 
  storage: storage,
  limits: { fileSize: 5 * 1024 * 1024 }
});

// ==================== STARTUP INTEREST CALCULATION ====================

async function runStartupInterestCalculation() {
    console.log('\n=================================================');
    console.log('STARTING INTEREST CALCULATION CHECK...');
    console.log('=================================================');
    
    try {
        const db = dbConfig;
        
        // Get the last date interest was calculated
        const [lastCalc] = await db.query(`
            SELECT MAX(calculation_date) AS last_calculation_date
            FROM loan_daily_interest
        `);
        
        let startDate;
        const yesterday = new Date();
        yesterday.setDate(yesterday.getDate() );
        yesterday.setHours(0, 0, 0, 0);
        
        if (lastCalc[0].last_calculation_date) {
            // Start from the day after last calculation
            startDate = new Date(lastCalc[0].last_calculation_date);
            startDate.setDate(startDate.getDate() + 1);
            
            console.log(`Last interest calculation: ${lastCalc[0].last_calculation_date}`);
        } else {
            // No previous calculations - get earliest loan disbursement date
            const [firstLoan] = await db.query(`
                SELECT MIN(disbursement_date) AS first_loan_date
                FROM loans
                WHERE status IN ('Active', 'Completed')
            `);
            
            if (firstLoan[0].first_loan_date) {
                startDate = new Date(firstLoan[0].first_loan_date);
                console.log(`No previous calculations found. Starting from first loan: ${firstLoan[0].first_loan_date}`);
            } else {
                console.log('No loans found. Skipping interest calculation.');
                console.log('=================================================\n');
                return;
            }
        }
        
        // Check if we need to calculate for any days
        if (startDate > yesterday) {
            console.log('✓ Interest calculations are up to date!');
            console.log('=================================================\n');
            return;
        }
        
        // Calculate how many days we need to process
        const daysDiff = Math.ceil((yesterday - startDate) / (1000 * 60 * 60 * 24));
        console.log(`\n⚠ Found ${daysDiff} day(s) of missing interest calculations`);
        console.log(`Processing from ${startDate.toISOString().split('T')[0]} to ${yesterday.toISOString().split('T')[0]}`);
        console.log('---------------------------------------------------');
        
        // Get all loans that need interest calculation
        const [loans] = await db.query(`
            SELECT 
                id, 
                member_id,
                loan_amount, 
                balance, 
                interest_rate, 
                accumulated_interest, 
                disbursement_date,
                status
            FROM loans 
            WHERE status IN ('Active', 'Completed')  AND interest_method = 'reducing_balance'
            AND disbursement_date <= ?
            ORDER BY id
        `, [yesterday.toISOString().split('T')[0]]);
        
        console.log(`Processing ${loans.length} loan(s)...\n`);
        
        let totalInterestCalculated = 0;
        let totalDaysProcessed = 0;
        let loansProcessed = 0;
        
        for (const loan of loans) {
            const result = await calculateMissingInterestForLoan(loan, startDate, yesterday);
            
            if (result.daysProcessed > 0) {
                loansProcessed++;
                totalInterestCalculated += result.totalInterest;
                totalDaysProcessed += result.daysProcessed;
                
                console.log(`  ✓ Loan #${loan.id}: ${result.daysProcessed} days, UGX ${result.totalInterest.toFixed(2)}`);
            }
        }
        
        console.log('\n---------------------------------------------------');
        console.log('STARTUP INTEREST CALCULATION COMPLETED');
        console.log('---------------------------------------------------');
        console.log(`Loans Processed: ${loansProcessed}`);
        console.log(`Total Days Calculated: ${totalDaysProcessed}`);
        console.log(`Total Interest Calculated: UGX ${totalInterestCalculated.toFixed(2)}`);
        console.log('=================================================\n');
        
    } catch (err) {
        console.error('❌ ERROR IN STARTUP INTEREST CALCULATION:', err);
        console.log('=================================================\n');
    }
}


// Calculate missing interest for a specific loan
// Calculate missing interest for a specific loan - OPTIMIZED VERSION
// Calculate missing interest for a specific loan - MATCHES CRON JOB LOGIC
async function calculateMissingInterestForLoan(loan, startDate, endDate) {
    const db = dbConfig;
    
    let calcStartDate = new Date(Math.max(startDate, new Date(loan.disbursement_date)));
    const calcEndDate = new Date(endDate);
    
    const dailyInterestRate = (loan.interest_rate / 100) / 365;
    let currentDate = new Date(calcStartDate);
    let totalInterest = 0;
    let daysProcessed = 0;
    
    console.log(`  Processing Loan #${loan.id}:`);
    console.log(`    Period: ${calcStartDate.toISOString().split('T')[0]} to ${calcEndDate.toISOString().split('T')[0]}`);
    
    // For each day, get the actual balance as of that date
    while (currentDate <= calcEndDate) {
        const dateStr = currentDate.toISOString().split('T')[0];
        
        // Get the balance as of this date by calculating: loan_amount - all principal paid up to this date
        const [balanceQuery] = await db.execute(`
            SELECT 
                ? - COALESCE(SUM(principal_paid), 0) as balance_on_date
            FROM loan_payment_history
            WHERE loan_id = ? 
              AND DATE(payment_date) <= ?
        `, [loan.loan_amount, loan.id, dateStr]);
        
        const balanceOnDate = parseFloat(balanceQuery[0].balance_on_date);
        
        // Only calculate interest if there's a positive balance (same as cron job)
        if (balanceOnDate > 0.01) {
            // Check if already calculated for this date
            const [existing] = await db.execute(
                `SELECT id FROM loan_daily_interest 
                 WHERE loan_id = ? AND calculation_date = ?`,
                [loan.id, dateStr]
            );
            
            if (existing.length === 0) {
                // Calculate daily interest (same formula as cron job)
                const dailyInterest = balanceOnDate * dailyInterestRate;
                
                // Insert daily interest record (same as cron job)
                await db.execute(`
                    INSERT INTO loan_daily_interest 
                    (loan_id, calculation_date, outstanding_balance, daily_interest_amount, annual_interest_rate)
                    VALUES (?, ?, ?, ?, ?)
                `, [loan.id, dateStr, balanceOnDate, dailyInterest, loan.interest_rate]);
                
                totalInterest += dailyInterest;
                daysProcessed++;
                
                // Log every 10 days for visibility
                if (daysProcessed === 1 || daysProcessed % 10 === 0) {
                    console.log(`    ${dateStr}: Balance = ${balanceOnDate.toFixed(2)}, Interest = ${dailyInterest.toFixed(2)}`);
                }
            }
        }
        
        currentDate.setDate(currentDate.getDate() + 1);
    }
    
    // Update loan's accumulated interest (same as cron job)
    if (totalInterest > 0) {
        await db.execute(`
            UPDATE loans 
            SET accumulated_interest = accumulated_interest + ?,
                total_repayment = loan_amount + total_interest + accumulated_interest + ?
            WHERE id = ?
        `, [totalInterest, totalInterest, loan.id]);
        
        console.log(`  ✓ Loan #${loan.id}: ${daysProcessed} days, Interest: ${totalInterest.toFixed(2)}`);
    }
    
    return {
        totalInterest: totalInterest,
        daysProcessed: daysProcessed
    };
}
// ==================== MIDDLEWARE ====================

// Authentication middleware
function checkAuth(req, res, next) {
  if (req.session && req.session.user) {
    next();
  } else {
    res.redirect('/login');
  }
}

// Error handling middleware
function asyncHandler(fn) {
  return (req, res, next) => {
    Promise.resolve(fn(req, res, next)).catch(next);
  };
}

//// Add this RIGHT AFTER your middleware and BEFORE everything else

// ==================== USER MANAGEMENT ROUTES ====================

// GET: User management page
app.get('/users', checkAuth, asyncHandler(async (req, res) => {
  // Check if user has admin role
  if (req.session.user.role !== 'Admin') {
    return res.status(403).send('Access denied. Admin only.');
  }

  const db = dbConfig;
  
  try {
    const [users] = await db.execute(`
      SELECT 
        u.id, 
        u.username, 
        u.first_name, 
        u.last_name, 
        u.role, 
        u.status,
        DATE_FORMAT(u.last_login, '%d-%b-%Y %H:%i') AS last_login,
        DATE_FORMAT(u.created_at, '%d-%b-%Y') AS created_at,
        creator.username AS created_by_username
      FROM users u
      LEFT JOIN users creator ON u.created_by = creator.id
      ORDER BY u.created_at DESC
    `);

    console.log('Users fetched:', users.length);

    res.render('users', { 
      users: users || [],
      currentPage: 'users', 
      user: req.session.user 
    });
  } catch (err) {
    console.error('Error fetching users:', err);
    res.render('users', { 
      users: [], 
      currentPage: 'users', 
      user: req.session.user,
      error: err.message
    });
  }
}));

// GET: Add user form
app.get('/users/add', checkAuth, asyncHandler(async (req, res) => {
  if (req.session.user.role !== 'Admin') {
    return res.status(403).send('Access denied. Admin only.');
  }

  res.render('user_add', { 
    currentPage: 'users', 
    user: req.session.user 
  });
}));

// POST: Create new user
app.post('/users/add', checkAuth, asyncHandler(async (req, res) => {
  if (req.session.user.role !== 'Admin') {
    return res.status(403).json({ error: 'Access denied' });
  }

  const db = dbConfig;
  const { username, password, confirm_password, first_name, last_name, role, status } = req.body;
  const errors = [];

  // Validations
  if (!username) errors.push("Username is required");
  if (!password) errors.push("Password is required");
  if (!first_name) errors.push("First name is required");
  if (!last_name) errors.push("Last name is required");
  if (!role) errors.push("Role is required");

  if (password && password.length < 6) {
    errors.push("Password must be at least 6 characters");
  }

  if (password !== confirm_password) {
    errors.push("Passwords do not match");
  }

  // Check username uniqueness
  const [existingUser] = await db.execute(
    'SELECT id FROM users WHERE username = ?',
    [username]
  );

  if (existingUser.length > 0) {
    errors.push("Username already exists");
  }

  if (errors.length > 0) {
    return res.status(400).json({ errors });
  }

  // Hash password
  const hashedPassword = await bcrypt.hash(password, 10);

  // Insert user
  await db.execute(`
    INSERT INTO users 
    (username, password, first_name, last_name, role, status, created_by, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, NOW())
  `, [
    username, 
    hashedPassword, 
    first_name, 
    last_name, 
    role || 'user', 
    status || 'active',
    req.session.user.id
  ]);

  res.json({
    success: true,
    message: 'User created successfully',
    user: { username, first_name, last_name, role }
  });
}));

// GET: Edit user form
app.get('/users/:id/edit', checkAuth, asyncHandler(async (req, res) => {
  if (req.session.user.role !== 'Admin') {
    return res.status(403).send('Access denied. Admin only.');
  }

  const { id } = req.params;
  const db = dbConfig;

  const [users] = await db.execute(`
    SELECT id, username, first_name, last_name, role, status
    FROM users 
    WHERE id = ?
  `, [id]);

  if (users.length === 0) {
    return res.status(404).send('User not found');
  }

  res.render('user_edit', { 
    editUser: users[0], 
    currentPage: 'users', 
    user: req.session.user 
  });
}));

// POST: Update user
app.post('/users/:id/edit', checkAuth, asyncHandler(async (req, res) => {
  if (req.session.user.role !== 'Admin') {
    return res.status(403).json({ error: 'Access denied' });
  }

  const { id } = req.params;
  const db = dbConfig;
  const { username, first_name, last_name, role, status } = req.body;
  const errors = [];

  // Validations
  if (!username) errors.push("Username is required");
  if (!first_name) errors.push("First name is required");
  if (!last_name) errors.push("Last name is required");
  if (!role) errors.push("Role is required");

  // Check username uniqueness (excluding current user)
  const [existingUser] = await db.execute(
    'SELECT id FROM users WHERE username = ? AND id != ?',
    [username, id]
  );

  if (existingUser.length > 0) {
    errors.push("Username already exists");
  }

  if (errors.length > 0) {
    return res.status(400).json({ errors });
  }

  // Update user
  await db.execute(`
    UPDATE users 
    SET username = ?, first_name = ?, last_name = ?, role = ?, status = ?
    WHERE id = ?
  `, [username, first_name, last_name, role, status || 'active', id]);

  res.json({
    success: true,
    message: 'User updated successfully'
  });
}));

// POST: Change password (by admin or self)
app.post('/users/:id/change-password', checkAuth, asyncHandler(async (req, res) => {
  const { id } = req.params;
  const { current_password, new_password, confirm_password } = req.body;
  const db = dbConfig;
  const errors = [];

  // Check if user is changing their own password or is admin
  const isOwnPassword = parseInt(id) === req.session.user.id;
  const isAdmin = req.session.user.role === 'Admin';

  if (!isOwnPassword && !isAdmin) {
    return res.status(403).json({ error: 'Access denied' });
  }

  // Validations
  if (!new_password) errors.push("New password is required");
  if (new_password && new_password.length < 6) {
    errors.push("Password must be at least 6 characters");
  }
  if (new_password !== confirm_password) {
    errors.push("Passwords do not match");
  }

  // If changing own password, verify current password
  if (isOwnPassword && !isAdmin) {
    if (!current_password) errors.push("Current password is required");

    const [user] = await db.execute('SELECT password FROM users WHERE id = ?', [id]);
    if (user.length > 0) {
      const match = await bcrypt.compare(current_password, user[0].password);
      if (!match) {
        errors.push("Current password is incorrect");
      }
    }
  }

  if (errors.length > 0) {
    return res.status(400).json({ errors });
  }

  // Hash new password
  const hashedPassword = await bcrypt.hash(new_password, 10);

  // Update password
  await db.execute('UPDATE users SET password = ? WHERE id = ?', [hashedPassword, id]);

  res.json({
    success: true,
    message: 'Password changed successfully'
  });
}));

// POST: Delete/Deactivate user
app.post('/users/:id/delete', checkAuth, asyncHandler(async (req, res) => {
  if (req.session.user.role !== 'Admin') {
    return res.status(403).json({ error: 'Access denied' });
  }

  const { id } = req.params;
  const db = dbConfig;

  // Prevent self-deletion
  if (parseInt(id) === req.session.user.id) {
    return res.status(400).json({ error: 'Cannot delete your own account' });
  }

  // Soft delete - just set status to inactive
  await db.execute('UPDATE users SET status = ? WHERE id = ?', ['inactive', id]);

  res.json({
    success: true,
    message: 'User deactivated successfully'
  });
}));



// ==================== PROFILE & SETTINGS ROUTES ====================

// GET: User profile
app.get('/profile', checkAuth, asyncHandler(async (req, res) => {
  const db = dbConfig;
  
  const [userDetails] = await db.execute(`
    SELECT 
      id, 
      username, 
      first_name, 
      last_name, 
      role, 
      status,
      DATE_FORMAT(last_login, '%d-%b-%Y %H:%i') AS last_login,
      DATE_FORMAT(created_at, '%d-%b-%Y') AS created_at
    FROM users 
    WHERE id = ?
  `, [req.session.user.id]);

  if (userDetails.length === 0) {
    return res.status(404).send('User not found');
  }

  res.render('profile', { 
    currentPage: 'profile', 
    user: req.session.user,
    userDetails: userDetails[0]
  });
}));

// POST: Update profile
app.post('/profile/update', checkAuth, asyncHandler(async (req, res) => {
  const { first_name, last_name } = req.body;
  const db = dbConfig;
  const errors = [];

  if (!first_name) errors.push("First name is required");
  if (!last_name) errors.push("Last name is required");

  if (errors.length > 0) {
    return res.status(400).json({ errors });
  }

  await db.execute(`
    UPDATE users 
    SET first_name = ?, last_name = ?
    WHERE id = ?
  `, [first_name, last_name, req.session.user.id]);

  // Update session
  req.session.user.first_name = first_name;
  req.session.user.last_name = last_name;

  res.json({
    success: true,
    message: 'Profile updated successfully'
  });
}));

// POST: Change own password
app.post('/profile/change-password', checkAuth, asyncHandler(async (req, res) => {
  const { current_password, new_password, confirm_password } = req.body;
  const db = dbConfig;
  const errors = [];

  if (!current_password) errors.push("Current password is required");
  if (!new_password) errors.push("New password is required");
  if (new_password && new_password.length < 6) {
    errors.push("Password must be at least 6 characters");
  }
  if (new_password !== confirm_password) {
    errors.push("Passwords do not match");
  }

  if (errors.length > 0) {
    return res.status(400).json({ errors });
  }

  // Verify current password
  const [user] = await db.execute('SELECT password FROM users WHERE id = ?', [req.session.user.id]);
  
  if (user.length === 0) {
    return res.status(404).json({ errors: ['User not found'] });
  }

  const match = await bcrypt.compare(current_password, user[0].password);
  
  if (!match) {
    return res.status(400).json({ errors: ['Current password is incorrect'] });
  }

  // Hash new password
  const hashedPassword = await bcrypt.hash(new_password, 10);

  // Update password
  await db.execute('UPDATE users SET password = ? WHERE id = ?', [hashedPassword, req.session.user.id]);

  res.json({
    success: true,
    message: 'Password changed successfully'
  });
}));

// GET: Settings page
app.get('/settings', checkAuth, asyncHandler(async (req, res) => {
  const db = dbConfig;
  
  const [userSettings] = await db.execute(`
    SELECT 
      id, 
      username, 
      first_name, 
      last_name, 
      role, 
      status
    FROM users 
    WHERE id = ?
  `, [req.session.user.id]);

  res.render('settings', { 
    currentPage: 'settings', 
    user: req.session.user,
    userSettings: userSettings[0]
  });
}));

// GET: Support page
app.get('/support', checkAuth, asyncHandler(async (req, res) => {
  res.render('support', { 
    currentPage: 'support', 
    user: req.session.user
  });
}));

// POST: Submit support ticket
app.post('/support/ticket', checkAuth, asyncHandler(async (req, res) => {
  const { subject, category, priority, message } = req.body;
  const db = dbConfig;
  const errors = [];

  if (!subject) errors.push("Subject is required");
  if (!category) errors.push("Category is required");
  if (!message) errors.push("Message is required");

  if (errors.length > 0) {
    return res.status(400).json({ errors });
  }

  await db.execute(`
    INSERT INTO support_tickets 
    (user_id, subject, category, priority, message, status, created_at)
    VALUES (?, ?, ?, ?, ?, 'Open', NOW())
  `, [req.session.user.id, subject, category, priority || 'Medium', message]);

  res.json({
    success: true,
    message: 'Support ticket submitted successfully. We will get back to you soon.'
  });
}));




// ==================== AUTHENTICATION ROUTES ====================

// GET: Login page
app.get('/login', (req, res) => {
  res.render('login', { error: null });
});

// POST: Handle login
// POST: Handle login
app.post('/login', asyncHandler(async (req, res) => {
  const { username, password } = req.body;
  const db = dbConfig;
  
  const [rows] = await db.execute('SELECT * FROM users WHERE status ="Active" and username = ?', [username]);

  if (rows.length === 0) {
    return res.status(401).json({ error: 'User not found or Disabled' });
  }

  const user = rows[0];
  const match = await bcrypt.compare(password, user.password);
  
  if (!match) {
    return res.status(401).json({ error: 'Incorrect password' });
  }

  await db.execute("UPDATE users SET last_login = NOW() WHERE id = ?", [user.id]);

  req.session.user = {
    id: user.id,
    username: user.username,
    first_name: user.first_name,
    last_name: user.last_name,
    role: user.role
  };

  // ✅ Wait for session to be saved before redirecting
  req.session.save((err) => {
    if (err) {
      console.error('Session save error:', err);
      return res.status(500).json({ error: 'Session error' });
    }
    res.redirect('/');
  });
}));
// GET: Logout
app.get('/logout', (req, res) => {
  req.session.destroy(err => {
    if (err) console.error(err);
    res.redirect('/login');
  });
});

// ==================== DASHBOARD ====================
  app.get('/', checkAuth, asyncHandler(async (req, res) => {
  const db = dbConfig;

  // Active members count
  const [memberCount] = await db.execute(
    'SELECT COUNT(*) AS memberCount FROM members_mst WHERE status = "ACTIVE"'
  );

  // Active members count
  const [YtdmemberCount] = await db.execute(
    'SELECT COUNT(*) AS ytd_member_Count FROM members_mst WHERE status = "ACTIVE" and  year(date_joined) = YEAR(CURDATE())' 
  );
  
  
  // Total savings
  const [savings] = await db.execute(
    'SELECT SUM(Amount) AS saving_sum FROM transactions WHERE transaction_type = "Saving"'
  );

  // Year to date savings
  const [savingsYTD] = await db.execute(
    'SELECT SUM(Amount) AS saving_ytd_sum FROM transactions WHERE transaction_type = "Saving" and year(tran_date) = YEAR(CURDATE())'
  );
  
  
  // Total income for current year
  const [income] = await db.execute(

    'select sum(income) income_sum from ( SELECT SUM(Amount) AS income FROM transactions t WHERE transaction_type LIKE "%Income%" AND payment_period = YEAR(CURDATE())      union select sum(interest_paid) as income from loan_payment_history lph  where year(lph.payment_date) = YEAR(CURDATE()) ) as utp'
    //'SELECT SUM(Amount) AS income_sum FROM transactions WHERE transaction_type LIKE "%Income%" AND payment_period = YEAR(CURDATE())'
  );

  // Total income for all years
  const [Allincome] = await db.execute(

    'select sum(income) full_income_sum from ( SELECT SUM(Amount) AS income FROM transactions t WHERE transaction_type LIKE "%Income%"  union select sum(interest_paid) as income from loan_payment_history lph  ) as utp'
    //'SELECT SUM(Amount) AS income_sum FROM transactions WHERE transaction_type LIKE "%Income%" AND payment_period = YEAR(CURDATE())'
  );

  // Total loans disbursed
  const [totalLoans] = await db.execute(
    'SELECT SUM(loan_amount) AS total_loans FROM loans'
  );

  // Outstanding loan balance
  const [outstandingLoans] = await db.execute(
    'SELECT SUM(balance) AS outstanding_balance FROM loans WHERE status = "Active"'
  );

  // Monthly savings for current year
  const [monthlySavings] = await db.execute(`
    SELECT 
      MONTH(tran_date) AS month,
      SUM(Amount) AS total
    FROM transactions
    WHERE transaction_type = 'Saving' 
      AND YEAR(tran_date) = YEAR(CURDATE())
    GROUP BY MONTH(tran_date)
    ORDER BY MONTH(tran_date)
  `);

   // Monthly loans for current year
  const [monthlyloans] = await db.execute(`
     SELECT 
      MONTH(disbursement_date) AS month,
      SUM(loan_amount) AS total
    FROM loans
    WHERE 
       YEAR(disbursement_date) = YEAR(CURDATE())
    GROUP BY MONTH(disbursement_date)
    ORDER BY MONTH(disbursement_date)
  `);

  // Monthly income for current year
  const [monthlyIncome] = await db.execute(`
   select month, sum(total) as total from
(
SELECT 
      MONTH(tran_date) AS month,
      SUM(Amount) AS total
    FROM transactions
    WHERE transaction_type LIKE '%Income%'
      AND YEAR(tran_date) = YEAR(CURDATE())
    GROUP BY MONTH(tran_date)
   -- ORDER BY MONTH(tran_date)    
    union    
    SELECT 
      MONTH(payment_date) AS month,
      SUM(interest_paid) AS total
    FROM loan_payment_history
    WHERE  YEAR(payment_date) = YEAR(CURDATE())
    GROUP BY MONTH(payment_date)
    ) tmp group by month order by month
  `);

  // Income breakdown
  const [incomeBreakdown] = await db.execute(`
    SELECT 
      SUM(CASE WHEN transaction_type = 'Loan Debit Intrest' THEN Amount ELSE 0 END) +
      (select sum(interest_paid) from loan_payment_history WHERE YEAR(payment_date) = YEAR(CURDATE())) as interest_income,
      SUM(CASE WHEN transaction_type = 'Penalty Fees' THEN Amount ELSE 0 END) AS penalty_income,
      SUM(CASE WHEN transaction_type LIKE '%Membership%' OR transaction_type LIKE '%welfare%' THEN Amount ELSE 0 END) AS fee_income
    FROM transactions
    WHERE YEAR(tran_date) = YEAR(CURDATE())
  `);

  // Recent transactions
  const [recentTransactions] = await db.execute(`
    SELECT 
      t.id,
      t.member_id,
      CONCAT(m.First_name, ' ', m.Last_Name) AS member_name,
      t.Amount,
      t.transaction_type,
      t.Debit_Credit,
      DATE_FORMAT(t.tran_date, '%d/%m/%Y') AS tran_date,
      t.created_at
    FROM transactions t
    JOIN members_mst m ON t.member_id = m.id
    ORDER BY t.created_at DESC
    LIMIT 10
  `);

  // Active loans with overdue status
  const [loanStats] = await db.execute(`
    SELECT 
      COUNT(DISTINCT l.id) AS total_active_loans,
      COUNT(DISTINCT CASE WHEN ls.status = 'Overdue' THEN l.id END) AS loans_with_overdue,
      SUM(l.balance) AS total_outstanding
    FROM loans l
    LEFT JOIN loan_schedule ls ON l.id = ls.loan_id
    WHERE l.status = 'Active'
  `);

  // Top borrowers
  const [topBorrowers] = await db.execute(`
    SELECT 
      m.id,
      CONCAT(m.First_name, ' ', m.Last_Name) AS name,
      SUM(l.loan_amount) AS total_borrowed,
      COUNT(l.id) AS loan_count
    FROM members_mst m
    JOIN loans l ON m.id = l.member_id
    GROUP BY m.id, m.First_name, m.Last_Name
    ORDER BY total_borrowed DESC
    LIMIT 5
  `);

  // Prepare monthly data arrays (all 12 months)
  const savingsData = new Array(12).fill(0);
  const incomeData = new Array(12).fill(0);
  const loansData = new Array(12).fill(0);

  monthlySavings.forEach(row => {
    savingsData[row.month - 1] = row.total;
  });

  monthlyIncome.forEach(row => {
    incomeData[row.month - 1] = row.total;
  });

  monthlyloans.forEach(row => {
    loansData[row.month - 1] = row.total;
  });

  // Calculate percentages for income breakdown
  const totalIncome = parseFloat(incomeBreakdown[0].interest_income) + 
                      parseFloat(incomeBreakdown[0].penalty_income) + 
                      parseFloat(incomeBreakdown[0].fee_income);

  const interestPercentage = totalIncome > 0 ? 
    ((incomeBreakdown[0].interest_income / totalIncome) * 100).toFixed(0) : 0;
  const penaltyPercentage = totalIncome > 0 ? 
    ((incomeBreakdown[0].penalty_income / totalIncome) * 100).toFixed(0) : 0;
  const feePercentage = totalIncome > 0 ? 
    ((incomeBreakdown[0].fee_income / totalIncome) * 100).toFixed(0) : 0;

  res.render('index', {
    currentPage: 'home',
    user: req.session.user,
    memberCount: memberCount[0].memberCount,
    ytd_memberCount: YtdmemberCount[0].ytd_member_Count,
    memberSaving: savings[0].saving_sum || 0,
    memberYTDSaving: savingsYTD[0].saving_ytd_sum||0,
    memberIncome: income[0].income_sum || 0,
    memberAllIncome: Allincome[0].full_income_sum || 0,
    totalLoans: totalLoans[0].total_loans || 0,
    outstandingLoans: outstandingLoans[0].outstanding_balance || 0,
    savingsData: JSON.stringify(savingsData),
    incomeData: JSON.stringify(incomeData),
    loansData: JSON.stringify(loansData),
    interestIncome: incomeBreakdown[0].interest_income || 0,
    penaltyIncome: incomeBreakdown[0].penalty_income || 0,
    feeIncome: incomeBreakdown[0].fee_income || 0,
    interestPercentage,
    penaltyPercentage,
    feePercentage,
    recentTransactions,
    loanStats: loanStats[0],
    topBorrowers
  });
}));


// ==================== PAGE ROUTES ====================

const pageRoutes = [
  'transaction',
  'membership_fee',
  'welfare',
  'Insurance_cover',
  'savings_report',
  'member_add',
  'dependant_add',
  'transaction_list',
  'internal_transaction',
  'loan_application',
  'loan_list',
  'loan_repayment',
  'Loan_statement',
  'loans_report',
  'internal_transaction_listing'
  

  //'users' 
];

pageRoutes.forEach(page => {
  app.get(`/${page}`, checkAuth, (req, res) => {
    res.render(page, { currentPage: page, user: req.session.user });
  });
});

// ==================== TRANSACTION ROUTES ====================

// POST: Internal transaction
app.post('/internal_txns', checkAuth, asyncHandler(async (req, res) => {
  const db = dbConfig;
  const { member_id, tran_date, Amount, payment_period, transaction_type, Narration, Debit_Credit } = req.body;
  const user_name = req.session.user.username;
  const errors = [];

  // Validate member
  const [rows] = await db.execute('SELECT status FROM members_mst WHERE id = ?', [member_id]);

  if (rows.length === 0) {
    errors.push("Member not found.");
  } else if (rows[0].status.toLowerCase() !== 'internal') {
    errors.push("Only Internal member IDs can be used for this type of transactions.");
  }

  // Validations
  if (!tran_date) errors.push("Transaction date is required.");
  if (!Amount || Amount <= 0) errors.push("Amount must be greater than zero.");
  if (!payment_period) errors.push("Payment period is required.");
  if (!transaction_type) errors.push("Transaction type is required.");

  if (errors.length > 0) {
    return res.status(400).json({ errors });
  }

  // Insert transaction
  await db.execute(
    `INSERT INTO transactions 
     (member_id, tran_date, Amount, payment_Period, transaction_type, description, Debit_Credit, Posted_by)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    [member_id, tran_date, Amount, payment_period, transaction_type, Narration, Debit_Credit, user_name]
  );

  res.json({
    success: true,
    transaction: { member_id, tran_date, Amount, payment_period, transaction_type, Narration, Debit_Credit }
  });
}));

// POST: Member transactions
app.post('/member_txns', checkAuth, asyncHandler(async (req, res) => {
  const db = dbConfig;
  const { member_id, tran_date, Amount, payment_period, transaction_type, Narration, Debit_Credit } = req.body;
  const user_name = req.session.user.username;
  const errors = [];

  // Validate member
  const [rows] = await db.execute('SELECT status FROM members_mst WHERE id = ?', [member_id]);

  if (rows.length === 0) {
    errors.push("Member not found.");
  } else if (rows[0].status.toLowerCase() !== 'active') {
    errors.push("Transactions are only allowed for Active members.");
  }

  // Validations
  if (!tran_date) errors.push("Transaction date is required.");
  if (!Amount || Amount <= 0) errors.push("Amount must be greater than zero.");
  if (!payment_period) errors.push("Payment period is required.");
  if (!transaction_type) errors.push("Transaction type is required.");

  if (errors.length > 0) {
    return res.status(400).json({ errors });
  }

  // Insert transaction
  await db.execute(
    `INSERT INTO transactions 
     (member_id, tran_date, Amount, payment_Period, transaction_type, description, Debit_Credit, Posted_by)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    [member_id, tran_date, Amount, payment_period, transaction_type, Narration, Debit_Credit, user_name]
  );

  res.json({
    success: true,
    transaction: { member_id, tran_date, Amount, payment_period, transaction_type, Narration, Debit_Credit }
  });
}));

// POST: Filter transactions
app.post('/transactions/filter', asyncHandler(async (req, res) => {
  const { startDate, endDate, memberStatus, memberId, payment_period, transaction_type, start, length, draw } = req.body;
  const db = dbConfig;
  
  let query = `
    SELECT SQL_CALC_FOUND_ROWS t.*, m.status AS member_status
    FROM transactions t
    JOIN members_mst m ON t.member_id = m.id
    WHERE 1=1
  `;
  const params = [];

  if (startDate && endDate) {
    query += ` AND t.tran_date BETWEEN ? AND ?`;
    params.push(startDate, endDate);
  }
  if (memberStatus) {
    query += ` AND m.status = ?`;
    params.push(memberStatus);
  }
  if (memberId) {
    query += ` AND t.member_id = ?`;
    params.push(memberId);
  }
  if (payment_period) {
    query += ` AND t.payment_period = ?`;
    params.push(payment_period);
  }
  if (transaction_type) {
    query += ` AND t.transaction_type = ?`;
    params.push(transaction_type);
  }

  const startNum = parseInt(start) || 0;
  const lengthNum = parseInt(length) || 10;
  query += ` ORDER BY t.tran_date DESC LIMIT ${startNum}, ${lengthNum}`;

  const [rows] = await db.execute(query, params);
  
  rows.forEach(r => {
    Object.keys(r).forEach(k => {
      if (r[k] === null) r[k] = '';
    });
  });

  const [[{ total }]] = await db.query('SELECT FOUND_ROWS() AS total');

  res.json({
    draw: Number(draw) || 0,
    recordsTotal: total,
    recordsFiltered: total,
    data: rows
  });
}));

// ==================== MEMBER ROUTES ====================

// GET: Members listing
app.get('/members', checkAuth, asyncHandler(async (req, res) => {
  const db = dbConfig;

  const [rows] = await db.execute(`
    SELECT id, First_Name, Middle_Name, Last_Name, tel_no, sex, marital_status,
           DATE_FORMAT(Date_of_Birth, '%d-%b-%Y') AS Date_of_Birth,
           occupation,
           DATE_FORMAT(created_at, '%d-%b-%Y') AS Date_Joined, village_lc1, Status
    FROM members_mst 
    WHERE Status IN ('ACTIVE', 'DISABLED')
    ORDER BY id DESC
  `);

  res.render('members', { users: rows, currentPage: 'members', user: req.session.user });
}));

// GET: Member profile
app.get('/members/:id', checkAuth, asyncHandler(async (req, res) => {
  const memberId = req.params.id;
  const db = dbConfig;

  const [rows] = await db.execute(`
    SELECT 
      a.*,
      DATE_FORMAT(a.Date_of_Birth, "%d-%b-%Y") AS Date_of_Birth,
      DATE_FORMAT(a.date_joined, "%d-%b-%Y") AS date_joined,
      COALESCE(t.total_savings_credit, 0) AS total_savings_credit,
      COALESCE(t.total_insurance_cover, 0) AS total_Insuarance_cover,
      COALESCE(t.total_membership_fee, 0) AS total_Membership_Fee,
      COALESCE(t.total_loan, 0) AS total_loan,
      COALESCE(d.dependants_count, 0) AS total_dependants
    FROM members_mst a
    LEFT JOIN (
      SELECT 
        member_id,
        SUM(CASE WHEN Debit_Credit = 'Credit' AND transaction_type = 'Saving' THEN Amount ELSE 0 END) AS total_savings_credit,
        SUM(CASE WHEN Debit_Credit = 'Credit' AND transaction_type = 'Insuarance Cover' THEN Amount ELSE 0 END) AS total_insurance_cover,
        SUM(CASE WHEN Debit_Credit = 'Credit' AND transaction_type = 'Membership Fee' THEN Amount ELSE 0 END) AS total_membership_fee,
        SUM(CASE WHEN Debit_Credit = 'Credit' AND transaction_type = 'Loan Disbursement' THEN Amount ELSE 0 END) -
        SUM(CASE WHEN Debit_Credit = 'Debit' AND transaction_type = 'Loan Repayment' THEN Amount ELSE 0 END) AS total_loan
      FROM transactions
      GROUP BY member_id
    ) t ON a.id = t.member_id
    LEFT JOIN (
      SELECT person_id, COUNT(*) AS dependants_count
      FROM dependants
      GROUP BY person_id
    ) d ON a.id = d.person_id
    WHERE a.id = ?
  `, [memberId]);

  if (rows.length === 0) {
    return res.status(404).send('Member not found');
  }

  const member = rows[0];
  let photoBase64 = null;
  if (member.Passport_Photo) {
    photoBase64 = member.Passport_Photo.toString('base64');
  }

  // Get transactions
  const [trans] = await db.execute(`
    SELECT 
      id,
      DATE_FORMAT(tran_date, "%d-%b-%Y") AS tran_date,
      transaction_type,
      Debit_Credit,
      Payment_Period,
      Amount,
      description
    FROM transactions
    WHERE member_id = ?
    ORDER BY id DESC
    LIMIT 10
  `, [memberId]);

  // Get recent activities
  const [activities] = await db.execute(`
    SELECT         
      transaction_type,
      description,
      DATE_FORMAT(created_at, '%Y-%m-%d %H:%i') AS created_at         
    FROM transactions
    WHERE member_id = ?
    ORDER BY id DESC
    LIMIT 3
  `, [memberId]);

  // Get dependants
  const [dependants] = await db.execute(`
    SELECT         
      First_name,
      Last_Name,
      DATE_FORMAT(child_date_of_birth, "%d-%b-%Y") AS child_date_of_birth         
    FROM dependants
    WHERE person_id = ?
    ORDER BY id ASC
  `, [memberId]);

  // Get financial details
  const [finances] = await db.execute(`
    SELECT 
      member_id,
      YEAR(CURDATE()) AS payment_year,      
      COALESCE(t.year_total_savings_credit, 0) AS total_savings_credit,
      COALESCE(t.year_total_insurance_cover, 0) AS total_Insuarance_cover,
      COALESCE(t.year_total_welfare_fee, 0) AS total_welfare_fee,
      COALESCE(t.year_total_loan_receieved, 0) AS year_total_loan_receieved,
      COALESCE(t.year_total_loan_repayment, 0) AS year_total_loan_repayment,
      COALESCE(((t.year_total_loan_repayment) / NULLIF(t.year_total_loan_receieved, 0)) * 100, 0) AS year_percentage_loan_repayment,
      COALESCE(((t.year_total_insurance_cover) / 60000) * 100, 0) AS year_percentage_insurance_payment,
      COALESCE(((t.year_total_welfare_fee) / 240000) * 100, 0) AS year_percentage_welfare_fee
    FROM members_mst a
    LEFT JOIN (
      SELECT 
        member_id,
        SUM(CASE WHEN Debit_Credit = 'Credit' AND payment_period = YEAR(CURDATE()) AND transaction_type = 'Saving' THEN Amount ELSE 0 END) AS year_total_savings_credit,
        SUM(CASE WHEN Debit_Credit = 'Credit' AND payment_period = YEAR(CURDATE()) AND transaction_type = 'Insuarance Cover' THEN Amount ELSE 0 END) AS year_total_insurance_cover,
        SUM(CASE WHEN Debit_Credit = 'Credit' AND payment_period = YEAR(CURDATE()) AND transaction_type = 'welfare Fee' THEN Amount ELSE 0 END) AS year_total_welfare_fee,
        SUM(CASE WHEN Debit_Credit = 'Credit' AND transaction_type = 'Loan Disbursement' THEN Amount ELSE 0 END) AS year_total_loan_receieved,
        SUM(CASE WHEN Debit_Credit = 'Debit' AND transaction_type = 'Loan Repayment' THEN Amount ELSE 0 END) AS year_total_loan_repayment
      FROM transactions
      GROUP BY member_id
    ) t ON a.id = t.member_id 
    WHERE a.id = ?
    ORDER BY a.id ASC
  `, [memberId]);

  const finance = finances[0];

  res.render('member_profile', {
    member,
    trans,
    activities,
    finance,
    dependants,
    photoBase64,
    currentPage: 'members',
    user: req.session.user
  });
}));

// GET: Edit member form
app.get('/members/:id/edit', checkAuth, asyncHandler(async (req, res) => {
  const { id } = req.params;
  const db = dbConfig;

  const [rows] = await db.execute(`
    SELECT 
      id, First_Name, Middle_Name, Last_Name, tel_no, sex, marital_status,
      DATE_FORMAT(Date_of_Birth, '%Y-%m-%d') AS Date_of_Birth,
      national_id,
      DATE_FORMAT(Date_joined, '%Y-%m-%d') AS date_joined,
      occupation, village_lc1, status, Parish, sub_county, place_of_birth,
      next_of_kin_First_name, next_of_kin_Last_name, next_of_kin_tel, next_of_kin_address,
      Father_First_name, Father_Last_name, father_tel, father_status,  
      father_village, father_parish, father_subcounty, father_district,
      mother_First_name, mother_Last_name, mother_tel, mother_status,
      mother_village, mother_parish, mother_subcounty, mother_district,
      comments, Passport_Photo, Status
    FROM members_mst 
    WHERE id = ?
  `, [id]);

  if (rows.length === 0) {
    return res.status(404).send("Member not found");
  }

  const member = rows[0];
  let photoBase64 = null;
  if (member.Passport_Photo) {
    photoBase64 = member.Passport_Photo.toString('base64');
  }

  res.render('member_edit', {
    currentPage: 'home',
    member: rows[0],
    photoBase64,
    user: req.session.user
  });
}));

// POST: Update member
// POST: Update member - WITH JSON RESPONSE
app.post('/members/:id/edit', checkAuth, upload.single('photo'), asyncHandler(async (req, res) => {
  const { id } = req.params;
  const db = dbConfig;

  console.log('Updating member:', id);
  console.log('Request body:', req.body);
  console.log('File uploaded:', req.file ? 'Yes' : 'No');

  const memberData = {
    First_name: req.body.First_name || null,
    Middle_Name: req.body.Middle_Name || null,
    Last_Name: req.body.Last_Name || null,
    tel_no: req.body.tel_no || null,
    sex: req.body.sex || null,
    marital_status: req.body.marital_status || null,
    date_of_birth: req.body.date_of_birth || null,
    occupation: req.body.occupation || null,
    village_lc1: req.body.village_lc1 || null,
    status: req.body.status || null,
    national_id: req.body.national_id || null,
    date_joined: req.body.date_joined || null,
    parish: req.body.parish || null,
    sub_county: req.body.sub_county || null,
    place_of_birth: req.body.place_of_birth || null,
    next_of_kin_First_name: req.body.next_of_kin_First_name || null,
    next_of_kin_Last_name: req.body.next_of_kin_Last_name || null,
    next_of_kin_tel: req.body.next_of_kin_tel || null,
    next_of_kin_address: req.body.next_of_kin_address || null,
    Father_First_name: req.body.Father_First_name || null,
    Father_Last_name: req.body.Father_Last_name || null,
    father_tel: req.body.father_tel || null,
    father_status: req.body.father_status || null,
    father_village: req.body.father_village || null,
    father_parish: req.body.father_parish || null,
    father_subcounty: req.body.father_subcounty || null,
    father_district: req.body.father_district || null,
    mother_First_name: req.body.mother_First_name || null,
    mother_Last_name: req.body.mother_Last_name || null,
    mother_tel: req.body.mother_tel || null,
    mother_status: req.body.mother_status || null,
    mother_village: req.body.mother_village || null,
    mother_parish: req.body.mother_parish || null,
    mother_subcounty: req.body.mother_subcounty || null,
    mother_district: req.body.mother_district || null,
    comments: req.body.comments || null
  };

  try {
    // Update member data
    await db.execute(`
      UPDATE members_mst 
      SET First_name=?, Middle_Name=?, Last_Name=?, tel_no=?, sex=?, marital_status=?, 
          date_of_birth=?, occupation=?, village_lc1=?, status=?, national_id=?, date_joined=?,
          parish=?, sub_county=?, place_of_birth=?,
          next_of_kin_First_name=?, next_of_kin_Last_name=?, next_of_kin_tel=?, next_of_kin_address=?,
          Father_First_name=?, Father_Last_name=?, father_tel=?, father_status=?, father_village=?, 
          father_parish=?, father_subcounty=?, father_district=?, mother_First_name=?,
          mother_Last_name=?, mother_tel=?, mother_status=?, mother_village=?,
          mother_parish=?, mother_subcounty=?, mother_district=?, comments=?
      WHERE id=?
    `, [
      memberData.First_name, memberData.Middle_Name, memberData.Last_Name, memberData.tel_no,
      memberData.sex, memberData.marital_status, memberData.date_of_birth, memberData.occupation,
      memberData.village_lc1, memberData.status, memberData.national_id, memberData.date_joined,
      memberData.parish, memberData.sub_county, memberData.place_of_birth,
      memberData.next_of_kin_First_name, memberData.next_of_kin_Last_name, memberData.next_of_kin_tel,
      memberData.next_of_kin_address, memberData.Father_First_name, memberData.Father_Last_name,
      memberData.father_tel, memberData.father_status, memberData.father_village, memberData.father_parish,
      memberData.father_subcounty, memberData.father_district, memberData.mother_First_name,
      memberData.mother_Last_name, memberData.mother_tel, memberData.mother_status, memberData.mother_village,
      memberData.mother_parish, memberData.mother_subcounty, memberData.mother_district, memberData.comments,
      id
    ]);

    // Update photo if uploaded
    if (req.file) {
      await db.execute(`UPDATE members_mst SET Passport_Photo=? WHERE id=?`, [req.file.buffer, id]);
      console.log('Photo updated successfully');
    }

    console.log('Member updated successfully');

    // Return JSON response
    res.json({
      success: true,
      message: 'Member profile updated successfully',
      member: {
        id: id,
        first_name: memberData.First_name,
        last_name: memberData.Last_Name
      }
    });

  } catch (err) {
    console.error('Error updating member:', err);
    res.status(500).json({
      success: false,
      error: 'Failed to update member profile. Please try again.'
    });
  }
}));

// POST: Register new member
app.post('/memberadd', checkAuth, asyncHandler(async (req, res) => {
  const db = dbConfig;
  const errors = [];

  // Validate national ID uniqueness
  const [existing] = await db.execute('SELECT national_id FROM members_mst WHERE national_id = ?', [req.body.national_id]);

  if (existing.length > 0) {
    errors.push("Member with this National ID already exists");
  }

  // Required field validations
  if (!req.body.national_id) errors.push("National ID is required.");
  if (!req.body.First_name) errors.push("First Name is required.");
  if (!req.body.Last_Name) errors.push("Last Name is required.");
  if (!req.body.tel_no) errors.push("Mobile Number is required.");
  if (!req.body.date_of_birth) errors.push("Date of Birth is required.");
  if (!req.body.date_joined) errors.push("Date Joined is required.");
  if (!req.body.village_lc1) errors.push("Village is required.");

  if (errors.length > 0) {
    return res.status(400).json({ errors });
  }

  // Insert member
  await db.execute(`
    INSERT INTO members_mst 
    (First_name, Middle_Name, Last_Name, tel_no, sex, marital_status, date_of_birth, occupation,
     place_of_birth, village_lc1, parish, sub_county, status, national_id, date_joined,
     next_of_kin_First_name, next_of_kin_Last_name, next_of_kin_tel, next_of_kin_address,
     Father_First_name, Father_Last_name, father_tel, father_status, father_village,
     father_parish, father_subcounty, father_district, mother_First_name, mother_Last_name,
     mother_tel, mother_status, mother_village, mother_parish, mother_subcounty,
     mother_district, comments)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `, [
    req.body.First_name, req.body.Middle_Name, req.body.Last_Name, req.body.tel_no,
    req.body.sex, req.body.marital_status, req.body.date_of_birth, req.body.occupation,
    req.body.place_of_birth, req.body.village_lc1, req.body.parish, req.body.sub_county,
    req.body.status, req.body.national_id, req.body.date_joined,
    req.body.next_of_kin_First_name, req.body.next_of_kin_Last_name, req.body.next_of_kin_tel,
    req.body.next_of_kin_address, req.body.Father_First_name, req.body.Father_Last_name,
    req.body.father_tel, req.body.father_status, req.body.father_village, req.body.father_parish,
    req.body.father_subcounty, req.body.father_district, req.body.mother_First_name,
    req.body.mother_Last_name, req.body.mother_tel, req.body.mother_status,
    req.body.mother_village, req.body.mother_parish, req.body.mother_subcounty,
    req.body.mother_district, req.body.comments
  ]);

  res.json({
    success: true,
    memberadd: {
      First_name: req.body.First_name,
      Last_Name: req.body.Last_Name,
      tel_no: req.body.tel_no,
      sex: req.body.sex,
      date_of_birth: req.body.date_of_birth,
      occupation: req.body.occupation,
      status: req.body.status
    }
  });
}));

// ==================== DEPENDANTS ROUTES ====================
         // Add this endpoint to your app.js

// GET: Validate member by ID (for dependant registration)
app.get('/api/member/:member_id', asyncHandler(async (req, res) => {
  const { member_id } = req.params;
  const db = dbConfig;

  try {
    const [member] = await db.execute(
      `SELECT id, First_name, Last_Name, Middle_Name, status 
       FROM members_mst 
       WHERE status ="Active" and id = ?`,
      [member_id]
    );

    if (member.length === 0) {
      return res.json({
        success: false,
        message: 'Member not found'
      });
    }

    const memberData = member[0];
    const fullName = `${memberData.First_name} ${memberData.Middle_Name || ''} ${memberData.Last_Name}`.trim();

    res.json({
      success: true,
      full_name: fullName,
      member: {
        id: memberData.id,
        first_name: memberData.First_name,
        middle_name: memberData.Middle_Name,
        last_name: memberData.Last_Name,
        status: memberData.status
      }
    });

  } catch (err) {
    console.error('Member validation error:', err);
    res.status(500).json({
      success: false,
      message: 'Server error'
    });
  }
}));

// GET: Validate member by ID (for dependant registration)
app.get('/api/internal/:member_id', asyncHandler(async (req, res) => {
  const { member_id } = req.params;
  const db = dbConfig;

  try {
    const [member] = await db.execute(
      `SELECT id, First_name, Last_Name, Middle_Name, status 
       FROM members_mst 
       WHERE status ="Internal" and id = ?`,
      [member_id]
    );

    if (member.length === 0) {
      return res.json({
        success: false,
        message: 'Member not found'
      });
    }

    const memberData = member[0];
    const fullName = `${memberData.First_name} ${memberData.Middle_Name || ''} ${memberData.Last_Name}`.trim();

    res.json({
      success: true,
      full_name: fullName,
      member: {
        id: memberData.id,
        first_name: memberData.First_name,
        middle_name: memberData.Middle_Name,
        last_name: memberData.Last_Name,
        status: memberData.status
      }
    });

  } catch (err) {
    console.error('Member validation error:', err);
    res.status(500).json({
      success: false,
      message: 'Server error'
    });
  }
}));

// POST: Add dependant
app.post('/dependantadd', checkAuth, asyncHandler(async (req, res) => {
  const db = dbConfig;
  const { person_id, First_name, Middle_Name, Last_Name, Gender, date_of_birth, status } = req.body;
  const errors = [];

  // Validate person_id exists
  const [member] = await db.execute(
    'SELECT id, status FROM members_mst WHERE id = ?',
    [person_id]
  );

  if (member.length === 0) {
    errors.push("Member ID not found");
  } else if (member[0].status !== 'Active') {
    errors.push("Member must be active to add dependants");
  }

  // Required field validations
  if (!person_id) errors.push("Member ID is required");
  if (!First_name) errors.push("First name is required");
  if (!Last_Name) errors.push("Last name is required");
  if (!Gender) errors.push("Gender is required");
  if (!date_of_birth) errors.push("Date of birth is required");
  if (!status) errors.push("Status is required");

  if (errors.length > 0) {
    return res.status(400).json({ errors });
  }

  // Insert dependant
  await db.execute(`
    INSERT INTO dependants 
    (person_id, First_name, Middle_Name, Last_Name, Gender, child_date_of_birth, Status)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `, [person_id, First_name, Middle_Name, Last_Name, Gender, date_of_birth, status]);

  res.json({
    success: true,
    dependantadd: {
      person_id,
      First_name,
      Middle_Name,
      Last_Name,
      Gender,
      date_of_birth,
      status
    }
  });
}));


app.get('/dependants', checkAuth, asyncHandler(async (req, res) => {
  const db = dbConfig;
  const [rows] = await db.execute(`
    SELECT 
      id, person_id, First_Name, Last_Name, Gender,
      DATE_FORMAT(child_date_of_birth, '%d-%b-%Y') AS child_date_of_birth             
    FROM dependants
    ORDER BY id DESC
  `);

  res.render('dependants', { users: rows, currentPage: 'dependants', user: req.session.user });
}));

app.get('/dependants/:id/edit', checkAuth, asyncHandler(async (req, res) => {
  const { id } = req.params;
  const db = dbConfig;

  const [rows] = await db.execute(`
    SELECT 
      id, person_id, First_Name, Middle_Name, Last_Name, Gender,
      DATE_FORMAT(child_date_of_birth, '%Y-%m-%d') AS child_date_of_birth, Status            
    FROM dependants 
    WHERE id = ?
  `, [id]);

  if (rows.length === 0) {
    return res.status(404).send("Dependant not found");
  }

  res.render('dependant_edit', {
    currentPage: 'home',
    dependant: rows[0],
    user: req.session.user
  });
}));

// POST: Update dependant
app.post('/dependants/:id/edit', checkAuth, asyncHandler(async (req, res) => {
  const { id } = req.params;
  const db = dbConfig;
  const dependantData = {
    First_Name: req.body.First_Name || null,
    Middle_Name: req.body.Middle_Name || null,
    Last_Name: req.body.Last_Name || null,
    Gender: req.body.sex || null,
    child_date_of_birth: req.body.child_date_of_birth || null,
    person_id: req.body.person_id || null,
    Status: req.body.status || null
  };

  console.log('Updating dependant:', id);
  console.log('Data:', dependantData);

  try {
    // Validate person_id exists and is active
    if (dependantData.person_id) {
      const [member] = await db.execute(
        'SELECT id, status FROM members_mst WHERE id = ?',
        [dependantData.person_id]
      );

      if (member.length === 0) {
        return res.status(400).json({
          success: false,
          error: 'Member ID not found'
        });
      }
    }

    // Update dependant
    await db.execute(`
      UPDATE dependants 
      SET First_Name = ?, 
          Middle_Name = ?, 
          Last_Name = ?, 
          Gender = ?, 
          child_date_of_birth = ?, 
          person_id = ?, 
          Status = ?
      WHERE id = ?
    `, [
      dependantData.First_Name,
      dependantData.Middle_Name,
      dependantData.Last_Name,
      dependantData.Gender,
      dependantData.child_date_of_birth,
      dependantData.person_id,
      dependantData.Status,
      id
    ]);

    console.log('Dependant updated successfully');

    // Return JSON response
    res.json({
      success: true,
      message: 'Dependant profile updated successfully'
    });

  } catch (err) {
    console.error('Error updating dependant:', err);
    res.status(500).json({
      success: false,
      error: 'Failed to update dependant profile'
    });
  }
}));

// ==================== PAYMENT STATUS ROUTES ====================

// Helper function for payment status queries
async function getPaymentStatus(req, res, transactionType) {
  const { payment_period, memberStatus, paymentStatus, start, length, draw } = req.body;
  const db = dbConfig;

  let query = `
    SELECT SQL_CALC_FOUND_ROWS * FROM (
      SELECT
        mm.id AS member_id,
        mm.Date_Joined,
        mm.First_name,
        mm.Last_Name,
        mm.tel_no,
        mm.Status,
        COALESCE(SUM(t.Amount), 0) AS Total_Amount,
        MAX(t.created_at) AS last_payment_date,
        MAX(t.payment_period) AS last_payment_period,
        CASE 
          WHEN COALESCE(SUM(t.Amount), 0) = 0 THEN 'Not Paid' 
          ELSE 'Paid' 
        END AS Payment_Status
      FROM members_mst mm
      LEFT OUTER JOIN transactions t 
        ON mm.id = t.member_id 
        AND t.transaction_type = ?
  `;

  const params = [transactionType];

  if (payment_period) {
    query += ` AND t.payment_period = ?`;
    params.push(payment_period);
  }

  query += ` WHERE mm.status = ?`;
  params.push(memberStatus || 'ACTIVE');

  query += `
      GROUP BY mm.id, mm.Date_Joined, mm.First_name, mm.Last_Name, mm.tel_no, mm.Status
    ) AS member_payments
  `;

  if (paymentStatus) {
    query += ` WHERE Payment_Status = ?`;
    params.push(paymentStatus);
  }

  query += ` ORDER BY member_id`;

  const startNum = parseInt(start) || 0;
  const lengthNum = parseInt(length) || 10;
  query += ` LIMIT ${startNum}, ${lengthNum}`;

  const [rows] = await db.execute(query, params);

  rows.forEach(r => {
    Object.keys(r).forEach(k => {
      if (r[k] === null) r[k] = '';
    });
  });

  const [[{ total }]] = await db.query('SELECT FOUND_ROWS() AS total');

  res.json({
    draw: Number(draw) || 0,
    recordsTotal: total,
    recordsFiltered: total,
    data: rows
  });
}

app.post('/membership_fee/payment-status', asyncHandler(async (req, res) => {
  await getPaymentStatus(req, res, 'Membership Fee');
}));

app.post('/welfare/payment-status', asyncHandler(async (req, res) => {
  await getPaymentStatus(req, res, 'welfare Fee');
}));

app.post('/Insurance_cover/payment-status', asyncHandler(async (req, res) => {
  await getPaymentStatus(req, res, 'Insuarance Cover');
}));

// ==================== SAVINGS REPORT ====================

app.post('/savings_report/savings-summary', asyncHandler(async (req, res) => {
  const { memberId, memberName, phoneNumber, start, length, draw } = req.body;
  const db = dbConfig;

  let query = `
    SELECT SQL_CALC_FOUND_ROWS
      ROW_NUMBER() OVER (ORDER BY mm.id) AS SN,
      mm.id,
      CONCAT(mm.First_name, ' ', mm.Last_Name) AS Name,
      mm.tel_no AS Tel_No,
      COALESCE(SUM(CASE WHEN YEAR(t.tran_date) < YEAR(CURDATE()) AND t.transaction_type = 'Saving' THEN t.Amount ELSE 0 END), 0) AS BF,
      COALESCE(SUM(CASE WHEN YEAR(t.tran_date) = YEAR(CURDATE()) AND MONTH(t.tran_date) = 1 AND t.transaction_type = 'Saving' THEN t.Amount ELSE 0 END), 0) AS Jan,
      COALESCE(SUM(CASE WHEN YEAR(t.tran_date) = YEAR(CURDATE()) AND MONTH(t.tran_date) = 2 AND t.transaction_type = 'Saving' THEN t.Amount ELSE 0 END), 0) AS Feb,
      COALESCE(SUM(CASE WHEN YEAR(t.tran_date) = YEAR(CURDATE()) AND MONTH(t.tran_date) = 3 AND t.transaction_type = 'Saving' THEN t.Amount ELSE 0 END), 0) AS Mar,
      COALESCE(SUM(CASE WHEN YEAR(t.tran_date) = YEAR(CURDATE()) AND MONTH(t.tran_date) = 4 AND t.transaction_type = 'Saving' THEN t.Amount ELSE 0 END), 0) AS April,
      COALESCE(SUM(CASE WHEN YEAR(t.tran_date) = YEAR(CURDATE()) AND MONTH(t.tran_date) = 5 AND t.transaction_type = 'Saving' THEN t.Amount ELSE 0 END), 0) AS May,
      COALESCE(SUM(CASE WHEN YEAR(t.tran_date) = YEAR(CURDATE()) AND MONTH(t.tran_date) = 6 AND t.transaction_type = 'Saving' THEN t.Amount ELSE 0 END), 0) AS June,
      COALESCE(SUM(CASE WHEN YEAR(t.tran_date) = YEAR(CURDATE()) AND MONTH(t.tran_date) = 7 AND t.transaction_type = 'Saving' THEN t.Amount ELSE 0 END), 0) AS July,
      COALESCE(SUM(CASE WHEN YEAR(t.tran_date) = YEAR(CURDATE()) AND MONTH(t.tran_date) = 8 AND t.transaction_type = 'Saving' THEN t.Amount ELSE 0 END), 0) AS Aug,
      COALESCE(SUM(CASE WHEN YEAR(t.tran_date) = YEAR(CURDATE()) AND MONTH(t.tran_date) = 9 AND t.transaction_type = 'Saving' THEN t.Amount ELSE 0 END), 0) AS Sept,
      COALESCE(SUM(CASE WHEN YEAR(t.tran_date) = YEAR(CURDATE()) AND MONTH(t.tran_date) = 10 AND t.transaction_type = 'Saving' THEN t.Amount ELSE 0 END), 0) AS Oct,
      COALESCE(SUM(CASE WHEN YEAR(t.tran_date) = YEAR(CURDATE()) AND MONTH(t.tran_date) = 11 AND t.transaction_type = 'Saving' THEN t.Amount ELSE 0 END), 0) AS Nov,
      COALESCE(SUM(CASE WHEN YEAR(t.tran_date) = YEAR(CURDATE()) AND MONTH(t.tran_date) = 12 AND t.transaction_type = 'Saving' THEN t.Amount ELSE 0 END), 0) AS 'Dec',
      COALESCE(SUM(CASE WHEN YEAR(t.tran_date) = YEAR(CURDATE()) AND t.transaction_type = 'Saving' THEN t.Amount ELSE 0 END), 0) AS Total_Deposits_YTD,
      COALESCE(SUM(CASE WHEN YEAR(t.tran_date) = YEAR(CURDATE()) AND t.transaction_type = 'Savings Withdrawal' THEN t.Amount ELSE 0 END), 0) AS Total_Withdrawals_YTD,
      COALESCE(SUM(CASE WHEN YEAR(t.tran_date) = YEAR(CURDATE()) AND t.transaction_type = 'Saving' THEN t.Amount ELSE 0 END), 0) - 
      COALESCE(SUM(CASE WHEN YEAR(t.tran_date) = YEAR(CURDATE()) AND t.transaction_type = 'Savings Withdrawal' THEN t.Amount ELSE 0 END), 0) AS Net_Savings_YTD,
      COALESCE(SUM(CASE WHEN t.transaction_type = 'Saving' THEN t.Amount ELSE 0 END), 0) - 
      COALESCE(SUM(CASE WHEN t.transaction_type = 'Savings Withdrawal' THEN t.Amount ELSE 0 END), 0) AS Net_Total
    FROM members_mst mm
    LEFT JOIN transactions t ON mm.id = t.member_id
    WHERE mm.status = 'ACTIVE'
  `;

  const params = [];

  if (memberId) {
    query += ` AND mm.id = ?`;
    params.push(memberId);
  }

  if (memberName) {
    query += ` AND (mm.First_name LIKE ? OR mm.Last_Name LIKE ? OR CONCAT(mm.First_name, ' ', mm.Last_Name) LIKE ?)`;
    const namePattern = `%${memberName}%`;
    params.push(namePattern, namePattern, namePattern);
  }

  if (phoneNumber) {
    query += ` AND mm.tel_no LIKE ?`;
    params.push(`%${phoneNumber}%`);
  }

  query += ` GROUP BY mm.id, mm.First_name, mm.Last_Name, mm.tel_no ORDER BY mm.id`;

  const startNum = parseInt(start) || 0;
  const lengthNum = parseInt(length) || 10;
  query += ` LIMIT ${startNum}, ${lengthNum}`;

  const [rows] = await db.execute(query, params);

  rows.forEach(r => {
    Object.keys(r).forEach(k => {
      if (r[k] === null) r[k] = '';
    });
  });

  const [[{ total }]] = await db.query('SELECT FOUND_ROWS() AS total');

  res.json({
    draw: Number(draw) || 0,
    recordsTotal: total,
    recordsFiltered: total,
    data: rows
  });
}));

// ==================== LOAN HELPER FUNCTIONS ====================

function calculateReducingBalance(principal, annualRate, months) {
  const monthlyRate = (annualRate / 100) / 12;

  const monthlyPayment = principal *
    (monthlyRate * Math.pow(1 + monthlyRate, months)) /
    (Math.pow(1 + monthlyRate, months) - 1);

  const schedule = [];
  let balance = principal;
  let totalInterest = 0;

  for (let i = 1; i <= months; i++) {
    const interestPayment = balance * monthlyRate;
    const principalPayment = monthlyPayment - interestPayment;
    balance -= principalPayment;
    totalInterest += interestPayment;

    schedule.push({
      paymentNumber: i,
      principalAmount: principalPayment,
      interestAmount: interestPayment,
      totalPayment: monthlyPayment,
      balanceAfterPayment: balance
    });
  }

  return {
    monthlyPayment: monthlyPayment,
    totalInterest: totalInterest,
    totalRepayment: principal + totalInterest,
    schedule: schedule
  };
}

async function calculateDailyInterest() {
  try {
    const db = dbConfig;
    const today = new Date().toISOString().split('T')[0];

    const [activeLoans] = await db.execute(`
      SELECT id, loan_amount, balance, interest_rate, accumulated_interest, disbursement_date
      FROM loans 
      WHERE status = 'Active' AND balance > 0 AND interest_method = 'reducing_balance'
    `);

    console.log(`[CRON] Processing ${activeLoans.length} active loans for ${today}...`);
    
    let totalInterest = 0;
    let loansProcessed = 0;

    for (const loan of activeLoans) {
      const dailyInterestRate = (loan.interest_rate / 100) / 365;
      const dailyInterest = loan.balance * dailyInterestRate;

      const [existing] = await db.execute(
        'SELECT id FROM loan_daily_interest WHERE loan_id = ? AND calculation_date = ?',
        [loan.id, today]
      );

      if (existing.length === 0) {
        await db.execute(`
          INSERT INTO loan_daily_interest 
          (loan_id, calculation_date, outstanding_balance, daily_interest_amount, annual_interest_rate)
          VALUES (?, ?, ?, ?, ?)
        `, [loan.id, today, loan.balance, dailyInterest, loan.interest_rate]);

        await db.execute(`
          UPDATE loans 
          SET accumulated_interest = accumulated_interest + ?,
              total_repayment = loan_amount + total_interest + accumulated_interest + ?
          WHERE id = ?
        `, [dailyInterest, dailyInterest, loan.id]);

        totalInterest += dailyInterest;
        loansProcessed++;
      }
    }

    console.log(`[CRON] ✓ Interest calculated: ${loansProcessed} loans, UGX ${totalInterest.toFixed(2)}`);
  } catch (err) {
    console.error('[CRON] ❌ Error calculating daily interest:', err);
  }
}

// ==================== LOAN ROUTES ====================

// Get member by ID
app.get('/members_id/:member_id', asyncHandler(async (req, res) => {
  const { member_id } = req.params;
  const db = dbConfig;

  const [member] = await db.execute(`
    SELECT id, First_name, Last_Name, tel_no, status, Date_Joined 
    FROM members_mst 
    WHERE status = 'Active' AND id = ?
  `, [member_id]);

  if (member.length === 0) {
    return res.status(404).json({ error: 'Member not found' });
  }

  res.json(member[0]);
}));

// Create loan
// Create loan - WITH SIMPLE INTEREST OPTION + PROCESSING FEE
app.post('/loans/create', asyncHandler(async (req, res) => {
  const { 
    member_id, 
    loan_amount, 
    interest_rate, 
    loan_term_months, 
    disbursement_date,
    interest_method = 'reducing_balance',
    processing_fee_pct    = 0,
    processing_fee_amount: fee_from_client
  } = req.body;
  
  const db = dbConfig;

  // Sanitize numbers
  const principal    = parseFloat(loan_amount);
  const rate         = parseFloat(interest_rate);
  const months       = parseInt(loan_term_months);
  const feePct       = parseFloat(processing_fee_pct) || 0;
  const feeAmount    = parseFloat(fee_from_client) || (principal * feePct / 100);
  const netDisburse  = principal - feeAmount;

  // Validate member
  const [member] = await db.execute(
    'SELECT id, First_name, Last_Name, status FROM members_mst WHERE id = ?',
    [member_id]
  );

  if (member.length === 0) {
    return res.status(400).json({ error: 'Member not found' });
  }

  if (member[0].status !== 'Active') {
    return res.status(400).json({ error: 'Member is not active' });
  }

  let monthlyPayment, totalInterest, totalRepayment;
  let schedule = [];

  // ── Calculate based on interest method ─────────────────────────
  if (interest_method === 'simple_interest') {
    totalInterest  = (principal * (rate / 100) * months) / 12;
    totalRepayment = principal + totalInterest;
    monthlyPayment = totalRepayment / months;

    console.log('Simple Interest | Principal:', principal, '| Interest:', totalInterest,
                '| Repayment:', totalRepayment, '| Monthly:', monthlyPayment);

    const scheduleDate = new Date(disbursement_date);
    let remainingAmount = totalRepayment;

    for (let i = 1; i <= months; i++) {
      scheduleDate.setMonth(scheduleDate.getMonth() + 1);
      const principalPortion = principal / months;
      const interestPortion  = totalInterest / months;
      remainingAmount -= monthlyPayment;

      schedule.push({
        payment_number        : i,
        due_date              : scheduleDate.toISOString().split('T')[0],
        expected_payment      : monthlyPayment,
        expected_principal    : principalPortion,
        expected_interest     : interestPortion,
        balance_after_payment : remainingAmount < 0 ? 0 : remainingAmount
      });
    }

  } else {
    // Reducing balance
    const monthlyRate = (rate / 100) / 12;
    monthlyPayment  = principal *
      (monthlyRate * Math.pow(1 + monthlyRate, months)) /
      (Math.pow(1 + monthlyRate, months) - 1);
    totalRepayment  = monthlyPayment * months;
    totalInterest   = totalRepayment - principal;

    console.log('Reducing Balance | Principal:', principal, '| Interest:', totalInterest,
                '| Repayment:', totalRepayment, '| Monthly:', monthlyPayment);

    const scheduleDate   = new Date(disbursement_date);
    let remainingBalance = principal;

    for (let i = 1; i <= months; i++) {
      scheduleDate.setMonth(scheduleDate.getMonth() + 1);
      const expectedInterest  = remainingBalance * monthlyRate;
      const expectedPrincipal = monthlyPayment - expectedInterest;
      remainingBalance       -= expectedPrincipal;

      schedule.push({
        payment_number        : i,
        due_date              : scheduleDate.toISOString().split('T')[0],
        expected_payment      : monthlyPayment,
        expected_principal    : expectedPrincipal,
        expected_interest     : expectedInterest,
        balance_after_payment : remainingBalance < 0 ? 0 : remainingBalance
      });
    }
  }

  const maturity_date = new Date(disbursement_date);
  maturity_date.setMonth(maturity_date.getMonth() + months);

  await db.query('START TRANSACTION');

  try {
    // ── 1. Insert loan record ─────────────────────────────────────
    const [loanResult] = await db.execute(`
      INSERT INTO loans (
        member_id, loan_amount, interest_rate, loan_term_months,
        disbursement_date, maturity_date, status, monthly_payment,
        total_interest, total_repayment, balance, accumulated_interest,
        interest_method, total_expected_interest,
        processing_fee_pct, processing_fee_amount, net_disbursement
      ) VALUES (?, ?, ?, ?, ?, ?, 'Active', ?, 0, ?, ?, 0, ?, ?, ?, ?, ?)
    `, [
      member_id,
      principal,
      rate,
      months,
      disbursement_date,
      maturity_date.toISOString().split('T')[0],
      monthlyPayment,
      totalRepayment,
      principal,          // balance = full principal
      interest_method,
      totalInterest,
      feePct,
      feeAmount,
      netDisburse
    ]);

    const loan_id = loanResult.insertId;
    console.log('Loan created ID:', loan_id, '| Fee:', feeAmount, '| Net disburse:', netDisburse);

    // ── 2. Insert payment schedule ────────────────────────────────
    for (const item of schedule) {
      await db.execute(`
        INSERT INTO loan_schedule (
          loan_id, payment_number, due_date,
          expected_payment, expected_principal, expected_interest,
          accumulated_interest, balance_after_payment,
          status, paid_amount
        ) VALUES (?, ?, ?, ?, ?, ?, 0, ?, 'Pending', 0)
      `, [
        loan_id,
        item.payment_number,
        item.due_date,
        item.expected_payment,
        item.expected_principal,
        item.expected_interest,
        item.balance_after_payment
      ]);
    }

    // ── 3. Record loan disbursement transaction (net amount) ──────
    await db.execute(`
      INSERT INTO transactions (
        member_id, tran_date, Amount, transaction_type,
        description, debit_credit, payment_period
      ) VALUES (?, ?, ?, 'Loan Disbursement', ?, 'Credit', ?)
    `, [
      member_id,
      disbursement_date,
      netDisburse,          // member receives net amount (after fee)
      `Loan disbursement - Loan ID: ${loan_id} (${interest_method})`,
      new Date(disbursement_date).getFullYear()
    ]);

    // ── 4. Record processing fee as income transaction ────────────
    if (feeAmount > 0) {
      await db.execute(`
        INSERT INTO transactions (
          member_id, tran_date, Amount, transaction_type,
          description, debit_credit, payment_period
        ) VALUES (?, ?, ?, 'Loan Processing Fee', ?, 'Debit', ?)
      `, [
        member_id,
        disbursement_date,
        feeAmount,
        `Loan processing fee ${feePct}% on Loan ID: ${loan_id} | Principal: UGX ${principal.toLocaleString()}`,
        new Date(disbursement_date).getFullYear()
      ]);
    }

    await db.query('COMMIT');

    console.log('Loan creation successful');

    res.json({
      success     : true,
      loan_id     : loan_id,
      member_name : member[0].First_name + ' ' + member[0].Last_Name,
      message     : 'Loan created and disbursed successfully',
      details     : {
        loan_amount           : principal,
        monthly_payment       : parseFloat(monthlyPayment),
        total_interest        : parseFloat(totalInterest),
        total_repayment       : parseFloat(totalRepayment),
        interest_rate         : rate,
        interest_method       : interest_method,
        processing_fee_pct    : feePct,
        processing_fee_amount : feeAmount,
        net_disbursement      : netDisburse,
        note: interest_method === 'simple_interest'
          ? 'Simple interest — fixed interest calculated upfront'
          : 'Reducing balance — interest calculated daily on outstanding balance',
        term_months: months
      }
    });

  } catch (err) {
    await db.query('ROLLBACK');
    console.error('Loan creation error:', err);
    throw err;
  }
}));

// Record loan repayment
// Record loan repayment - HANDLE BOTH SIMPLE AND REDUCING BALANCE
app.post('/loans/repayment', asyncHandler(async (req, res) => {
  const { loan_id, payment_amount, payment_date, payment_method, reference_number, notes } = req.body;
  const db = dbConfig;

  const [loan] = await db.execute(
    'SELECT * FROM loans WHERE id = ? AND status IN ("Active", "Pending")',
    [loan_id]
  );

  if (loan.length === 0) {
    return res.status(400).json({ error: 'Loan not found or not active' });
  }

  const loanData = loan[0];
  const interestMethod = loanData.interest_method || 'reducing_balance';

  if (payment_amount <= 0) {
    return res.status(400).json({ error: 'Payment amount must be greater than zero' });
  }

  // Calculate total owed based on interest method
  let totalOwed;
  if (interestMethod === 'simple_interest') {
    // For simple interest, total owed = total_repayment - already paid
    const [paidSummary] = await db.execute(
      'SELECT COALESCE(SUM(amount), 0) AS total_paid FROM loan_payment_history WHERE loan_id = ?',
      [loan_id]
    );
    totalOwed = parseFloat(loanData.total_repayment) - parseFloat(paidSummary[0].total_paid);
  } else {
    // For reducing balance, total owed = balance + accumulated interest
    totalOwed = parseFloat(loanData.balance) + parseFloat(loanData.accumulated_interest);
  }

  if (payment_amount > totalOwed) {
    return res.status(400).json({
      error: `Payment amount (${payment_amount}) exceeds total owed (${totalOwed.toFixed(2)})`
    });
  }

  await db.query('START TRANSACTION');

  try {
    let remainingPayment = parseFloat(payment_amount);
    let interestPaid = 0;
    let principalPaid = 0;
    const paymentsUpdated = [];

    if (interestMethod === 'simple_interest') {
      // SIMPLE INTEREST REPAYMENT LOGIC
      // Get pending installments in order
      const [pendingPayments] = await db.execute(`
        SELECT * FROM loan_schedule 
        WHERE loan_id = ? AND status IN ('Pending', 'Overdue') 
        ORDER BY payment_number ASC
      `, [loan_id]);

      for (const payment of pendingPayments) {
        if (remainingPayment <= 0) break;

        const expectedPayment = parseFloat(payment.expected_payment);
        const expectedPrincipal = parseFloat(payment.expected_principal);
        const expectedInterest = parseFloat(payment.expected_interest);
        const alreadyPaid = parseFloat(payment.paid_amount || 0);
        const stillOwed = expectedPayment - alreadyPaid;

        if (stillOwed <= 0) continue;

        const amountToApply = Math.min(remainingPayment, stillOwed);
        const newPaidAmount = alreadyPaid + amountToApply;
        const isFullyPaid = newPaidAmount >= expectedPayment;

        // Calculate interest and principal portions
        if (alreadyPaid === 0) {
          // First payment on this installment
          const paymentRatio = amountToApply / expectedPayment;
          const interestPortion = expectedInterest * paymentRatio;
          const principalPortion = amountToApply - interestPortion;
          
          interestPaid += interestPortion;
          principalPaid += principalPortion;
        } else {
          // Partial payment - split proportionally based on remaining amounts
          const remainingInterest = expectedInterest * (stillOwed / expectedPayment);
          const paymentRatio = amountToApply / stillOwed;
          const interestPortion = remainingInterest * paymentRatio;
          const principalPortion = amountToApply - interestPortion;
          
          interestPaid += interestPortion;
          principalPaid += principalPortion;
        }

        // Update schedule
        await db.execute(`
          UPDATE loan_schedule 
          SET paid_amount = ?, 
              status = ?, 
              paid_date = CASE WHEN ? = 1 THEN ? ELSE paid_date END
          WHERE id = ?
        `, [
          newPaidAmount,
          isFullyPaid ? 'Paid' : payment.status,
          isFullyPaid ? 1 : 0,
          isFullyPaid ? payment_date : null,
          payment.id
        ]);

        paymentsUpdated.push({
          payment_number: payment.payment_number,
          amount_applied: amountToApply,
          fully_paid: isFullyPaid
        });

        remainingPayment -= amountToApply;
      }

      // Update loan balance (principal only)
      const newBalance = Math.max(0, parseFloat(loanData.balance) - principalPaid);
      
      await db.execute(`
        UPDATE loans 
        SET balance = ?, 
            total_interest = total_interest + ?
        WHERE id = ?
      `, [newBalance, interestPaid, loan_id]);

      // Check if loan is fully paid
      if (newBalance <= 0.01) {
        await db.execute('UPDATE loans SET status = "Completed" WHERE id = ?', [loan_id]);
      }

    } else {
      // REDUCING BALANCE REPAYMENT LOGIC (existing code)
      const accumulatedInterest = parseFloat(loanData.accumulated_interest);

      // First, pay accumulated interest
      if (accumulatedInterest > 0) {
        interestPaid = Math.min(remainingPayment, accumulatedInterest);
        remainingPayment -= interestPaid;

        await db.execute(
          'UPDATE loans SET accumulated_interest = accumulated_interest - ? WHERE id = ?',
          [interestPaid, loan_id]
        );
      }

      // Then, pay principal
      let newBalance = parseFloat(loanData.balance);
      if (remainingPayment > 0) {
        principalPaid = Math.min(remainingPayment, loanData.balance);
        newBalance = Math.max(0, loanData.balance - principalPaid);

        await db.execute(`
          UPDATE loans 
          SET balance = ?, total_interest = total_interest + ?
          WHERE id = ?
        `, [newBalance, interestPaid, loan_id]);

        // Check if loan is fully paid
        if (newBalance <= 0.01) {
          const [remainingInterest] = await db.execute(
            'SELECT accumulated_interest FROM loans WHERE id = ?',
            [loan_id]
          );

          if (parseFloat(remainingInterest[0].accumulated_interest) <= 0.01) {
            await db.execute('UPDATE loans SET status = "Completed" WHERE id = ?', [loan_id]);
          }
        }
      }

      // Update loan schedule
      const [pendingPayments] = await db.execute(`
        SELECT * FROM loan_schedule 
        WHERE loan_id = ? AND status IN ('Pending', 'Overdue') 
        ORDER BY payment_number ASC
      `, [loan_id]);

      let principalToDistribute = principalPaid;

      for (const payment of pendingPayments) {
        if (principalToDistribute <= 0) break;

        const expectedPayment = parseFloat(payment.expected_payment);
        const alreadyPaid = parseFloat(payment.paid_amount || 0);
        const stillOwed = expectedPayment - alreadyPaid;

        if (stillOwed <= 0) continue;

        const amountToApply = Math.min(principalToDistribute, stillOwed);
        const newPaidAmount = alreadyPaid + amountToApply;
        const isFullyPaid = newPaidAmount >= expectedPayment;

        await db.execute(`
          UPDATE loan_schedule 
          SET paid_amount = ?, 
              status = ?, 
              paid_date = CASE WHEN ? = 1 THEN ? ELSE paid_date END
          WHERE id = ?
        `, [
          newPaidAmount,
          isFullyPaid ? 'Paid' : payment.status,
          isFullyPaid ? 1 : 0,
          isFullyPaid ? payment_date : null,
          payment.id
        ]);

        paymentsUpdated.push({
          payment_number: payment.payment_number,
          amount_applied: amountToApply,
          fully_paid: isFullyPaid
        });

        principalToDistribute -= amountToApply;
      }
    }

    // Record in transactions table
    await db.execute(`
      INSERT INTO transactions (
        member_id, tran_date, Amount, transaction_type,
        description, debit_credit, payment_period
      ) VALUES (?, ?, ?, 'Loan Repayment', ?, 'Debit', ?)
    `, [
      loanData.member_id, payment_date, payment_amount,
      `Loan repayment - Loan ID: ${loan_id}. Interest: ${interestPaid.toFixed(2)}, Principal: ${principalPaid.toFixed(2)}. ${notes || ''}`,
      new Date(payment_date).getFullYear()
    ]);

    // Record payment history
    await db.execute(`
      INSERT INTO loan_payment_history (
        loan_id, payment_date, amount, payment_method, 
        reference_number, notes, principal_paid, interest_paid, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW())
    `, [loan_id, payment_date, payment_amount, payment_method, reference_number, notes, principalPaid, interestPaid]);

    // Record balance history
    const [currentLoan] = await db.execute('SELECT balance FROM loans WHERE id = ?', [loan_id]);
    await db.execute(`
      INSERT INTO loan_balance_history (loan_id, balance_date, balance)
      VALUES (?, ?, ?)
      ON DUPLICATE KEY UPDATE balance = VALUES(balance)
    `, [loan_id, payment_date, currentLoan[0].balance]);

    // Get updated loan data
    const [updatedLoan] = await db.execute(
      'SELECT balance, accumulated_interest, status, interest_method FROM loans WHERE id = ?',
      [loan_id]
    );

    await db.query('COMMIT');

    // Calculate remaining amount based on method
    let totalRemaining;
    if (interestMethod === 'simple_interest') {
      const [paidSummary] = await db.execute(
        'SELECT COALESCE(SUM(amount), 0) AS total_paid FROM loan_payment_history WHERE loan_id = ?',
        [loan_id]
      );
      totalRemaining = parseFloat(loanData.total_repayment) - parseFloat(paidSummary[0].total_paid) - parseFloat(payment_amount);
    } else {
      totalRemaining = parseFloat(updatedLoan[0].balance) + parseFloat(updatedLoan[0].accumulated_interest);
    }

    // Send response with correct structure
    res.json({
      success: true,
      message: 'Payment recorded successfully',
      details: {
        amount_paid: parseFloat(payment_amount),
        interest_paid: parseFloat(interestPaid),
        principal_paid: parseFloat(principalPaid),
        remaining_balance: parseFloat(updatedLoan[0].balance),
        accumulated_interest: parseFloat(updatedLoan[0].accumulated_interest || 0),
        total_remaining: totalRemaining,
        status: updatedLoan[0].status,
        interest_method: updatedLoan[0].interest_method,
        payments_updated: paymentsUpdated
      }
    });
  } catch (err) {
    await db.query('ROLLBACK');
    throw err;
  }
}));

// Get loan payment history
app.get('/loans/:loan_id/payment-history', asyncHandler(async (req, res) => {
  const { loan_id } = req.params;
  const db = dbConfig;

  const [history] = await db.execute(`
    SELECT * FROM loan_payment_history 
    WHERE loan_id = ? 
    ORDER BY payment_date DESC, created_at DESC
  `, [loan_id]);

  res.json({ history: history });
}));

// Get loan statement
app.get('/loans/:loan_id/statement', asyncHandler(async (req, res) => {
  const { loan_id } = req.params;
  const db = dbConfig;

  const [loan] = await db.execute(`
    SELECT l.*, CONCAT(m.First_name, ' ', m.Last_Name) AS member_name
    FROM loans l
    JOIN members_mst m ON l.member_id = m.id
    WHERE l.id = ?
  `, [loan_id]);

  if (loan.length === 0) {
    return res.status(404).json({ error: 'Loan not found' });
  }

  const [schedule] = await db.execute(`
    SELECT * FROM loan_schedule 
    WHERE loan_id = ? 
    ORDER BY payment_number ASC
  `, [loan_id]);

  res.json({
    loan: loan[0],
    schedule: schedule
  });

}));

// Get member loans
app.get('/members/:member_id/loans', asyncHandler(async (req, res) => {
  const { member_id } = req.params;
  const db = dbConfig;

  const [loans] = await db.execute(`
    SELECT 
      l.*,
      (SELECT COUNT(*) FROM loan_schedule WHERE loan_id = l.id AND status = 'Paid') AS payments_made,
      (SELECT COUNT(*) FROM loan_schedule WHERE loan_id = l.id AND status = 'Pending') AS payments_remaining,
      (SELECT COUNT(*) FROM loan_schedule WHERE loan_id = l.id AND status = 'Overdue') AS payments_overdue
    FROM loans l
    WHERE l.member_id = ?
    ORDER BY l.disbursement_date DESC
  `, [member_id]);

  res.json({ loans: loans });
}));

// Loans list
app.post('/loans/list', asyncHandler(async (req, res) => {
  const { memberId, memberName, loanStatus, start, length, draw } = req.body;
  const db = dbConfig;

  let query = `
    SELECT SQL_CALC_FOUND_ROWS
      l.id AS loan_id,
      l.member_id,
      CONCAT(m.First_name, ' ', m.Last_Name) AS member_name,
      l.loan_amount,
      l.interest_rate,
      l.loan_term_months,
      l.monthly_payment,
      l.total_interest,
      l.total_repayment,
      l.balance,
      l.accumulated_interest,
      l.disbursement_date,
      l.maturity_date,
      l.status,
      (SELECT COUNT(*) FROM loan_schedule WHERE loan_id = l.id AND status = 'Paid') AS payments_made,
      (SELECT COUNT(*) FROM loan_schedule WHERE loan_id = l.id AND status = 'Pending') AS payments_pending,
      (SELECT COUNT(*) FROM loan_schedule WHERE loan_id = l.id AND status = 'Overdue') AS payments_overdue
    FROM loans l
    JOIN members_mst m ON l.member_id = m.id
    WHERE 1=1
  `;

  const params = [];

  if (memberId) {
    query += ` AND l.member_id = ?`;
    params.push(memberId);
  }

  if (memberName) {
    query += ` AND (m.First_name LIKE ? OR m.Last_Name LIKE ? OR CONCAT(m.First_name, ' ', m.Last_Name) LIKE ?)`;
    const namePattern = `%${memberName}%`;
    params.push(namePattern, namePattern, namePattern);
  }

  if (loanStatus) {
    query += ` AND l.status = ?`;
    params.push(loanStatus);
  }

  query += ` ORDER BY l.disbursement_date DESC`;

  const startNum = parseInt(start) || 0;
  const lengthNum = parseInt(length) || 10;
  query += ` LIMIT ${startNum}, ${lengthNum}`;

  const [rows] = await db.execute(query, params);

  rows.forEach(r => {
    Object.keys(r).forEach(k => {
      if (r[k] === null) r[k] = '';
    });
  });

  const [[{ total }]] = await db.query('SELECT FOUND_ROWS() AS total');

  res.json({
    draw: Number(draw) || 0,
    recordsTotal: total,
    recordsFiltered: total,
    data: rows
  });
}));

// GET: Full interest history page
// Get daily interest history - FIXED (NO LIMIT PARAMETER)
app.get('/loans/:loan_id/interest-history', asyncHandler(async (req, res) => {
  const { loan_id } = req.params;
  const { start_date, end_date, limit = 90 } = req.query;
  const db = dbConfig;

  // Parse limit to integer and sanitize
  const limitNum = Math.min(Math.max(parseInt(limit) || 90, 1), 1000);

  let query = `
    SELECT 
      calculation_date, 
      outstanding_balance, 
      daily_interest_amount, 
      annual_interest_rate
    FROM loan_daily_interest
    WHERE loan_id = ?
  `;
  const params = [loan_id];

  if (start_date && end_date) {
    query += ` AND calculation_date BETWEEN ? AND ?`;
    params.push(start_date, end_date);
  }

  query += ` ORDER BY calculation_date DESC LIMIT ${limitNum}`; // ✅ Direct value, not parameter

  const [history] = await db.execute(query, params);

  res.json({ history: history });
}));

// GET: Full interest history page - FIXED
app.get('/loans/:loan_id/interest-full', checkAuth, asyncHandler(async (req, res) => {
  const { loan_id } = req.params;
  const db = dbConfig;

  // Get loan details
  const [loan] = await db.execute(`
    SELECT 
      l.id,
      l.member_id,
      CONCAT(m.First_name, ' ', m.Last_Name) AS member_name,
      l.loan_amount,
      l.balance,
      l.accumulated_interest,
      l.interest_rate
    FROM loans l
    JOIN members_mst m ON l.member_id = m.id
    WHERE l.id = ?
  `, [loan_id]);

  if (loan.length === 0) {
    return res.status(404).send('Loan not found');
  }

  // Get all interest history (NO LIMIT)
  const [history] = await db.execute(`
    SELECT 
      calculation_date,
      outstanding_balance,
      daily_interest_amount,
      annual_interest_rate
    FROM loan_daily_interest
    WHERE loan_id = ?
    ORDER BY calculation_date DESC
  `, [loan_id]);

  res.render('loan_interest_history', {
    currentPage: 'loans',
    user: req.session.user,
    loan: loan[0],
    history: history
  });
}));

// GET: Export interest history to CSV - Already correct
app.get('/loans/:loan_id/interest-export', checkAuth, asyncHandler(async (req, res) => {
  const { loan_id } = req.params;
  const db = dbConfig;

  const [history] = await db.execute(`
    SELECT 
      calculation_date as 'Date',
      outstanding_balance as 'Outstanding Balance',
      daily_interest_amount as 'Daily Interest',
      annual_interest_rate as 'Annual Rate (%)'
    FROM loan_daily_interest
    WHERE loan_id = ?
    ORDER BY calculation_date DESC
  `, [loan_id]);

  if (history.length === 0) {
    return res.status(404).send('No interest history found');
  }

  // Generate CSV
  const headers = Object.keys(history[0]);
  const csvRows = [headers.join(',')];

  for (const row of history) {
    const values = headers.map(header => {
      const value = row[header];
      return typeof value === 'string' ? `"${value}"` : value;
    });
    csvRows.push(values.join(','));
  }

  const csv = csvRows.join('\n');

  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', `attachment; filename=loan_${loan_id}_interest_history.csv`);
  res.send(csv);
}));

// GET: Interest summary for dashboard
app.get('/loans/:loan_id/interest-summary', checkAuth, asyncHandler(async (req, res) => {
  const { loan_id } = req.params;
  const db = dbConfig;

  const [summary] = await db.execute(`
    SELECT 
      COUNT(*) as total_days,
      SUM(daily_interest_amount) as total_interest,
      AVG(outstanding_balance) as avg_balance,
      MIN(calculation_date) as first_date,
      MAX(calculation_date) as last_date,
      SUM(CASE WHEN calculation_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) 
          THEN daily_interest_amount ELSE 0 END) as last_30_days_interest,
      SUM(CASE WHEN calculation_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY) 
          THEN daily_interest_amount ELSE 0 END) as last_7_days_interest
    FROM loan_daily_interest
    WHERE loan_id = ?
  `, [loan_id]);

  res.json(summary[0] || {});
}));

// Get next payment
app.get('/loans/:loan_id/next-payment', asyncHandler(async (req, res) => {
  const { loan_id } = req.params;
  const db = dbConfig;

  const [loan] = await db.execute(`
    SELECT l.*, CONCAT(m.First_name, ' ', m.Last_Name) AS member_name
    FROM loans l
    JOIN members_mst m ON l.member_id = m.id
    WHERE l.id = ?
  `, [loan_id]);

  if (loan.length === 0) {
    return res.status(404).json({ error: 'Loan not found' });
  }

  const [nextPayment] = await db.execute(`
    SELECT * FROM loan_schedule 
    WHERE loan_id = ? AND status = 'Pending' 
    ORDER BY payment_number ASC LIMIT 1
  `, [loan_id]);

  res.json({
    loan: loan[0],
    nextPayment: nextPayment.length > 0 ? nextPayment[0] : null
  });
}));

// Loans report
app.post('/reports/loans-report', asyncHandler(async (req, res) => {
  const { memberId, memberName, loanStatus, startDate, endDate, start, length, draw } = req.body;
  const db = dbConfig;

  let query = `
    SELECT SQL_CALC_FOUND_ROWS
      ROW_NUMBER() OVER (ORDER BY l.id) AS SN,
      l.id AS loan_id,
      l.member_id,
      CONCAT(m.First_name, ' ', m.Last_Name) AS member_name,
      m.tel_no,
      l.loan_amount,
      l.interest_rate,
      l.loan_term_months,
      l.monthly_payment,
      l.total_interest,
      l.total_repayment,
      l.balance,
      l.accumulated_interest,
      l.disbursement_date,
      l.maturity_date,
      l.status,
      (SELECT COUNT(*) FROM loan_schedule WHERE loan_id = l.id AND status = 'Paid') AS payments_made,
      (SELECT COUNT(*) FROM loan_schedule WHERE loan_id = l.id AND status = 'Pending') AS payments_pending,
      (SELECT COUNT(*) FROM loan_schedule WHERE loan_id = l.id AND status = 'Overdue') AS payments_overdue,
      (SELECT SUM(paid_amount) FROM loan_schedule WHERE loan_id = l.id AND status = 'Paid') AS total_paid,
      DATEDIFF(CURDATE(), l.disbursement_date) AS days_since_disbursement,
      CASE 
        WHEN l.status = 'Completed' THEN 'Completed'
        WHEN (SELECT COUNT(*) FROM loan_schedule WHERE loan_id = l.id AND status = 'Overdue') > 0 THEN 'Has Overdue'
        WHEN l.balance > 0 THEN 'Active'
        ELSE 'Up to Date'
      END AS payment_status
    FROM loans l
    JOIN members_mst m ON l.member_id = m.id
    WHERE 1=1
  `;

  const params = [];

  if (memberId) {
    query += ` AND l.member_id = ?`;
    params.push(memberId);
  }

  if (memberName) {
    query += ` AND (m.First_name LIKE ? OR m.Last_Name LIKE ? OR CONCAT(m.First_name, ' ', m.Last_Name) LIKE ?)`;
    const namePattern = `%${memberName}%`;
    params.push(namePattern, namePattern, namePattern);
  }

  if (loanStatus) {
    query += ` AND l.status = ?`;
    params.push(loanStatus);
  }

  if (startDate && endDate) {
    query += ` AND l.disbursement_date BETWEEN ? AND ?`;
    params.push(startDate, endDate);
  }

  query += ` ORDER BY l.disbursement_date DESC`;

  const startNum = parseInt(start) || 0;
  const lengthNum = parseInt(length) || 10;
  query += ` LIMIT ${startNum}, ${lengthNum}`;

  const [rows] = await db.execute(query, params);

  rows.forEach(r => {
    Object.keys(r).forEach(k => {
      if (r[k] === null) r[k] = '';
    });
  });

  const [[{ total }]] = await db.query('SELECT FOUND_ROWS() AS total');

  res.json({
    draw: Number(draw) || 0,
    recordsTotal: total,
    recordsFiltered: total,
    data: rows
  });
}));

// Loans summary
app.get('/reports/loans-summary', asyncHandler(async (req, res) => {
  const db = dbConfig;

  const [summary] = await db.query(`
  SELECT 
      COUNT(*) AS total_loans,
      SUM(CASE WHEN status = 'Active' THEN 1 ELSE 0 END) AS active_loans,
      SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) AS completed_loans,
      SUM(CASE WHEN status = 'Defaulted' THEN 1 ELSE 0 END) AS defaulted_loans,
      SUM(CASE WHEN status = 'Active' and interest_method ='simple_interest'  THEN 1 ELSE 0 END) AS quick_loans,
      SUM(loan_amount) AS total_disbursed,
      SUM(balance) AS total_outstanding,
      SUM(total_interest) AS total_interest_expected,
      (SELECT SUM(paid_amount) FROM loan_schedule WHERE status = 'Paid') AS total_collected,
      (SELECT COUNT(DISTINCT loan_id) FROM loan_schedule WHERE status = 'Overdue') AS loans_with_overdue,
      (select sum(interest_paid) from loan_payment_history) as Interest_Paid
    FROM loans
  `);

  res.json(summary[0]);
}));

// Export loans report
app.post('/reports/loans-export', asyncHandler(async (req, res) => {
  const { memberId, memberName, loanStatus, startDate, endDate } = req.body;
  const db = dbConfig;

  let query = `
    SELECT 
      l.id AS 'Loan ID',
      l.member_id AS 'Member ID',
      CONCAT(m.First_name, ' ', m.Last_Name) AS 'Member Name',
      m.tel_no AS 'Phone',
      l.loan_amount AS 'Loan Amount',
      l.interest_rate AS 'Interest Rate (%)',
      l.loan_term_months AS 'Term (Months)',
      l.monthly_payment AS 'Monthly Payment',
      l.total_interest AS 'Total Interest',
      l.total_repayment AS 'Total Repayment',
      l.balance AS 'Balance',
      l.accumulated_interest AS 'Accumulated Interest',
      l.disbursement_date AS 'Disbursement Date',
      l.maturity_date AS 'Maturity Date',
      l.status AS 'Status',
      (SELECT COUNT(*) FROM loan_schedule WHERE loan_id = l.id AND status = 'Paid') AS 'Payments Made',
      (SELECT COUNT(*) FROM loan_schedule WHERE loan_id = l.id AND status = 'Overdue') AS 'Overdue Payments'
    FROM loans l
    JOIN members_mst m ON l.member_id = m.id
    WHERE 1=1
  `;

  const params = [];

  if (memberId) {
    query += ` AND l.member_id = ?`;
    params.push(memberId);
  }

  if (memberName) {
    query += ` AND (m.First_name LIKE ? OR m.Last_Name LIKE ? OR CONCAT(m.First_name, ' ', m.Last_Name) LIKE ?)`;
    const namePattern = `%${memberName}%`;
    params.push(namePattern, namePattern, namePattern);
  }

  if (loanStatus) {
    query += ` AND l.status = ?`;
    params.push(loanStatus);
  }

  if (startDate && endDate) {
    query += ` AND l.disbursement_date BETWEEN ? AND ?`;
    params.push(startDate, endDate);
  }

  query += ` ORDER BY l.disbursement_date DESC`;

  const [rows] = await db.execute(query, params);

  const headers = Object.keys(rows[0] || {});
  const csvRows = [headers.join(',')];

  for (const row of rows) {
    const values = headers.map(header => `"${row[header]}"`);
    csvRows.push(values.join(','));
  }

  const csv = csvRows.join('\n');

  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', 'attachment; filename=loans-report.csv');
  res.send(csv);
}));

// Get loan details with accumulated interest
// Get loan details with accumulated interest - HANDLE BOTH METHODS
app.get('/loans/:loan_id/details', asyncHandler(async (req, res) => {
  const { loan_id } = req.params;
  const db = dbConfig;

  const [loan] = await db.execute(`
    SELECT l.*, CONCAT(m.First_name, ' ', m.Last_Name) AS member_name,
           (l.balance + l.accumulated_interest) AS total_owed
    FROM loans l
    JOIN members_mst m ON l.member_id = m.id
    WHERE l.id = ?
  `, [loan_id]);

  if (loan.length === 0) {
    return res.status(404).json({ error: 'Loan not found' });
  }

  const interestMethod = loan[0].interest_method || 'reducing_balance';

  if (interestMethod === 'reducing_balance') {
    // Get interest accrued in last 30 days
    const [recentInterest] = await db.execute(`
      SELECT SUM(daily_interest_amount) AS last_30_days_interest
      FROM loan_daily_interest
      WHERE loan_id = ? 
        AND calculation_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
    `, [loan_id]);

    res.json({
      loan: loan[0],
      interest_last_30_days: recentInterest[0]?.last_30_days_interest || 0
    });
  } else {
    // Simple interest - no daily calculation
    res.json({
      loan: loan[0],
      interest_last_30_days: 0
    });
  }
}));
// Get daily interest history
app.get('/loans/:loan_id/interest-history', asyncHandler(async (req, res) => {
  const { loan_id } = req.params;
  const { start_date, end_date, limit = 30 } = req.query;
  const db = dbConfig;

  let query = `
    SELECT calculation_date, outstanding_balance, daily_interest_amount, annual_interest_rate
    FROM loan_daily_interest
    WHERE loan_id = ?
  `;
  const params = [loan_id];

  if (start_date && end_date) {
    query += ` AND calculation_date BETWEEN ? AND ?`;
    params.push(start_date, end_date);
  }

  query += ` ORDER BY calculation_date DESC LIMIT ?`;
  params.push(parseInt(limit));

  const [history] = await db.execute(query, params);

  res.json({ history: history });
}));

// Calculate interest for date range
app.post('/loans/:loan_id/calculate-interest', asyncHandler(async (req, res) => {
  const { loan_id } = req.params;
  const { start_date, end_date } = req.body;
  const db = dbConfig;

  const [loan] = await db.execute('SELECT * FROM loans WHERE id = ?', [loan_id]);

  if (loan.length === 0) {
    throw new Error('Loan not found');
  }

  const loanData = loan[0];
  const dailyInterestRate = (loanData.interest_rate / 100) / 365;

  let currentDate = new Date(start_date || new Date());
  const endDateTime = new Date(end_date || new Date());
  let totalInterest = 0;

  while (currentDate <= endDateTime) {
    const dateStr = currentDate.toISOString().split('T')[0];
    const dailyInterest = loanData.balance * dailyInterestRate;

    const [existing] = await db.execute(
      'SELECT id FROM loan_daily_interest WHERE loan_id = ? AND calculation_date = ?',
      [loan_id, dateStr]
    );

    if (existing.length === 0) {
      await db.execute(`
        INSERT INTO loan_daily_interest 
        (loan_id, calculation_date, outstanding_balance, daily_interest_amount, annual_interest_rate)
        VALUES (?, ?, ?, ?, ?)
      `, [loan_id, dateStr, loanData.balance, dailyInterest, loanData.interest_rate]);

      totalInterest += dailyInterest;
    }

    currentDate.setDate(currentDate.getDate() + 1);
  }

  await db.execute(
    'UPDATE loans SET accumulated_interest = accumulated_interest + ? WHERE id = ?',
    [totalInterest, loan_id]
  );

  res.json({
    success: true,
    message: 'Interest calculated successfully',
    total_interest: totalInterest
  });
}));

// ==================== LIQUIDITY ROUTES ====================

// GET: Liquidity summary for dashboard pie chart
app.get('/liquidity/summary', checkAuth, asyncHandler(async (req, res) => {
  const db = dbConfig;

  // Get latest amount per category (most recent as_at_date)
  const [rows] = await db.execute(`
    SELECT 
      category,
      SUM(amount) AS total_amount,
      MAX(as_at_date) AS last_updated
    FROM liquidity_funds    
    GROUP BY category
    ORDER BY total_amount DESC
  `);

  const total = rows.reduce((sum, r) => sum + parseFloat(r.total_amount), 0);

  res.json({
    categories: rows,
    total: total
  });
}));

// GET: Liquidity report page
app.get('/liquidity_report', checkAuth, asyncHandler(async (req, res) => {
  const db = dbConfig;

  // Get all distinct dates for the date filter
 const [dates] = await db.execute(`
    SELECT DATE_FORMAT(as_at_date, '%Y-%m-%d') AS date_val
    FROM liquidity_funds
    GROUP BY as_at_date
    ORDER BY as_at_date DESC
    LIMIT 24
`);

  // Get latest records grouped by category and institution
  const [records] = await db.execute(`
    SELECT 
      lf.*,
      DATE_FORMAT(lf.as_at_date, '%d-%b-%Y') AS formatted_date
    FROM liquidity_funds lf
    WHERE lf.as_at_date = (SELECT MAX(as_at_date) FROM liquidity_funds)
    ORDER BY lf.category, lf.institution
  `);

  // Summary by category
  const [summary] = await db.execute(`
    SELECT 
      category,
      SUM(amount) AS total_amount,
      COUNT(*) AS account_count
    FROM liquidity_funds    
    GROUP BY category
    ORDER BY total_amount DESC
  `);

  const grandTotal = summary.reduce((s, r) => s + parseFloat(r.total_amount), 0);

  res.render('liquidity_report', {
    currentPage: 'liquidity_report',
    user: req.session.user,
    records,
    summary,
    dates,
    grandTotal
  });
}));

// POST: Save/update liquidity entry
app.post('/liquidity/save', checkAuth, asyncHandler(async (req, res) => {
  const { id, category, institution, account_ref, amount, notes, as_at_date } = req.body;
  const db = dbConfig;

  if (!category || !amount || !as_at_date) {
    return res.status(400).json({ error: 'Category, amount and date are required' });
  }

  if (id) {
    await db.execute(`
      UPDATE liquidity_funds
      SET category=?, institution=?, account_ref=?, amount=?, notes=?, as_at_date=?, created_by=?
      WHERE id=?
    `, [category, institution, account_ref, amount, notes, as_at_date, req.session.user.username, id]);
  } else {
    await db.execute(`
      INSERT INTO liquidity_funds (category, institution, account_ref, amount, notes, as_at_date, created_by)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `, [category, institution, account_ref, amount, notes, as_at_date, req.session.user.username]);
  }

  res.json({ success: true, message: 'Liquidity record saved successfully' });
}));

// POST: Filter liquidity by date
app.post('/liquidity/filter', checkAuth, asyncHandler(async (req, res) => {
  const { as_at_date } = req.body;
  const db = dbConfig;

  const dateFilter = as_at_date || (await db.execute(
    'SELECT MAX(as_at_date) AS d FROM liquidity_funds'
  ))[0][0].d;

  const [records] = await db.execute(`
    SELECT *, DATE_FORMAT(as_at_date, '%d-%b-%Y') AS formatted_date
    FROM liquidity_funds
    WHERE as_at_date = ?
    ORDER BY category, institution
  `, [dateFilter]);

  const [summary] = await db.execute(`
    SELECT category, SUM(amount) AS total_amount, COUNT(*) AS account_count
    FROM liquidity_funds
    WHERE as_at_date = ?
    GROUP BY category ORDER BY total_amount DESC
  `, [dateFilter]);

  const grandTotal = summary.reduce((s, r) => s + parseFloat(r.total_amount), 0);

  res.json({ records, summary, grandTotal });
}));

// DELETE: Remove liquidity entry
app.post('/liquidity/delete/:id', checkAuth, asyncHandler(async (req, res) => {
  const db = dbConfig;
  await db.execute('DELETE FROM liquidity_funds WHERE id = ?', [req.params.id]);
  res.json({ success: true });
}));

// ==================== CRON JOBS ====================

// Mark overdue payments (runs daily at midnight)
cron.schedule('0 0 * * *', async () => {
  try {
    const db = dbConfig;
    await db.execute(`
      UPDATE loan_schedule 
      SET status = 'Overdue' 
      WHERE status = 'Pending' AND due_date < CURDATE()
    `);
    console.log('[CRON] Loan schedule updated - overdue payments marked');
  } catch (err) {
    console.error('[CRON] Error updating loan schedule:', err);
  }
});

// Calculate daily interest (runs at 11:59 PM)
cron.schedule('59 23 * * *', async () => {
  console.log('\n[CRON] Running daily interest calculation...');
  await calculateDailyInterest();
});

// ==================== ERROR HANDLING ====================

// 404 handler
app.use((req, res) => {
  res.status(404).send('Page not found');
});

// Global error handler
app.use((err, req, res, next) => {
  console.error('Error:', err);
  res.status(500).json({
    error: process.env.NODE_ENV === 'production'
      ? 'An error occurred'
      : err.message
  });
});


// GET: Dashboard with dynamic data
app.get('/', checkAuth, asyncHandler(async (req, res) => {
  const db = dbConfig;

  // Active members count
  const [memberCount] = await db.execute(
    'SELECT COUNT(*) AS memberCount FROM members_mst WHERE status = "ACTIVE"'
  );
  
  // Total savings
  const [savings] = await db.execute(
    'SELECT SUM(Amount) AS saving_sum FROM transactions WHERE transaction_type = "Saving"'
  );
  
  // Total income for current year
  const [income] = await db.execute(
    'SELECT SUM(Amount) AS income_sum FROM transactions WHERE transaction_type LIKE "%Income%" AND payment_period = YEAR(CURDATE())'
  );

  // Total loans disbursed
  const [totalLoans] = await db.execute(
    'SELECT SUM(loan_amount) AS total_loans FROM loans'
  );

  // Outstanding loan balance
  const [outstandingLoans] = await db.execute(
    'SELECT SUM(balance) AS outstanding_balance FROM loans WHERE status = "Active"'
  );

  // Monthly savings for current year
  const [monthlySavings] = await db.execute(`
    SELECT 
      MONTH(tran_date) AS month,
      SUM(Amount) AS total
    FROM transactions
    WHERE transaction_type = 'Saving' 
      AND YEAR(tran_date) = YEAR(CURDATE())
    GROUP BY MONTH(tran_date)
    ORDER BY MONTH(tran_date)
  `);

  // Monthly income for current year
  const [monthlyIncome] = await db.execute(`
    SELECT 
      MONTH(tran_date) AS month,
      SUM(Amount) AS total
    FROM transactions
    WHERE transaction_type LIKE '%Income%'
      AND YEAR(tran_date) = YEAR(CURDATE())
    GROUP BY MONTH(tran_date)
    ORDER BY MONTH(tran_date)
  `);

  // Income breakdown
  const [incomeBreakdown] = await db.execute(`
    SELECT 
      SUM(CASE WHEN transaction_type = 'Loan Debit Intrest' THEN Amount ELSE 0 END) AS interest_income,
      SUM(CASE WHEN transaction_type = 'Penalty Fees' THEN Amount ELSE 0 END) AS penalty_income,
      SUM(CASE WHEN transaction_type LIKE '%Membership%' OR transaction_type LIKE '%welfare%' THEN Amount ELSE 0 END) AS fee_income
    FROM transactions
    WHERE YEAR(tran_date) = YEAR(CURDATE())
  `);

  // Recent transactions
  const [recentTransactions] = await db.execute(`
    SELECT 
      t.id,
      t.member_id,
      CONCAT(m.First_name, ' ', m.Last_Name) AS member_name,
      t.Amount,
      t.transaction_type,
      t.Debit_Credit,
      DATE_FORMAT(t.tran_date, '%d/%m/%Y') AS tran_date,
      t.created_at
    FROM transactions t
    JOIN members_mst m ON t.member_id = m.id
    ORDER BY t.created_at DESC
    LIMIT 10
  `);

  // Active loans with overdue status
  const [loanStats] = await db.execute(`
    SELECT 
      COUNT(DISTINCT l.id) AS total_active_loans,
      COUNT(DISTINCT CASE WHEN ls.status = 'Overdue' THEN l.id END) AS loans_with_overdue,
      SUM(l.balance) AS total_outstanding
    FROM loans l
    LEFT JOIN loan_schedule ls ON l.id = ls.loan_id
    WHERE l.status = 'Active'
  `);

  // Top borrowers
  const [topBorrowers] = await db.execute(`
    SELECT 
      m.id,
      CONCAT(m.First_name, ' ', m.Last_Name) AS name,
      SUM(l.loan_amount) AS total_borrowed,
      COUNT(l.id) AS loan_count
    FROM members_mst m
    JOIN loans l ON m.id = l.member_id
    GROUP BY m.id, m.First_name, m.Last_Name
    ORDER BY total_borrowed DESC
    LIMIT 5
  `);

  // Prepare monthly data arrays (all 12 months)
  const savingsData = new Array(12).fill(0);
  const incomeData = new Array(12).fill(0);

  monthlySavings.forEach(row => {
    savingsData[row.month - 1] = row.total;
  });

  monthlyIncome.forEach(row => {
    incomeData[row.month - 1] = row.total;
  });

  // Calculate percentages for income breakdown
  const totalIncome = parseFloat(incomeBreakdown[0].interest_income) + 
                      parseFloat(incomeBreakdown[0].penalty_income) + 
                      parseFloat(incomeBreakdown[0].fee_income);

  const interestPercentage = totalIncome > 0 ? 
    ((incomeBreakdown[0].interest_income / totalIncome) * 100).toFixed(0) : 0;
  const penaltyPercentage = totalIncome > 0 ? 
    ((incomeBreakdown[0].penalty_income / totalIncome) * 100).toFixed(0) : 0;
  const feePercentage = totalIncome > 0 ? 
    ((incomeBreakdown[0].fee_income / totalIncome) * 100).toFixed(0) : 0;

  res.render('index', {
    currentPage: 'home',
    user: req.session.user,
    memberCount: memberCount[0].memberCount,
    memberSaving: savings[0].saving_sum || 0,
    memberIncome: income[0].income_sum || 0,
    totalLoans: totalLoans[0].total_loans || 0,
    outstandingLoans: outstandingLoans[0].outstanding_balance || 0,
    savingsData: JSON.stringify(savingsData),
    incomeData: JSON.stringify(incomeData),
    interestIncome: incomeBreakdown[0].interest_income || 0,
    penaltyIncome: incomeBreakdown[0].penalty_income || 0,
    feeIncome: incomeBreakdown[0].fee_income || 0,
    interestPercentage,
    penaltyPercentage,
    feePercentage,
    recentTransactions,
    loanStats: loanStats[0],
    topBorrowers
  });
}));


// TEST ROUTE - Add this BEFORE the main /users route
app.get('/users-test', checkAuth, asyncHandler(async (req, res) => {
  console.log('Test route called');
  console.log('User role:', req.session.user.role);
  
  const db = dbConfig;
  
  const [users] = await db.execute('SELECT * FROM users');
  console.log('Users found:', users.length);
  
  res.json({
    user: req.session.user,
    users: users,
    count: users.length
  });
}));

// ==================== USER MANAGEMENT ROUTES ====================

// GET: User management page
app.get('/users', checkAuth, asyncHandler(async (req, res) => {
  // Check if user has admin role
  if (req.session.user.role !== 'Admin') {
    return res.status(403).send('Access denied. Admin only.');
  }

  const db = dbConfig;
  
  try {
    const [users] = await db.execute(`
      SELECT 
        u.id, 
        u.username, 
        u.first_name, 
        u.last_name, 
        u.role, 
        u.status,
        DATE_FORMAT(u.last_login, '%d-%b-%Y %H:%i') AS last_login,
        DATE_FORMAT(u.created_at, '%d-%b-%Y') AS created_at,
        creator.username AS created_by_username
      FROM users u
      LEFT JOIN users creator ON u.created_by = creator.id
      ORDER BY u.created_at DESC
    `);

    console.log('Users fetched:', users.length);

    res.render('users', { 
      users: users || [],
      currentPage: 'users', 
      user: req.session.user 
    });
  } catch (err) {
    console.error('Error fetching users:', err);
    res.render('users', { 
      users: [], 
      currentPage: 'users', 
      user: req.session.user,
      error: err.message
    });
  }
}));

// GET: Add user form
app.get('/users/add', checkAuth, asyncHandler(async (req, res) => {
  if (req.session.user.role !== 'Admin') {
    return res.status(403).send('Access denied. Admin only.');
  }

  res.render('user_add', { 
    currentPage: 'users', 
    user: req.session.user 
  });
}));

// POST: Create new user
app.post('/users/add', checkAuth, asyncHandler(async (req, res) => {
  if (req.session.user.role !== 'Admin') {
    return res.status(403).json({ error: 'Access denied' });
  }

  const db = dbConfig;
  const { username, password, confirm_password, first_name, last_name, role, status } = req.body;
  const errors = [];

  // Validations
  if (!username) errors.push("Username is required");
  if (!password) errors.push("Password is required");
  if (!first_name) errors.push("First name is required");
  if (!last_name) errors.push("Last name is required");
  if (!role) errors.push("Role is required");

  if (password && password.length < 6) {
    errors.push("Password must be at least 6 characters");
  }

  if (password !== confirm_password) {
    errors.push("Passwords do not match");
  }

  // Check username uniqueness
  const [existingUser] = await db.execute(
    'SELECT id FROM users WHERE username = ?',
    [username]
  );

  if (existingUser.length > 0) {
    errors.push("Username already exists");
  }

  if (errors.length > 0) {
    return res.status(400).json({ errors });
  }

  // Hash password
  const hashedPassword = await bcrypt.hash(password, 10);

  // Insert user
  await db.execute(`
    INSERT INTO users 
    (username, password, first_name, last_name, role, status, created_by, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, NOW())
  `, [
    username, 
    hashedPassword, 
    first_name, 
    last_name, 
    role || 'user', 
    status || 'active',
    req.session.user.id
  ]);

  res.json({
    success: true,
    message: 'User created successfully',
    user: { username, first_name, last_name, role }
  });
}));

// GET: Edit user form
app.get('/users/:id/edit', checkAuth, asyncHandler(async (req, res) => {
  if (req.session.user.role !== 'Admin') {
    return res.status(403).send('Access denied. Admin only.');
  }

  const { id } = req.params;
  const db = dbConfig;

  const [users] = await db.execute(`
    SELECT id, username, first_name, last_name, role, status
    FROM users 
    WHERE id = ?
  `, [id]);

  if (users.length === 0) {
    return res.status(404).send('User not found');
  }

  res.render('user_edit', { 
    editUser: users[0], 
    currentPage: 'users', 
    user: req.session.user 
  });
}));

// POST: Update user
app.post('/users/:id/edit', checkAuth, asyncHandler(async (req, res) => {
  if (req.session.user.role !== 'Admin') {
    return res.status(403).json({ error: 'Access denied' });
  }

  const { id } = req.params;
  const db = dbConfig;
  const { username, first_name, last_name, role, status } = req.body;
  const errors = [];

  // Validations
  if (!username) errors.push("Username is required");
  if (!first_name) errors.push("First name is required");
  if (!last_name) errors.push("Last name is required");
  if (!role) errors.push("Role is required");

  // Check username uniqueness (excluding current user)
  const [existingUser] = await db.execute(
    'SELECT id FROM users WHERE username = ? AND id != ?',
    [username, id]
  );

  if (existingUser.length > 0) {
    errors.push("Username already exists");
  }

  if (errors.length > 0) {
    return res.status(400).json({ errors });
  }

  // Update user
  await db.execute(`
    UPDATE users 
    SET username = ?, first_name = ?, last_name = ?, role = ?, status = ?
    WHERE id = ?
  `, [username, first_name, last_name, role, status || 'active', id]);

  res.json({
    success: true,
    message: 'User updated successfully'
  });
}));

// POST: Change password (by admin or self)
app.post('/users/:id/change-password', checkAuth, asyncHandler(async (req, res) => {
  const { id } = req.params;
  const { current_password, new_password, confirm_password } = req.body;
  const db = dbConfig;
  const errors = [];

  // Check if user is changing their own password or is admin
  const isOwnPassword = parseInt(id) === req.session.user.id;
  const isAdmin = req.session.user.role === 'Admin';

  if (!isOwnPassword && !isAdmin) {
    return res.status(403).json({ error: 'Access denied' });
  }

  // Validations
  if (!new_password) errors.push("New password is required");
  if (new_password && new_password.length < 6) {
    errors.push("Password must be at least 6 characters");
  }
  if (new_password !== confirm_password) {
    errors.push("Passwords do not match");
  }

  // If changing own password, verify current password
  if (isOwnPassword && !isAdmin) {
    if (!current_password) errors.push("Current password is required");

    const [user] = await db.execute('SELECT password FROM users WHERE id = ?', [id]);
    if (user.length > 0) {
      const match = await bcrypt.compare(current_password, user[0].password);
      if (!match) {
        errors.push("Current password is incorrect");
      }
    }
  }

  if (errors.length > 0) {
    return res.status(400).json({ errors });
  }

  // Hash new password
  const hashedPassword = await bcrypt.hash(new_password, 10);

  // Update password
  await db.execute('UPDATE users SET password = ? WHERE id = ?', [hashedPassword, id]);

  res.json({
    success: true,
    message: 'Password changed successfully'
  });
}));

// POST: Delete/Deactivate user
app.post('/users/:id/delete', checkAuth, asyncHandler(async (req, res) => {
  if (req.session.user.role !== 'Admin') {
    return res.status(403).json({ error: 'Access denied' });
  }

  const { id } = req.params;
  const db = dbConfig;

  // Prevent self-deletion
  if (parseInt(id) === req.session.user.id) {
    return res.status(400).json({ error: 'Cannot delete your own account' });
  }

  // Soft delete - just set status to inactive
  await db.execute('UPDATE users SET status = ? WHERE id = ?', ['inactive', id]);

  res.json({
    success: true,
    message: 'User deactivated successfully'
  });
}));


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




// ==================== START SERVER WITH INTEREST CALCULATION ====================

const PORT = process.env.PORT || 3000;

async function startServer() {
    try {
        // Test database connection
        await dbConfig.query('SELECT 1');
        console.log('✓ Database connection successful');
        
        // Run startup interest calculation
        await runStartupInterestCalculation();
        
        // Start the server
        app.listen(PORT, () => {
            console.log('=================================================');
            console.log(`✓ Server running at http://localhost:${PORT}`);
            console.log('=================================================\n');
        });
        
    } catch (err) {
        console.error('❌ Failed to start server:', err);
        process.exit(1);
    }
}

// Start the application
startServer();