// server.js
const express = require('express');
const path = require('path');
const mysql = require('mysql2/promise');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// MySQL connection pool
const pool = mysql.createPool({
  host: 'localhost',       // change if needed
  user: 'root',            // your MySQL username
  password: '',            // your MySQL password
  database: 'crud_demo',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

// Get all items
app.get('/api/items', async (req, res) => {
  try {
    const [rows] = await pool.query('SELECT * FROM items ORDER BY id DESC');
    res.json(rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Database error' });
  }
});

// Insert new item
app.post('/api/items', async (req, res) => {
  const { title, description } = req.body;
  if (!title || typeof title !== 'string') {
    return res.status(400).json({ error: 'Title is required' });
  }
  try {
    const [result] = await pool.query(
      'INSERT INTO items (title, description) VALUES (?, ?)',
      [title.trim(), description || null]
    );
    const [newItem] = await pool.query('SELECT * FROM items WHERE id = ?', [result.insertId]);
    res.status(201).json(newItem[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Database insert error' });
  }
});

// Update item
app.put('/api/items/:id', async (req, res) => {
  const { title, description } = req.body;
  const id = parseInt(req.params.id);
  if (!title || typeof title !== 'string') {
    return res.status(400).json({ error: 'Title is required' });
  }
  try {
    const [result] = await pool.query(
      'UPDATE items SET title=?, description=?, updatedAt=NOW() WHERE id=?',
      [title.trim(), description || null, id]
    );
    if (result.affectedRows === 0) return res.status(404).json({ error: 'Not found' });
    const [updatedItem] = await pool.query('SELECT * FROM items WHERE id = ?', [id]);
    res.json(updatedItem[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Database update error' });
  }
});

// Delete item
app.delete('/api/items/:id', async (req, res) => {
  const id = parseInt(req.params.id);
  try {
    const [result] = await pool.query('DELETE FROM items WHERE id = ?', [id]);
    if (result.affectedRows === 0) return res.status(404).json({ error: 'Not found' });
    res.json({ message: 'Deleted' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Database delete error' });
  }
});

// Fallback for SPA
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public/index.html'));
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running at http://localhost:${PORT}`));
