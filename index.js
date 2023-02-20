const fs = require('fs');
const path = require('path');

class Database {
  constructor(databaseName, username, password) {
    this.databaseName = databaseName;
    this.databasePath = path.join(__dirname, databaseName);
    this.username = username;
    this.password = password;
  }

  authenticate(username, password) {
    return username === this.username && password === this.password;
  }

  createTable(tableName, schema, username, password) {
    if (!this.authenticate(username, password)) {
      throw new Error('Authentication failed');
    }

    const tablePath = path.join(this.databasePath, `${tableName}.json`);

    if (fs.existsSync(tablePath)) {
      throw new Error(`Table ${tableName} already exists`);
    }

    const columns = schema.reduce((acc, column) => {
      acc[column] = null;
      return acc;
    }, {});

    const data = { columns, rows: [] };
    fs.writeFileSync(tablePath, JSON.stringify(data));
  }

  insert(tableName, row, username, password) {
    if (!this.authenticate(username, password)) {
      throw new Error('Authentication failed');
    }

    const tablePath = path.join(this.databasePath, `${tableName}.json`);

    if (!fs.existsSync(tablePath)) {
      throw new Error(`Table ${tableName} does not exist`);
    }

    const tableData = JSON.parse(fs.readFileSync(tablePath));

    const newRow = Object.keys(tableData.columns).reduce((acc, column) => {
      if (column in row) {
        acc[column] = row[column];
      } else {
        acc[column] = null;
      }
      return acc;
    }, {});

    tableData.rows.push(newRow);
    fs.writeFileSync(tablePath, JSON.stringify(tableData));
  }

  select(tableName, condition, username, password) {
    if (!this.authenticate(username, password)) {
      throw new Error('Authentication failed');
    }

    const tablePath = path.join(this.databasePath, `${tableName}.json`);

    if (!fs.existsSync(tablePath)) {
      throw new Error(`Table ${tableName} does not exist`);
    }

    const tableData = JSON.parse(fs.readFileSync(tablePath));

    const filteredRows = tableData.rows.filter(condition);

    return filteredRows;
  }

  update(tableName, condition, updateValues, username, password) {
    if (!this.authenticate(username, password)) {
      throw new Error('Authentication failed');
    }

    const tablePath = path.join(this.databasePath, `${tableName}.json`);

    if (!fs.existsSync(tablePath)) {
      throw new Error(`Table ${tableName} does not exist`);
    }

    const tableData = JSON.parse(fs.readFileSync(tablePath));

    tableData.rows.forEach(row => {
      if (condition(row)) {
        Object.keys(updateValues).forEach(key => {
          row[key] = updateValues[key];
        });
      }
    });

    fs.writeFileSync(tablePath, JSON.stringify(tableData));
  }

  delete(tableName, condition, username, password) {
    if (!this.authenticate(username, password)) {
      throw new Error('Authentication failed');
    }

    const tablePath = path.join(this.databasePath,`${tableName}.json`);

    if (!fs.existsSync(tablePath)) {
      throw new Error(`Table ${tableName} does not exist`);
    }

    const tableData = JSON.parse(fs.readFileSync(tablePath));

    const filteredRows = tableData.rows.filter(row => !condition(row));

    tableData.rows = filteredRows;
    fs.writeFileSync(tablePath, JSON.stringify(tableData));
  }
}

const db = new Database('myDatabase', 'admin', 'password');

// Create a new table called 'users' with two columns: 'name' and 'age'
// db.createTable('users', ['name', 'age'], 'admin', 'password');

// Insert some rows into the 'users' table
// db.insert('users', { name: 'Alice', age: 30 }, 'admin', 'password');
// db.insert('users', { name: 'Bob', age: 40 }, 'admin', 'password');
// db.insert('users', { name: 'Charlie' }, 'admin', 'password'); // 'age' will be null for this row

// Select all rows from the 'users' table
const allUsers = db.select('users', () => true, 'admin', 'password');
console.log(allUsers);

// // Select only rows where the 'age' column is greater than 35
// const usersOver35 = db.select('users', row => row.age > 35, 'admin', 'password');
// console.log(usersOver35);

// // Update the 'age' of all rows where the 'name' is 'Bob'
// db.update('users', row => row.name === 'Bob', { age: 50 }, 'admin', 'password');

// // Delete all rows where the 'name' is 'Charlie'
// db.delete('users', row => row.name === 'Charlie', 'admin', 'password');