import sqlite3

# Connect to SQLite (or your DB connection)
conn = sqlite3.connect('stockanalysis.db')
cur = conn.cursor()

# Query the table
cur.execute("SELECT * FROM school_schema.Student")
rows = cur.fetchall()

# Print the rows
for row in rows:
    print(row)

# Close the connection
conn.close()
