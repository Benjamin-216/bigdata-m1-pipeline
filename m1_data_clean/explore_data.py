import duckdb

con = duckdb.connect()
query = """
SELECT 
    behavior_type,
    COUNT(*) as cnt,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pct
FROM read_csv_auto('D:/BigData_Lab/week1/UserBehavior.csv', header=False, columns={
    'user_id': 'INTEGER',
    'item_id': 'INTEGER', 
    'product_id': 'INTEGER',
    'behavior_type': 'VARCHAR',
    'timestamp': 'INTEGER'
})
GROUP BY behavior_type
ORDER BY cnt DESC
"""
result = con.execute(query).fetchall()
print("behavior_type\t行数\t\t占比")
print("-" * 40)
for row in result:
    print(f"{row[0]}\t{row[1]:,}\t{row[2]}%")
