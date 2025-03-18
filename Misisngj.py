import pandas as pd
import sqlparse
from sqlparse.sql import Identifier, Parenthesis
from sqlparse.tokens import Keyword

def extract_table_name(token):
    """
    Extracts table name from a JOIN statement token.
    Supports table aliases and subqueries.
    """
    if isinstance(token, Identifier):
        return str(token.get_real_name())
    elif isinstance(token, Parenthesis):
        return "Subquery"
    return str(token)

def find_missing_join_conditions(query_id, query_text):
    """
    Checks if there are JOINs in the SQL query missing an ON or USING clause and identifies the table or subquery.
    Prints warnings with query ID and query text for missing join conditions.
    
    :param query_id: Query ID
    :param query_text: SQL query as a string
    """
    parsed = sqlparse.parse(query_text)
    warnings = []

    for stmt in parsed:
        tokens = list(stmt.flatten())
        join_positions = []

        # Identify JOIN positions and corresponding tables or subqueries
        for i, token in enumerate(tokens):
            if token.match(Keyword, ("JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN")):
                # Check if the next token is a subquery (Parenthesis) or a table
                table_name = "Unknown"
                if i + 1 < len(tokens):
                    table_name = extract_table_name(tokens[i + 1])
                join_positions.append((i, table_name))

        # Check for missing ON or USING clause
        for pos, table_name in join_positions:
            has_condition = False
            for j in range(pos + 1, min(pos + 6, len(tokens))):
                if tokens[j].match(Keyword, ("ON", "USING")):
                    has_condition = True
                    break
            if not has_condition:
                warnings.append(f"⚠️ Missing ON/USING condition for JOIN on '{table_name}' in query ID {query_id}.")

    # Print Results
    if warnings:
        print(f"Query ID: {query_id}")
        print("Query Text:")
        print(query_text)
        for warning in warnings:
            print(warning)
        print("-" * 80)
    else:
        print(f"✅ Query ID {query_id}: All JOINs have ON/USING conditions.\n{'-' * 80}")

# Example DataFrame with Query IDs and SQL Queries
data = {
    'query_id': [101, 102, 103, 104],
    'query_text': [
        """SELECT * FROM users u 
           JOIN orders o ON u.id = o.user_id
           JOIN payments p ON o.id = p.order_id
           JOIN shipments s""",  # Missing ON for shipments

        """SELECT * FROM employees e 
           JOIN departments d USING (department_id) 
           JOIN locations l""",  # Missing ON for locations

        """SELECT * FROM customers c 
           JOIN orders o 
           JOIN payments p ON o.id = p.order_id""",  # Missing ON for orders

        """SELECT * FROM students s 
           JOIN classes c ON s.class_id = c.id 
           JOIN teachers t ON c.teacher_id = t.id 
           JOIN schools sch ON sch.id = t.school_id"""  # All valid joins
    ]
}

df = pd.DataFrame(data)

# Process each query in the DataFrame
for _, row in df.iterrows():
    find_missing_join_conditions(row['query_id'], row['query_text'])
