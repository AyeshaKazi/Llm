import sqlparse
from sqlparse.sql import Identifier, TokenList
from sqlparse.tokens import Keyword

def extract_table_name(token):
    """
    Extracts table name from a JOIN statement token.
    Supports table aliases like 'table_name AS alias'.
    """
    if isinstance(token, Identifier):
        # Extract the actual table name, ignoring aliases
        return str(token.get_real_name())
    return str(token)

def find_missing_join_conditions(query):
    """
    Checks if there are JOINs in the SQL query missing an ON or USING clause and identifies the table.
    
    :param query: SQL query as a string
    :return: List of warnings indicating missing join conditions with table names
    """
    parsed = sqlparse.parse(query)
    warnings = []

    for stmt in parsed:
        tokens = list(stmt.flatten())
        join_positions = []

        # Identify JOIN positions and corresponding tables
        for i, token in enumerate(tokens):
            if token.match(Keyword, ("JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN")):
                join_positions.append((i, extract_table_name(tokens[i + 1])))

        # Check for missing ON or USING clause
        for pos, table_name in join_positions:
            has_condition = False
            for j in range(pos + 1, min(pos + 6, len(tokens))):
                if tokens[j].match(Keyword, ("ON", "USING")):
                    has_condition = True
                    break
            if not has_condition:
                warnings.append(f"⚠️ Missing ON/USING condition for JOIN on table '{table_name}'.")
    
    return warnings

# Example SQL Queries with Multiple Joins
sql_queries = [
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
       JOIN schools sch ON sch.id = t.school_id"""  # All joins have ON conditions (valid)
]

for query in sql_queries:
    print(f"Checking Query:\n{query}\n")
    issues = find_missing_join_conditions(query)
    if issues:
        for issue in issues:
            print(issue)
    else:
        print("✅ All JOINs have ON/USING conditions.")
    print("-" * 80)
