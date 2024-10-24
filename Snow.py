import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

# Extended DataFrame with 10+ SQL queries
df = pd.DataFrame({
    'query': [
        "UPDATE Query 1", "UPDATE Query 2", "UPDATE Query 3", "UPDATE Query 4", 
        "UPDATE Query 5", "UPDATE Query 6", "UPDATE Query 7", "UPDATE Query 8",
        "UPDATE Query 9", "UPDATE Query 10", "UPDATE Query 11"
    ],
    'table': [
        "table1", "table1", "table2", "table1", 
        "table3", "table2", "table3", "table4",
        "table1", "table2", "table3"
    ],
    'columns': [
        ["col1", "col2"], ["col1", "col2"], ["col3"], ["col1"], 
        ["col4"], ["col3"], ["col4"], ["col5"],
        ["col2"], ["col3"], ["col4"]
    ],
    'where_clause': [
        "condition1", "condition1", "condition2", "condition3", 
        "condition4", "condition2", "condition4", "condition5",
        "condition6", "condition2", "condition4"
    ],
    'join_conditions': [
        None, None, None, None, 
        None, None, None, "join1",
        None, None, None
    ]
})

# Initialize an empty directed graph
G = nx.DiGraph()

# Add nodes for each query
for i, row in df.iterrows():
    G.add_node(row['query'], table=row['table'], columns=row['columns'], where=row['where_clause'], joins=row['join_conditions'])

# Analyze dependencies and add edges
for i, row1 in df.iterrows():
    for j, row2 in df.iterrows():
        if i != j:
            # Add a dependency if query j updates a table/column that query i depends on
            if row1['table'] == row2['table'] and any(col in row2['columns'] for col in row1['columns']):
                G.add_edge(row2['query'], row1['query'])  # row1 depends on row2

# Identify mergeable queries based on similar table, columns, and conditions
mergeable_queries = []
for i, row1 in df.iterrows():
    for j, row2 in df.iterrows():
        if i != j:
            if row1['table'] == row2['table'] and row1['columns'] == row2['columns'] \
                    and row1['where_clause'] == row2['where_clause'] and row1['join_conditions'] == row2['join_conditions']:
                mergeable_queries.append((row1['query'], row2['query']))

# Print out mergeable queries
print("Mergeable Queries:", mergeable_queries)

# Draw the graph
plt.figure(figsize=(12, 8))
pos = nx.spring_layout(G, k=0.5)
nx.draw(G, pos, with_labels=True, node_color='lightgreen', font_weight='bold', node_size=3500, font_size=10, edge_color='gray')
plt.title('SQL Dependency Graph with Mergeable Queries')
plt.show()


