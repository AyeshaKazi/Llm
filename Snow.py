import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

# Assuming your data frame looks like this
# columns: ['query', 'table', 'columns', 'where_clause', 'join_conditions']
df = pd.DataFrame({
    'query': ["UPDATE Query 1", "UPDATE Query 2", "UPDATE Query 3"],
    'table': ["table1", "table2", "table1"],
    'columns': [["col1", "col2"], ["col3"], ["col1"]],
    'where_clause': ["condition1", "condition2", "condition3"],
    'join_conditions': [None, None, None]
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
plt.figure(figsize=(10, 8))
pos = nx.spring_layout(G)
nx.draw(G, pos, with_labels=True, node_color='lightblue', font_weight='bold', node_size=3000, font_size=10)
plt.title('SQL Dependency Graph')
plt.show()
