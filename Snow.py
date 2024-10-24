import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
from graphviz import Digraph

# Initialize the DataFrame with SQL queries (as before)
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

# Initialize Graphviz tree for the order of execution
tree = Digraph(format='png')

# Add nodes for each query in the tree
for i, row in df.iterrows():
    tree.node(f"Q{i+1}", f"{row['query']}")

# Connect queries to show the execution order in a tree structure
for i in range(len(df) - 1):
    tree.edge(f"Q{i+1}", f"Q{i+2}")  # Sequential order of execution

# Initialize NetworkX for subgraph construction (for mergeable queries)
G = nx.DiGraph()

# Add nodes for each query in the NetworkX graph
for i, row in df.iterrows():
    G.add_node(row['query'], table=row['table'], columns=row['columns'], where=row['where_clause'], joins=row['join_conditions'])

# Identify mergeable queries
mergeable_queries = []
for i, row1 in df.iterrows():
    for j, row2 in df.iterrows():
        if i != j:
            if row1['table'] == row2['table'] and row1['columns'] == row2['columns'] \
                    and row1['where_clause'] == row2['where_clause'] and row1['join_conditions'] == row2['join_conditions']:
                mergeable_queries.append((row1['query'], row2['query']))

# Remove merged queries from the main tree and construct subgraphs for them
for q1, q2 in mergeable_queries:
    idx1 = df.index[df['query'] == q1][0] + 1
    idx2 = df.index[df['query'] == q2][0] + 1
    
    # Create a merged node in the tree for the group of mergeable queries
    merge_node = f"Merge({q1},{q2})"
    tree.node(merge_node, f"Merged: {q1} + {q2}")
    
    # Connect the merged node to the sequential execution order in the tree
    if idx1 > 1:
        tree.edge(f"Q{idx1-1}", merge_node)  # Link previous query to the merged node
    if idx2 < len(df):
        tree.edge(merge_node, f"Q{idx2+1}")  # Link merged node to the next query in the tree

    # Add cyclic subgraph for mergeable queries using NetworkX
    G.add_edge(q1, q2)  # Cyclic edge between mergeable queries
    G.add_edge(q2, q1)

# Render and show the tree structure with merged nodes
tree.render('tree_with_merged_graph', view=True)

# Draw the subgraph for merged queries
plt.figure(figsize=(8, 6))
pos = nx.spring_layout(G)
nx.draw(G, pos, with_labels=True, node_color='lightblue', font_size=10, font_weight='bold', node_size=3000)
plt.title('Subgraph for Mergeable Queries')
plt.show()
