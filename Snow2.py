from graphviz import Digraph

# Initialize a directed graph for the execution tree (top-down layout)
tree = Digraph(format='png')
tree.attr(rankdir='TB')  # Force top-to-bottom layout

# Example SQL queries from the dataframe
queries = [
    "Query 1", "Query 2", "Query 3", "Query 4", 
    "Query 5", "Query 6", "Query 7", "Query 8", 
    "Query 9", "Query 10", "Query 11"
]

# Add nodes for each query in the tree, assigning the order of execution explicitly
for i, query in enumerate(queries):
    tree.node(f"Q{i+1}", f"{query}")

# Define which queries are mergeable and create subgraphs for them
merged_queries = {
    'Merge 1': ['Query 2', 'Query 5'],  # Example of mergeable queries
    'Merge 2': ['Query 7', 'Query 10']
}

# Connect nodes in the main tree, skipping over merged queries
for i in range(len(queries) - 1):
    # If the current query is part of a merged group, skip it
    if any(queries[i] in group for group in merged_queries.values()):
        continue
    # If the next query is part of a merged group, connect to the merged node instead of the individual query
    if any(queries[i+1] in group for group in merged_queries.values()):
        # Find the merged group for the next query
        for merge_group, merge_group_queries in merged_queries.items():
            if queries[i+1] in merge_group_queries:
                tree.edge(f"Q{i+1}", merge_group)
                break
    else:
        tree.edge(f"Q{i+1}", f"Q{i+2}")

# Adding subgraph for each group of mergeable queries
for merge_group, merge_queries in merged_queries.items():
    with tree.subgraph(name=f'cluster_{merge_group}') as subgraph:
        subgraph.attr(style='dotted', label=merge_group)
        # Add each query as a node in the subgraph
        for merge_query in merge_queries:
            query_idx = queries.index(merge_query) + 1
            subgraph.node(f"Q{query_idx}", merge_query)
        # Connect the first and last query in the merge group to the main tree
        first_query_idx = queries.index(merge_queries[0]) + 1
        last_query_idx = queries.index(merge_queries[-1]) + 1
        # Connect the first merged query to the previous node in the tree
        if first_query_idx > 1:
            tree.edge(f"Q{first_query_idx-1}", f"Q{first_query_idx}")
        # Connect the last merged query to the next node in the tree
        if last_query_idx < len(queries):
            tree.edge(f"Q{last_query_idx}", f"Q{last_query_idx+1}")

# Render and display the tree with subgraphs
tree.render('tree_with_merged_queries_updated_all', view=True)
