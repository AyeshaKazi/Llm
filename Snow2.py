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

# Define which queries are mergeable (this can be dynamic in the future)
merged_queries = {
    'Merge 1': ['Query 2', 'Query 5'],  # Example of mergeable queries
    'Merge 2': ['Query 7', 'Query 10']
}

# Flatten merged queries to know which queries are part of a merge
flattened_merged = [query for merge_group in merged_queries.values() for query in merge_group]

# Add nodes for each query in the tree, assigning the order of execution explicitly
for i, query in enumerate(queries):
    tree.node(f"Q{i+1}", f"{query}")

# Dynamically add edges between queries, skipping over merged nodes
i = 0
while i < len(queries) - 1:
    current_query = queries[i]
    next_query = queries[i + 1]
    
    # If current query is part of a merge, skip over the entire merged group
    if current_query in flattened_merged:
        # Find the last query in the current merged group and connect it to the next unmerged query
        merge_group_name = [k for k, v in merged_queries.items() if current_query in v][0]
        last_query_in_merge = merged_queries[merge_group_name][-1]
        last_query_idx = queries.index(last_query_in_merge) + 1
        
        # Check if the last query in the merge is the last in the list
        if last_query_idx < len(queries):
            tree.edge(f"Q{last_query_idx}", f"Q{last_query_idx + 1}")
        
        # Move index past the merged group
        i = last_query_idx
    else:
        # Regular edge addition if not part of a merge
        tree.edge(f"Q{i+1}", f"Q{i+2}")
        i += 1

# Adding subgraph for each group of mergeable queries
for merge_group, merge_queries in merged_queries.items():
    with tree.subgraph(name=f'cluster_{merge_group}') as subgraph:
        subgraph.attr(style='dotted', label=merge_group)
        # Add each query as a node in the subgraph
        for merge_query in merge_queries:
            query_idx = queries.index(merge_query) + 1
            subgraph.node(f"Q{query_idx}", merge_query)
        
        # Connect first merged query to the previous node in the main tree
        first_query_idx = queries.index(merge_queries[0]) + 1
        if first_query_idx > 1:
            tree.edge(f"Q{first_query_idx-1}", f"Q{first_query_idx}")
        
        # Connect last merged query to the next unmerged query in the main tree
        last_query_idx = queries.index(merge_queries[-1]) + 1
        if last_query_idx < len(queries):
            tree.edge(f"Q{last_query_idx}", f"Q{last_query_idx + 1}")

# Render and display the tree with subgraphs
tree.render('tree_with_dynamic_edges', view=True)
