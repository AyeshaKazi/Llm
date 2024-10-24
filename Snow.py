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

# Connect nodes sequentially to show the flow of execution
for i in range(len(queries) - 1):
    tree.edge(f"Q{i+1}", f"Q{i+2}")  # Sequential execution

# Define which queries are mergeable and create subgraphs for them
merged_queries = {
    'Merge 1': ['Query 2', 'Query 5'],  # Example of mergeable queries
    'Merge 2': ['Query 7', 'Query 10']
}

# Adding subgraph for each group of mergeable queries
for merge_group, merge_queries in merged_queries.items():
    with tree.subgraph(name=f'cluster_{merge_group}') as subgraph:
        subgraph.attr(style='dotted', label=merge_group)
        # Add each query as a node in the subgraph
        for merge_query in merge_queries:
            query_idx = queries.index(merge_query) + 1
            subgraph.node(f"Q{query_idx}", merge_query)
            # Connect first merged query to the main tree
            if merge_query == merge_queries[0]:
                tree.edge(f"Q{query_idx-1}", f"Q{query_idx}")  # Connect the previous query to the merge node
            # Connect last merged query to the next query in the tree
            if merge_query == merge_queries[-1]:
                next_query_idx = queries.index(merge_queries[-1]) + 2
                if next_query_idx <= len(queries):
                    tree.edge(f"Q{query_idx}", f"Q{next_query_idx}")

# Render and display the tree with subgraphs
tree.render('tree_with_merged_queries', view=True)
