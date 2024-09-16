import sqlparse
import networkx as nx
import matplotlib.pyplot as plt
from collections import defaultdict

# Sample SQL queries
queries = [
    "UPDATE customers SET name = 'John' WHERE id = 1",
    "UPDATE customers SET email = 'john@example.com' WHERE id = 1",
    "UPDATE orders SET status = 'shipped' WHERE order_id = 100",
    "DELETE FROM orders WHERE order_id = 101",
    "INSERT INTO customers (name, email) VALUES ('Jane', 'jane@example.com')"
]

# Function to parse SQL queries and extract tables and columns affected
def extract_table_columns(query):
    parsed = sqlparse.parse(query)
    table = None
    columns = []
    
    for token in parsed[0].tokens:
        if token.ttype is None and token.value.lower() == "update":
            table = token.get_next().value  # Get the table name after 'UPDATE'
        elif token.ttype is None and token.value.lower() in ["set", "insert", "delete"]:
            column_token = token.get_next()
            if column_token:
                columns.append(column_token.value)  # Get columns after 'SET' or 'INSERT'
    
    return table, columns

# Create a dependency graph
def create_dependency_graph(queries):
    graph = nx.DiGraph()
    table_to_queries = defaultdict(list)
    
    # Build the graph
    for idx, query in enumerate(queries):
        table, columns = extract_table_columns(query)
        if table:
            # Add node for each query
            graph.add_node(f"Query {idx+1}", query=query)
            
            # Check for potential merges based on table
            for previous_query_idx, previous_query in table_to_queries[table]:
                graph.add_edge(f"Query {previous_query_idx+1}", f"Query {idx+1}")
                
            # Store the query for future merges
            table_to_queries[table].append((idx, query))
    
    return graph

# Visualize the dependency graph
def visualize_dependency_graph(graph):
    plt.figure(figsize=(10, 7))
    pos = nx.spring_layout(graph)
    labels = {node: data['query'][:30] for node, data in graph.nodes(data=True)}
    nx.draw(graph, pos, with_labels=True, node_size=2000, node_color="skyblue", font_size=8)
    nx.draw_networkx_labels(graph, pos, labels, font_size=8)
    plt.show()

# Main execution
graph = create_dependency_graph(queries)
visualize_dependency_graph(graph)

# You can also analyze the graph and suggest merged queries
def suggest_merged_queries(graph):
    merged_queries = []
    for node in graph.nodes:
        successors = list(graph.successors(node))
        if successors:
            print(f"Query {node} can be merged with {successors}")
            merged_queries.append((node, successors))
    
    return merged_queries

merged_queries = suggest_merged_queries(graph)

# Example: Combine SQL queries that update the same table
for query_set in merged_queries:
    queries_to_merge = [graph.nodes[q]['query'] for q in [query_set[0]] + query_set[1]]
    print(f"Possible merged queries:\n{queries_to_merge}")
