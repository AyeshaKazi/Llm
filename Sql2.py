import sqlparse
import networkx as nx
import matplotlib.pyplot as plt
from collections import defaultdict

# Ensure plots display in Jupyter notebook
%matplotlib inline

# Sample SQL queries
queries = [
    "UPDATE customers SET name = 'John' WHERE id = 1",
    "UPDATE customers SET email = 'john@example.com' WHERE id = 1",
    "UPDATE customers SET phone = '123-456-7890' WHERE id = 1",
    "DELETE FROM orders WHERE order_id = 101",
    "INSERT INTO customers (name, email) VALUES ('Jane', 'jane@example.com')",
    "UPDATE orders SET status = 'shipped' WHERE order_id = 100",
    "UPDATE orders SET delivery_date = '2024-09-10' WHERE order_id = 100"
]

# Improved function to extract table and columns
def extract_table_columns(query):
    parsed = sqlparse.parse(query)[0]  # Parse the SQL query
    tokens = parsed.tokens  # Get all tokens in the parsed query

    table = None
    columns = []

    # Flags to track positions in the SQL query
    inside_set_clause = False
    inside_insert_clause = False
    
    for token in tokens:
        token_str = token.value.lower()

        # Extract the table name after 'UPDATE', 'DELETE FROM', or 'INSERT INTO'
        if token_str.startswith('update') or token_str.startswith('delete from'):
            table = token.get_next().value  # Get table name after UPDATE or DELETE
        elif token_str.startswith('insert into'):
            table = token.get_next().value  # Get table name after INSERT INTO

        # Identify columns inside SET clause
        if token_str.startswith('set'):
            inside_set_clause = True
        elif inside_set_clause and token.ttype is None:  # Token type None -> column names in SET
            if token_str != ',':
                columns.append(token_str)
        
        # Identify columns in INSERT INTO clause
        if token_str.startswith('(') and table:
            inside_insert_clause = True
        if inside_insert_clause and token.ttype is None and token_str != ',':
            columns.append(token_str.strip('()'))

    return table, columns

# Create a dependency graph
def create_dependency_graph(queries):
    graph = nx.DiGraph()
    table_to_queries = defaultdict(list)
    
    # Build the graph
    for idx, query in enumerate(queries):
        table, columns = extract_table_columns(query)
        print(f"Query {idx+1}: {query}")
        print(f"Extracted Table: {table}, Columns: {columns}\n")  # Debugging output
        if table:
            # Add node for each query
            graph.add_node(f"Query {idx+1}", query=query)
            
            # Check for potential merges based on table
            for previous_query_idx, previous_query in table_to_queries[table]:
                graph.add_edge(f"Query {previous_query_idx+1}", f"Query {idx+1}")
                
            # Store the query for future merges
            table_to_queries[table].append((idx, query))
    
    return graph

# Visualize the dependency graph and save it to a file
def visualize_dependency_graph(graph, file_name="sql_dependency_graph.png"):
    plt.figure(figsize=(10, 7))
    pos = nx.spring_layout(graph, k=0.5, seed=42)  # Adjust layout for better visualization
    labels = {node: data['query'][:30] for node, data in graph.nodes(data=True)}
    nx.draw(graph, pos, with_labels=True, node_size=2000, node_color="skyblue", font_size=8)
    nx.draw_networkx_labels(graph, pos, labels, font_size=8)
    
    # Save the graph to a file
    plt.savefig(file_name, format="png")
    plt.show()

# Main execution
graph = create_dependency_graph(queries)
visualize_dependency_graph(graph)
