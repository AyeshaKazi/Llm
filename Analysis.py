@staticmethod
def _create_rows_produced_heatmap(df):
    # Extract ROWS_PRODUCED from query plan
    df['ROWS_PRODUCED'] = df['PLAN_SUMMARY'].apply(
        lambda x: x.get('estimated_rows', 0) if x else 0
    )
    
    # Group queries by ROWS_PRODUCED
    query_groups = df.groupby('ROWS_PRODUCED')['QUERY_ID'].agg(list).reset_index()
    
    # Create matrix for heatmap
    matrix_data = []
    y_labels = []  # For ROWS_PRODUCED values
    query_ids = []  # For storing query IDs
    
    for _, row in query_groups.iterrows():
        y_labels.append(str(row['ROWS_PRODUCED']))
        queries = row['QUERY_ID']
        matrix_data.append([len(queries)])  # Count of queries
        query_ids.append(queries)
    
    # Create heatmap figure
    fig = go.Figure(data=go.Heatmap(
        z=matrix_data,
        x=['Query Count'],
        y=y_labels,
        colorscale='Viridis',
        text=[[str(val[0])] for val in matrix_data],  # Show count in cells
        texttemplate="%{text}",
        textfont={"size": 12},
        showscale=True,
    ))
    
    fig.update_layout(
        title='Query Distribution by Rows Produced',
        yaxis_title='Rows Produced',
        height=max(350, len(y_labels) * 30),  # Dynamic height based on number of rows
        margin=dict(t=50, l=120)  # Adjust margins for better visibility
    )
    
    return fig, query_ids, y_labels
