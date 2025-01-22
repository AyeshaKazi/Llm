# Modified generate_query_summary method in data_processor.py
@staticmethod
def generate_query_summary(df):
    if df.empty:
        return {}

    # Convert duration to seconds
    df['DURATION_SECONDS'] = df['TOTAL_ELAPSED_TIME'] / 1000

    # Extract rows_updated from query plan
    df['ROWS_UPDATED'] = df['PLAN_SUMMARY'].apply(
        lambda x: x.get('estimated_rows', 0) if x else 0
    )

    # Group queries by rows_updated
    rows_updated_groups = df.groupby('ROWS_UPDATED').agg({
        'QUERY_ID': list,
        'QUERY_TEXT': list,
        'DURATION_SECONDS': 'mean'
    }).reset_index()

    # Filter for groups with multiple queries
    similar_queries = rows_updated_groups[rows_updated_groups['QUERY_ID'].str.len() > 1]

    summary = {
        'total_queries': len(df),
        'avg_duration': df['DURATION_SECONDS'].mean(),
        'query_type_distribution': df['QUERY_TYPE'].value_counts(),
        'performance_metrics': {
            'min_duration': df['DURATION_SECONDS'].min(),
            'max_duration': df['DURATION_SECONDS'].max(),
            'median_duration': df['DURATION_SECONDS'].median(),
            'p95_duration': df['DURATION_SECONDS'].quantile(0.95),
        }
    }

    # Generate visualizations
    summary['visualizations'] = {
        'query_type_pie': px.pie(
            values=summary['query_type_distribution'].values,
            names=summary['query_type_distribution'].index,
            title='Query Type Distribution'
        ),

        'duration_histogram': px.histogram(
            df,
            x='DURATION_SECONDS',
            title='Query Duration Distribution (seconds)',
            labels={'DURATION_SECONDS': 'Duration (seconds)'},
            nbins=30,
            range_x=[0, df['DURATION_SECONDS'].max()]  # Start from 0
        ),

        'similar_queries_scatter': px.scatter(
            similar_queries,
            x='ROWS_UPDATED',
            y='DURATION_SECONDS',
            size=[len(x) for x in similar_queries['QUERY_ID']],  # Bubble size based on number of similar queries
            hover_data={
                'ROWS_UPDATED': True,
                'DURATION_SECONDS': ':.2f',
                'QUERY_TEXT': True
            },
            title='Similar Queries by Rows Updated',
            labels={
                'ROWS_UPDATED': 'Number of Rows Updated',
                'DURATION_SECONDS': 'Average Duration (seconds)'
            }
        )
    }

    # Store similar queries data for detailed view
    summary['similar_queries'] = similar_queries

    return summary
