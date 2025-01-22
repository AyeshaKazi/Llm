summary['visualizations']['rows_produced_heatmap'], summary['query_groups'], summary['rows_values'] = DataProcessor._create_rows_produced_heatmap(df)

st.write("### Query Distribution by Rows Produced")
st.plotly_chart(summary['visualizations']['rows_produced_heatmap'], use_container_width=True)

# Show queries for each group
st.write("### Query Groups")
for row_value, queries in zip(summary['rows_values'], summary['query_groups']):
    with st.expander(f"Queries producing {row_value} rows ({len(queries)} queries)"):
        for query_id in queries:
            query_text = df[df['QUERY_ID'] == query_id]['QUERY_TEXT'].iloc[0]
            st.code(f"Query ID: {query_id}\n{query_text}", language="sql")
