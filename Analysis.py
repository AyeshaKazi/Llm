# Modified display_query_summary method in ui_components.py
@staticmethod
def display_query_summary(summary):
    if not summary:
        st.warning("No data available for analysis")
        return

    # Basic Statistics
    st.write("### Query Statistics")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Queries", summary['total_queries'])
    with col2:
        st.metric("Avg Duration (sec)", f"{summary['avg_duration']:.2f}")
    with col3:
        st.metric("95th Percentile (sec)", f"{summary['performance_metrics']['p95_duration']:.2f}")
    with col4:
        st.metric("Max Duration (sec)", f"{summary['performance_metrics']['max_duration']:.2f}")

    # Query Type Distribution
    st.write("### Query Type Distribution")
    st.plotly_chart(summary['visualizations']['query_type_pie'], use_container_width=True)

    # Duration Distribution
    st.write("### Query Duration Distribution")
    st.plotly_chart(summary['visualizations']['duration_histogram'], use_container_width=True)

    # Similar Queries Analysis
    st.write("### Similar Queries Analysis")
    st.plotly_chart(summary['visualizations']['similar_queries_scatter'], use_container_width=True)

    # Detailed Similar Queries Table
    if not summary['similar_queries'].empty:
        st.write("### Detailed Similar Queries")
        for _, row in summary['similar_queries'].iterrows():
            with st.expander(f"Queries updating {row['ROWS_UPDATED']} rows"):
                for query in row['QUERY_TEXT']:
                    st.code(query, language="sql")
                st.write(f"Average Duration: {row['DURATION_SECONDS']:.2f} seconds")
            
