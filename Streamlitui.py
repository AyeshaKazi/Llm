import streamlit as st
import pandas as pd
import requests
import plotly.express as px

# Set up Streamlit app
st.set_page_config(page_title="Query Merge Optimizer", layout="wide")
st.title("Query Merge Optimizer")
st.write("Upload a CSV file containing `query_id`, `query`, and `execution_time`, and optimize your queries.")

# File uploader
uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])

# Process file
if uploaded_file:
    try:
        # Load CSV into a DataFrame
        df = pd.read_csv(uploaded_file)
        st.write("Uploaded Data:")
        st.dataframe(df)

        # Validate columns
        if set(['query_id', 'query', 'execution_time']).issubset(df.columns):
            # Call API to get merge recommendations
            api_url = st.text_input("Enter the API URL to fetch merge recommendations:")
            if api_url and st.button("Optimize Queries"):
                try:
                    response = requests.post(api_url, json=df.to_dict(orient="records"))
                    if response.status_code == 200:
                        merge_recommendations = response.json()
                        merged_queries = merge_recommendations.get("merged_queries", [])
                        time_saved = merge_recommendations.get("time_saved", 0)

                        # Display merged queries
                        st.subheader("Merged Queries:")
                        st.write(merged_queries)

                        # Visualization
                        original_total_time = df["execution_time"].sum()
                        optimized_total_time = original_total_time - time_saved

                        st.subheader("Time Savings Visualization:")
                        fig = px.bar(
                            x=["Original Execution Time", "Optimized Execution Time"],
                            y=[original_total_time, optimized_total_time],
                            labels={"x": "Execution Scenario", "y": "Total Time (seconds)"},
                            title="Execution Time Comparison"
                        )
                        st.plotly_chart(fig)

                        st.write(f"**Time Saved:** {time_saved:.2f} seconds")
                    else:
                        st.error(f"API call failed with status code {response.status_code}: {response.text}")
                except Exception as e:
                    st.error(f"Error calling API: {e}")
        else:
            st.error("The uploaded file must contain `query_id`, `query`, and `execution_time` columns.")
    except Exception as e:
        st.error(f"Error processing file: {e}")
else:
    st.info("Please upload a CSV file to proceed.")
