from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict
import pandas as pd

# Define the app
app = FastAPI()

# Define the input data model
class Query(BaseModel):
    query_id: int
    query: str
    execution_time: float

class MergeResponse(BaseModel):
    merged_queries: List[Dict]
    time_saved: float

@app.post("/optimize", response_model=MergeResponse)
async def optimize_queries(queries: List[Query]):
    try:
        # Convert input queries to a DataFrame
        df = pd.DataFrame([query.dict() for query in queries])
        
        # Example merge logic: merge queries with the same prefix (simplistic)
        df['query_prefix'] = df['query'].apply(lambda q: q.split()[0])  # Extract prefix (e.g., "SELECT")
        merged = df.groupby('query_prefix').agg({
            'query_id': list,
            'query': lambda x: " MERGED QUERY ".join(x),
            'execution_time': 'sum'
        }).reset_index()

        # Calculate potential time saved
        original_time = df['execution_time'].sum()
        optimized_time = merged['execution_time'].sum()
        time_saved = original_time - optimized_time

        # Prepare response
        response = {
            "merged_queries": [
                {
                    "merged_query_id": i + 1,
                    "queries": row['query_id'],
                    "merged_query": row['query']
                }
                for i, row in merged.iterrows()
            ],
            "time_saved": time_saved
        }
        return response

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing queries: {e}")
