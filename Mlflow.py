How can I register the model and tokenizer for the above model on to mlflow 


import mlflow.pyfunc
import mlflow
import pickle

# Example PyfuncModel class encapsulating model and tokenizer
class MyPyfuncModel(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        # Load artifacts from context (where they were saved during logging)
        model_path = context.artifacts["model"]
        tokenizer_path = context.artifacts["tokenizer"]

        # Load model and tokenizer using pickle (adjust as per your serialization method)
        with open(model_path, 'rb') as f:
            self.model = pickle.load(f)

        with open(tokenizer_path, 'rb') as f:
            self.tokenizer = pickle.load(f)

    def predict(self, context, model_input):
        # Implement predict method using loaded model and tokenizer
        # Example: return self.model.predict(self.tokenizer.transform(model_input))
        pass  # Replace with actual prediction logic

# Example usage to log the PyfuncModel
model = MyPyfuncModel()

# Serialize and log model and tokenizer with MLflow
with mlflow.start_run():
    mlflow.pyfunc.log_model(
        artifact_path="pyfunc_model",
        python_model=model,
        artifacts={"model": "model.pkl", "tokenizer": "tokenizer.pkl"}
    )

import mlflow.pyfunc
import mlflow

# Example function to load model and tokenizer from pyfunc model
def load_pyfunc_model(run_id):
    # Initialize MLflow client
    client = mlflow.tracking.MlflowClient()

    # Load pyfunc model
    pyfunc_model = mlflow.pyfunc.load_model(f"runs:/{run_id}/pyfunc_model")

    # Access model and tokenizer attributes from the loaded pyfunc model
    model = pyfunc_model.model
    tokenizer = pyfunc_model.tokenizer

    return model, tokenizer

# Example usage: Provide the run_id of the MLflow run where pyfunc model is stored
run_id = "your_run_id_here"
loaded_model, loaded_tokenizer = load_pyfunc_model(run_id)

# Now you can use 'loaded_model' and 'loaded_tokenizer' in your application






