import mlflow.pyfunc
import mlflow
from transformers import AutoModelForSequenceClassification, AutoTokenizer
import torch

# Define the PyfuncModel class encapsulating Hugging Face model and tokenizer
class MyHuggingFacePyfuncModel(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        # Load model and tokenizer
        self.model = AutoModelForSequenceClassification.from_pretrained(context.artifacts["model"])
        self.tokenizer = AutoTokenizer.from_pretrained(context.artifacts["tokenizer"])

    def predict(self, context, model_input):
        # Implement predict method using loaded model and tokenizer
        inputs = self.tokenizer(model_input, return_tensors="pt", padding=True, truncation=True)
        with torch.no_grad():
            outputs = self.model(**inputs)
        return outputs.logits.numpy()

# Example usage to log the Hugging Face model and tokenizer
model_name = "distilbert-base-uncased-finetuned-sst-2-english"  # Replace with your model name

# Load the Hugging Face model and tokenizer
model = AutoModelForSequenceClassification.from_pretrained(model_name)
tokenizer = AutoTokenizer.from_pretrained(model_name)

# Save the model and tokenizer locally to use as artifacts
model.save_pretrained("hf_model")
tokenizer.save_pretrained("hf_tokenizer")

# Log the model and tokenizer with MLflow
with mlflow.start_run():
    mlflow.pyfunc.log_model(
        artifact_path="pyfunc_model",
        python_model=MyHuggingFacePyfuncModel(),
        artifacts={"model": "hf_model", "tokenizer": "hf_tokenizer"}
    )
