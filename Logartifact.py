import mlflow
import mlflow.pyfunc
import ctranslate2
import os

class CTranslate2Wrapper(mlflow.pyfunc.PythonModel):

    def load_context(self, context):
        self.model_path = context.artifacts["model_path"]
        self.model = ctranslate2.Generator(self.model_path)

    def predict(self, context, model_input):
        # Implement the prediction logic
        return self.model(model_input)

def save_model(model, path):
    os.makedirs(path, exist_ok=True)
    model.save(path)
    mlflow.pyfunc.save_model(
        path=path,
        python_model=CTranslate2Wrapper(),
        artifacts={"model_path": path},
        conda_env={
            'channels': ['defaults', 'conda-forge'],
            'dependencies': [
                'python=3.8.5',
                'ctranslate2',
                'mlflow',
            ]
        }
    )

# Usage
model = ctranslate2.Generator("path_to_model")
save_model(model, "path_to_save_model")



import mlflow

with mlflow.start_run() as run:
    save_model(model, "path_to_save_model")
    mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=CTranslate2Wrapper(),
        artifacts={"model_path": "path_to_save_model"}
    )


import mlflow
import mlflow.pyfunc
import ctranslate2
import os
import shutil

class CTranslate2Wrapper(mlflow.pyfunc.PythonModel):

    def load_context(self, context):
        self.model_path = context.artifacts["model_path"]
        self.model = ctranslate2.Generator(self.model_path)

    def predict(self, context, model_input):
        # Implement the prediction logic
        return self.model(model_input)

def save_model(model, source_model_path, destination_path):
    os.makedirs(destination_path, exist_ok=True)
    # Copy the model files to the destination path
    for item in os.listdir(source_model_path):
        s = os.path.join(source_model_path, item)
        d = os.path.join(destination_path, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, False, None)
        else:
            shutil.copy2(s, d)
    
    mlflow.pyfunc.save_model(
        path=destination_path,
        python_model=CTranslate2Wrapper(),
        artifacts={"model_path": destination_path},
        conda_env={
            'channels': ['defaults', 'conda-forge'],
            'dependencies': [
                'python=3.8.5',
                'ctranslate2',
                'mlflow',
            ]
        }
    )

# Usage
source_model_path = "path_to_model"  # Directory containing the ctranslate2 model files
destination_path = "path_to_save_model"
save_model(model, source_model_path, destination_path)

