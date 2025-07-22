from flask import Flask, request, send_file
from flask_cors import CORS
import tempfile
import os
from converter import convert_excel_to_ppt  # Import your function

app = Flask(__name__)
CORS(app)

@app.route('/convert', methods=['POST'])
def convert():
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as temp_excel:
        file = request.files['file']
        file.save(temp_excel.name)
        temp_excel_path = temp_excel.name

    with tempfile.NamedTemporaryFile(suffix=".pptx", delete=False) as temp_pptx:
        temp_pptx_path = temp_pptx.name

    convert_excel_to_ppt(temp_excel_path, temp_pptx_path)

    response = send_file(
        temp_pptx_path,
        as_attachment=True,
        download_name="presentation.pptx"
    )

    @response.call_on_close
    def cleanup():
        os.remove(temp_excel_path)
        os.remove(temp_pptx_path)

    return response

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
