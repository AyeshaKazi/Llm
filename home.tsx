import React, { useState } from "react";
import "./home.css";

const Home: React.FC = () => {
  const [file, setFile] = useState<File | null>(null);
  const [loading, setLoading] = useState(false);
  const [success, setSuccess] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setFile(e.target.files && e.target.files[0] ? e.target.files[0] : null);
    setSuccess(false);
    setError(null);
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setSuccess(false);
    setError(null);

    if (!file) {
      setError("Please select an Excel file.");
      return;
    }

    setLoading(true);
    const formData = new FormData();
    formData.append("file", file);

    try {
      const response = await fetch("http://localhost:5000/convert", {
        method: "POST",
        body: formData,
      });

      if (!response.ok) throw new Error("Conversion failed");

      const contentDisposition = response.headers.get("Content-Disposition");
      let filename = "presentation.pptx";
      if (contentDisposition) {
        const match = contentDisposition.match(/filename="?(.+)"?/);
        if (match) filename = match[1];
      }

      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(url);

      setSuccess(true);
    } catch (err: any) {
      setError("Error: " + err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="excel2ppt-bg">
      <header className="excel2ppt-header">
        <img src="/logo_large.svg" alt="Company Logo" className="header-logo" />
      </header>
      <div className="excel2ppt-container">
        <div className="excel2ppt-card">
          <h1 className="project-title">ROADMAP CONVERTOR</h1>
          <p className="subtitle">
            Upload Excel. If you don't have the Excel format,{" "}
            <a
              href="/template.xlsx"
              download
              className="download-link"
            >
              download from here
            </a>
            .
          </p>
          <form onSubmit={handleSubmit} className="upload-form">
            <label className="file-label">
              <input
                type="file"
                accept=".xls,.xlsx"
                onChange={handleFileChange}
                required
                disabled={loading}
              />
              <span>{file ? file.name : "Choose Excel file"}</span>
            </label>
            <button
              type="submit"
              className="convert-btn"
              disabled={loading || !file}
            >
              {loading ? (
                <span className="loader"></span>
              ) : (
                <b>CONVERT TO PPT AND DOWNLOAD</b>
              )}
            </button>
          </form>
          {success && (
            <div className="success-msg">
              ✅ Your PowerPoint is ready and downloading!
            </div>
          )}
          {error && (
            <div className="error-msg">
              {error}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default Home;
