import React, { useState } from "react";

const Home: React.FC = () => {
  const [file, setFile] = useState<File | null>(null);
  const [loading, setLoading] = useState(false);

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files.length > 0) {
      setFile(e.target.files[0]);
    }
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!file) {
      alert("Please select an Excel file.");
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
    } catch (err: any) {
      alert("Error: " + err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ maxWidth: 400, margin: "auto", padding: 20 }}>
      <h2>Excel to PowerPoint Converter</h2>
      <form onSubmit={handleSubmit}>
        <input
          type="file"
          accept=".xls,.xlsx"
          onChange={handleFileChange}
          required
        />
        <br />
        <button type="submit" disabled={loading}>
          {loading ? "Converting..." : "Convert to PPT"}
        </button>
      </form>
    </div>
  );
};

export default Home;
