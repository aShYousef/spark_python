import { useState } from "react";

export default function App() {
  const [file, setFile] = useState(null);
  const [results, setResults] = useState([]);

  const upload = async () => {
    const form = new FormData();
    form.append("file", file);

    const res = await fetch("http://localhost:8000/upload", {
      method: "POST",
      body: form,
    });
    return res.json();
  };

  const runJobs = async (path) => {
    const res = await fetch("http://localhost:8000/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        file_path: path,
        tasks: [
          "descriptive_stats",
          "linear_regression",
          "logistic_regression",
          "kmeans",
          "fpgrowth",
        ],
        workers: [1, 2, 4, 8],
      }),
    });
    const data = await res.json();
    setResults(data.results);
  };

  return (
    <div style={{ padding: 20 }}>
      <h1>Spark Data Processor</h1>

      <input type="file" onChange={(e) => setFile(e.target.files[0])} />
      <br /><br />

      <button
        onClick={async () => {
          const res = await upload();
          await runJobs(res.path);
        }}
      >
        Start Processing
      </button>

      <h2>Results</h2>
      <pre>{JSON.stringify(results, null, 2)}</pre>
    </div>
  );
}

