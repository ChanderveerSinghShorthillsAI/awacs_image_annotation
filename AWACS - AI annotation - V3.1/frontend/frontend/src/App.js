import React, { useState, useEffect, useRef } from 'react';
import './App.css';

const API_BASE = 'http://localhost:8000';

// Status badge component
const StatusBadge = ({ status }) => {
  const statusStyles = {
    pending: { bg: '#3d3d3d', color: '#a8a8a8', text: 'Pending' },
    scraping: { bg: '#1a3a5c', color: '#4fc3f7', text: 'Scraping' },
    processing: { bg: '#3d2c5c', color: '#b388ff', text: 'AI Processing' },
    verifying_dually: { bg: '#4d3d1a', color: '#ffb74d', text: 'Verifying Dually' },
    completed: { bg: '#1a4d3c', color: '#4caf50', text: 'Completed' },
    failed: { bg: '#4d1a1a', color: '#ef5350', text: 'Failed' }
  };

  const style = statusStyles[status] || statusStyles.pending;

  return (
    <span
      className="status-badge"
      style={{
        backgroundColor: style.bg,
        color: style.color,
        padding: '6px 16px',
        borderRadius: '20px',
        fontSize: '0.85rem',
        fontWeight: '600',
        letterSpacing: '0.5px'
      }}
    >
      {style.text}
    </span>
  );
};

// Tab component for switching between modes
const TabSwitcher = ({ activeTab, onTabChange }) => (
  <div className="tab-switcher">
    <button
      className={`tab-btn ${activeTab === 'annotation' ? 'active' : ''}`}
      onClick={() => onTabChange('annotation')}
    >
      <span className="tab-icon">🤖</span>
      AI Annotation
    </button>
    <button
      className={`tab-btn ${activeTab === 'audit' ? 'active' : ''}`}
      onClick={() => onTabChange('audit')}
    >
      <span className="tab-icon">📊</span>
      Accuracy Audit
    </button>
  </div>
);

// File upload component with dual mode (Upload & Scrape OR Reannotate)
const FileUpload = ({ onUpload, onReannotate, isUploading }) => {
  const [dragActive, setDragActive] = useState(false);
  const [uploadMode, setUploadMode] = useState('upload'); // 'upload' or 'reannotate'
  const fileInputRef = useRef(null);

  const handleDrag = (e) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === 'dragenter' || e.type === 'dragover') {
      setDragActive(true);
    } else if (e.type === 'dragleave') {
      setDragActive(false);
    }
  };

  const handleDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);

    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      const handler = uploadMode === 'reannotate' ? onReannotate : onUpload;
      handler(e.dataTransfer.files[0]);
    }
  };

  const handleChange = (e) => {
    if (e.target.files && e.target.files[0]) {
      const handler = uploadMode === 'reannotate' ? onReannotate : onUpload;
      handler(e.target.files[0]);
    }
  };

  const handleClick = (e) => {
    // Don't trigger file input if clicking on mode buttons
    if (e.target.closest('.upload-mode-switcher')) {
      return;
    }
    fileInputRef.current?.click();
  };

  return (
    <div className="file-upload-container">
      {/* Mode switcher */}
      <div className="upload-mode-switcher">
        <button
          className={`mode-btn ${uploadMode === 'upload' ? 'active' : ''}`}
          onClick={() => setUploadMode('upload')}
        >
          <span>📤</span> Upload & Scrape
        </button>
        <button
          className={`mode-btn ${uploadMode === 'reannotate' ? 'active' : ''}`}
          onClick={() => setUploadMode('reannotate')}
        >
          <span>🔄</span> Reannotate Only
        </button>
      </div>

      {/* Upload area */}
      <div
        className={`file-upload ${dragActive ? 'drag-active' : ''} ${isUploading ? 'uploading' : ''}`}
        onDragEnter={handleDrag}
        onDragLeave={handleDrag}
        onDragOver={handleDrag}
        onDrop={handleDrop}
        onClick={handleClick}
      >
        <input
          ref={fileInputRef}
          type="file"
          accept=".xlsx,.xls"
          onChange={handleChange}
          style={{ display: 'none' }}
        />

        <div className="upload-icon">
          {isUploading ? (
            <div className="spinner" />
          ) : (
            <svg width="64" height="64" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5">
              <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
              <polyline points="17 8 12 3 7 8" />
              <line x1="12" y1="3" x2="12" y2="15" />
            </svg>
          )}
        </div>

        <h3>{isUploading ? 'Processing...' : 'Drop Excel File Here'}</h3>
        <p>or click to browse</p>
        {uploadMode === 'upload' ? (
          <span className="file-hint">📤 Will scrape breadcrumbs & images, then run AI annotation</span>
        ) : (
          <span className="file-hint">🔄 Will skip scraping and re-run AI annotation on existing data</span>
        )}
      </div>
    </div>
  );
};

// Stats card component
const StatsCard = ({ title, value, icon, color }) => (
  <div className="stats-card" style={{ borderColor: color }}>
    <div className="stats-icon" style={{ color }}>{icon}</div>
    <div className="stats-content">
      <h4>{title}</h4>
      <div className="stats-value">{value}</div>
    </div>
  </div>
);

// Audit file upload component - for single file with label
const AuditFileUpload = ({ label, description, file, onFileSelect, icon }) => {
  const fileInputRef = useRef(null);
  const [dragActive, setDragActive] = useState(false);

  const handleDrag = (e) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === 'dragenter' || e.type === 'dragover') {
      setDragActive(true);
    } else if (e.type === 'dragleave') {
      setDragActive(false);
    }
  };

  const handleDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      onFileSelect(e.dataTransfer.files[0]);
    }
  };

  const handleChange = (e) => {
    if (e.target.files && e.target.files[0]) {
      onFileSelect(e.target.files[0]);
    }
  };

  const handleRemove = (e) => {
    e.stopPropagation();
    onFileSelect(null);
  };

  return (
    <div
      className={`audit-file-upload ${dragActive ? 'drag-active' : ''} ${file ? 'has-file' : ''}`}
      onDragEnter={handleDrag}
      onDragLeave={handleDrag}
      onDragOver={handleDrag}
      onDrop={handleDrop}
      onClick={() => !file && fileInputRef.current?.click()}
    >
      <input
        ref={fileInputRef}
        type="file"
        accept=".xlsx,.xls"
        onChange={handleChange}
        style={{ display: 'none' }}
      />

      <div className="audit-file-icon">{icon}</div>
      <div className="audit-file-content">
        <h4>{label}</h4>
        <p>{description}</p>
        {file ? (
          <div className="selected-file">
            <span className="file-name">📎 {file.name}</span>
            <button className="remove-file-btn" onClick={handleRemove}>✕</button>
          </div>
        ) : (
          <span className="file-hint">Drop file here or click to browse</span>
        )}
      </div>
    </div>
  );
};

// Audit Section Component
const AuditSection = () => {
  const [aiFile, setAiFile] = useState(null);
  const [manualFile, setManualFile] = useState(null);
  const [isRunning, setIsRunning] = useState(false);
  const [auditResult, setAuditResult] = useState(null);
  const [error, setError] = useState(null);

  const canRunAudit = aiFile && manualFile && !isRunning;

  const handleRunAudit = async () => {
    if (!canRunAudit) return;

    setIsRunning(true);
    setError(null);
    setAuditResult(null);

    try {
      const formData = new FormData();
      formData.append('ai_file', aiFile);
      formData.append('manual_file', manualFile);

      const res = await fetch(`${API_BASE}/api/audit`, {
        method: 'POST',
        body: formData
      });

      if (!res.ok) {
        const errData = await res.json();
        throw new Error(errData.detail || 'Audit failed');
      }

      const data = await res.json();
      setAuditResult(data);

    } catch (err) {
      setError(err.message);
    } finally {
      setIsRunning(false);
    }
  };

  const handleDownload = () => {
    if (auditResult?.audit_id) {
      window.open(`${API_BASE}/api/audit/${auditResult.audit_id}/download`, '_blank');
    }
  };

  const handleReset = () => {
    setAiFile(null);
    setManualFile(null);
    setAuditResult(null);
    setError(null);
  };

  return (
    <div className="audit-section">
      {/* File upload area */}
      {!auditResult && (
        <>
          <div className="audit-uploads">
            <AuditFileUpload
              label="AI Annotated File"
              description="Upload the Excel file generated by AI annotation"
              file={aiFile}
              onFileSelect={setAiFile}
              icon="🤖"
            />
            <div className="audit-arrow">
              <span>VS</span>
            </div>
            <AuditFileUpload
              label="Manual Feedback File"
              description="Upload the Excel file with manual categories from data team"
              file={manualFile}
              onFileSelect={setManualFile}
              icon="👥"
            />
          </div>

          {/* Run button */}
          <div className="audit-actions">
            <button
              className={`btn btn-audit ${canRunAudit ? '' : 'disabled'}`}
              onClick={handleRunAudit}
              disabled={!canRunAudit}
            >
              {isRunning ? (
                <>
                  <div className="btn-spinner"></div>
                  Running Audit...
                </>
              ) : (
                <>
                  <span>📊</span> Run Accuracy Audit
                </>
              )}
            </button>
          </div>

          {/* Error message */}
          {error && (
            <div className="audit-error">
              <span className="error-icon">❌</span>
              <span>{error}</span>
            </div>
          )}
        </>
      )}

      {/* Results */}
      {auditResult && (
        <div className="audit-results">
          <div className="audit-result-header">
            <div className="result-icon">✅</div>
            <h3>Audit Complete!</h3>
          </div>

          {/* Accuracy Stats */}
          <div className="accuracy-stats">
            <div className="accuracy-card main">
              <div className="accuracy-value">
                {auditResult.summary.active_accuracy}%
              </div>
              <div className="accuracy-label">Active Accuracy</div>
              <div className="accuracy-hint">(Excluding inactive ads)</div>
            </div>
            <div className="accuracy-card">
              <div className="accuracy-value secondary">
                {auditResult.summary.global_accuracy}%
              </div>
              <div className="accuracy-label">Global Accuracy</div>
              <div className="accuracy-hint">(Including all ads)</div>
            </div>
          </div>

          {/* Detailed Stats Grid */}
          <div className="audit-stats-grid">
            <div className="audit-stat-item">
              <span className="stat-number">{auditResult.summary.total_audited}</span>
              <span className="stat-label">Total Audited</span>
            </div>
            <div className="audit-stat-item">
              <span className="stat-number green">{auditResult.summary.total_accepted}</span>
              <span className="stat-label">Accepted</span>
            </div>
            <div className="audit-stat-item">
              <span className="stat-number red">{auditResult.summary.total_rejected}</span>
              <span className="stat-label">Rejected</span>
            </div>
            <div className="audit-stat-item">
              <span className="stat-number gray">{auditResult.summary.total_inactive}</span>
              <span className="stat-label">Inactive</span>
            </div>
          </div>

          {/* File comparison info */}
          <div className="file-comparison-info">
            <div className="comparison-item">
              <span className="comparison-label">AI File:</span>
              <span className="comparison-value">{auditResult.ai_file}</span>
              <span className="comparison-count">({auditResult.stats.ai_file_total_ads} ads)</span>
            </div>
            <div className="comparison-item">
              <span className="comparison-label">Manual File:</span>
              <span className="comparison-value">{auditResult.manual_file}</span>
              <span className="comparison-count">({auditResult.stats.manual_file_total_ads} ads)</span>
            </div>
            <div className="comparison-item highlight">
              <span className="comparison-label">Matching Ads Compared:</span>
              <span className="comparison-value">{auditResult.stats.matching_ads_compared}</span>
            </div>
          </div>

          {/* Action buttons */}
          <div className="audit-result-actions">
            <button className="btn btn-success" onClick={handleDownload}>
              <span>📥</span> Download Audit Report
            </button>
            <button className="btn btn-secondary" onClick={handleReset}>
              <span>🔄</span> New Audit
            </button>
          </div>
        </div>
      )}
    </div>
  );
};

// Main App component
function App() {
  const [activeTab, setActiveTab] = useState('annotation');
  const [job, setJob] = useState(null);
  const [isUploading, setIsUploading] = useState(false);
  const [config, setConfig] = useState(null);
  const pollIntervalRef = useRef(null);

  // Fetch config on mount
  useEffect(() => {
    fetch(`${API_BASE}/api/config`)
      .then(res => res.json())
      .then(data => setConfig(data))
      .catch(err => console.error('Failed to fetch config:', err));
  }, []);

  // Poll for job updates
  useEffect(() => {
    // Poll during scraping, processing, AND dually verification phases
    if (job?.status === 'scraping' || job?.status === 'processing' || job?.status === 'verifying_dually') {
      pollIntervalRef.current = setInterval(async () => {
        try {
          const res = await fetch(`${API_BASE}/api/jobs/${job.id}`);
          const data = await res.json();
          setJob(prev => ({ ...prev, ...data }));

          // Stop polling if completed or failed
          if (data.status === 'completed' || data.status === 'failed') {
            clearInterval(pollIntervalRef.current);
          }
        } catch (err) {
          console.error('Failed to poll job status:', err);
        }
      }, 3000);
    }

    return () => {
      if (pollIntervalRef.current) {
        clearInterval(pollIntervalRef.current);
      }
    };
  }, [job?.status, job?.id]);

  // Handle file upload
  const handleUpload = async (file) => {
    setIsUploading(true);

    try {
      const formData = new FormData();
      formData.append('file', file);

      const res = await fetch(`${API_BASE}/api/upload`, {
        method: 'POST',
        body: formData
      });

      if (!res.ok) {
        const error = await res.json();
        throw new Error(error.detail || 'Upload failed');
      }

      const data = await res.json();
      setJob({
        id: data.job_id,
        filename: data.filename,
        adCount: data.ad_count,
        status: data.status
      });

    } catch (err) {
      alert(`Upload failed: ${err.message}`);
    } finally {
      setIsUploading(false);
    }
  };

  // Handle reannotation (skip scraping, go directly to AI annotation)
  const handleReannotate = async (file) => {
    setIsUploading(true);

    try {
      const formData = new FormData();
      formData.append('file', file);

      const res = await fetch(`${API_BASE}/api/reannotate`, {
        method: 'POST',
        body: formData
      });

      if (!res.ok) {
        const error = await res.json();
        throw new Error(error.detail || 'Reannotation failed');
      }

      const data = await res.json();
      setJob({
        id: data.job_id,
        filename: data.filename,
        adCount: data.ad_count,
        status: data.status
      });

    } catch (err) {
      alert(`Reannotation failed: ${err.message}`);
    } finally {
      setIsUploading(false);
    }
  };

  // Start processing
  const handleStartProcessing = async () => {
    if (!job?.id) return;

    try {
      const res = await fetch(`${API_BASE}/api/jobs/${job.id}/start`, {
        method: 'POST'
      });

      if (!res.ok) {
        const error = await res.json();
        throw new Error(error.detail || 'Failed to start');
      }

      const data = await res.json();
      setJob(prev => ({ ...prev, status: data.status }));

    } catch (err) {
      alert(`Failed to start: ${err.message}`);
    }
  };

  // Download result
  const handleDownload = () => {
    if (job?.id && job?.status === 'completed') {
      window.open(`${API_BASE}/api/jobs/${job.id}/download`, '_blank');
    }
  };

  // Reset to upload new file
  const handleReset = () => {
    setJob(null);
  };

  return (
    <div className="app">
      {/* Animated background */}
      <div className="bg-animation">
        <video
          className="bg-video"
          autoPlay
          loop
          muted
          playsInline
        >
          <source src="/videos/Truck_Video_From_Front_POV.mp4" type="video/mp4" />
        </video>
        <div className="bg-gradient"></div>
        <div className="bg-grid"></div>
      </div>

      {/* Header */}
      <header className="header">
        <div className="header-content">
          <div className="logo">
            <span className="logo-icon">⚡</span>
            <h1>AWACS</h1>
            <span className="logo-subtitle">AI Annotation System</span>
          </div>
          {config && (
            <div className="header-stats">
              <span className="stat-item">
                <span className="stat-icon">🔑</span>
                {config.api_keys_count} API Keys
              </span>
              <span className="stat-item">
                <span className="stat-icon">🤖</span>
                {config.model}
              </span>
            </div>
          )}
        </div>
      </header>

      {/* Main content */}
      <main className="main-content">
        <div className="container">
          {/* Tab Switcher */}
          <TabSwitcher activeTab={activeTab} onTabChange={setActiveTab} />

          {/* ANNOTATION TAB */}
          {activeTab === 'annotation' && (
            <>
              {/* Title section */}
              <div className="section-header">
                <h2>Scraper → Auto Parallel AI</h2>
                <p className="mode-badge">High Accuracy Mode • No Vision v2</p>
              </div>

              {/* Upload section (shown when no job) */}
              {!job && (
                <div className="upload-section">
                  <FileUpload
                    onUpload={handleUpload}
                    onReannotate={handleReannotate}
                    isUploading={isUploading}
                  />
                </div>
              )}

              {/* Job dashboard (shown when job exists) */}
              {job && (
                <div className="job-dashboard">
                  {/* Job info header */}
                  <div className="job-header">
                    <div className="job-info">
                      <span className="job-filename">{job.filename}</span>
                      <StatusBadge status={job.status} />
                    </div>
                    <div className="job-actions">
                      {job.status === 'pending' && (
                        <button className="btn btn-primary" onClick={handleStartProcessing}>
                          <span>🚀</span> Start Processing
                        </button>
                      )}
                      {job.status === 'completed' && (
                        <>
                          <button className="btn btn-success" onClick={handleDownload}>
                            <span>📥</span> Download Result
                          </button>
                          <button className="btn btn-secondary" onClick={handleReset}>
                            <span>🔄</span> New Job
                          </button>
                        </>
                      )}
                      {job.status === 'failed' && (
                        <button className="btn btn-secondary" onClick={handleReset}>
                          <span>🔄</span> Try Again
                        </button>
                      )}
                    </div>
                  </div>

                  {/* Stats cards */}
                  <div className="stats-grid">
                    <StatsCard
                      title="Total Ads"
                      value={job.adCount || job.total_ads || 0}
                      icon="📊"
                      color="#4fc3f7"
                    />
                    <StatsCard
                      title="Phase"
                      value={
                        job.status === 'scraping' ? 'Scraping'
                          : job.status === 'processing' ? 'AI'
                            : job.status === 'verifying_dually' ? 'Verifying'
                              : job.status
                      }
                      icon="⚙️"
                      color="#b388ff"
                    />
                    <StatsCard
                      title="Job ID"
                      value={job.id}
                      icon="🔖"
                      color="#4caf50"
                    />
                  </div>

                  {/* Processing indicator */}
                  {(job.status === 'scraping' || job.status === 'processing' || job.status === 'verifying_dually') && (
                    <div className="processing-card">
                      <div className="processing-spinner"></div>
                      <h3>
                        {job.status === 'scraping'
                          ? '🌐 Scraping Ads...'
                          : job.status === 'verifying_dually'
                            ? '🔍 Verifying Dually Detections...'
                            : '🤖 AI Classification in Progress...'}
                      </h3>
                      <p>
                        {job.status === 'scraping'
                          ? 'Extracting breadcrumbs and images from Commercial Truck Trader...'
                          : job.status === 'verifying_dually'
                            ? `Double-checking Dually annotations using LLM verification (${job.dually_verification?.verified || 0}/${job.dually_verification?.total || 0})...`
                            : 'Classifying vehicles using Gemini AI...'}
                      </p>
                      <span className="processing-hint">
                        {job.status === 'verifying_dually'
                          ? 'Reducing false positives. Almost done!'
                          : 'This may take a few minutes. Please wait...'}
                      </span>
                    </div>
                  )}

                  {/* Completion card */}
                  {job.status === 'completed' && (
                    <div className="completion-card">
                      <div className="completion-icon">🎉</div>
                      <h3>Processing Complete!</h3>
                      <p>Your annotated Excel file is ready for download.</p>
                      {job.output_filename && (
                        <span className="output-filename">{job.output_filename}</span>
                      )}
                      {/* Show cost breakdown */}
                      {(job.total_cost > 0 || job.dually_verification_cost > 0) && (
                        <div className="cost-breakdown" style={{ marginTop: '12px', fontSize: '0.9rem', color: '#a8a8a8' }}>
                          💰 Total Cost: {job.total_cost}¢
                          {job.dually_verification_cost > 0 && (
                            <span style={{ marginLeft: '8px', color: '#ffb74d' }}>
                              (includes {job.dually_verification_cost}¢ for Dually verification)
                            </span>
                          )}
                        </div>
                      )}
                      {/* Show dually verification results */}
                      {job.dually_verification?.removed > 0 && (
                        <div className="dually-results" style={{ marginTop: '8px', fontSize: '0.85rem', color: '#4caf50' }}>
                          ✅ Dually Verification: Removed {job.dually_verification.removed} false positive(s)
                        </div>
                      )}
                    </div>
                  )}

                  {/* Error card */}
                  {job.status === 'failed' && (
                    <div className="error-card">
                      <div className="error-icon">❌</div>
                      <h3>Processing Failed</h3>
                      <p>{job.error || 'An unexpected error occurred'}</p>
                    </div>
                  )}
                </div>
              )}

              {/* Info cards */}
              <div className="info-section">
                <div className="info-card">
                  <div className="info-icon">🌐</div>
                  <h4>Web Scraping</h4>
                  <p>Automatically extracts breadcrumb categories and image URLs from Commercial Truck Trader listings.</p>
                </div>
                <div className="info-card">
                  <div className="info-icon">🤖</div>
                  <h4>AI Classification</h4>
                  <p>Uses Gemini AI with high accuracy mode to classify vehicle types from images and metadata.</p>
                </div>
                <div className="info-card">
                  <div className="info-icon">📊</div>
                  <h4>Smart Output</h4>
                  <p>Generates annotated Excel files with confidence scores, status updates, and cost tracking.</p>
                </div>
              </div>
            </>
          )}

          {/* AUDIT TAB */}
          {activeTab === 'audit' && (
            <>
              <div className="section-header">
                <h2>AI Accuracy Audit</h2>
                <p className="mode-badge">Compare AI Output vs Manual Feedback</p>
              </div>

              <AuditSection />

              {/* Info cards for audit */}
              <div className="info-section">
                <div className="info-card">
                  <div className="info-icon">📤</div>
                  <h4>Upload Files</h4>
                  <p>Upload the AI annotated Excel file and the manual feedback file from your data team.</p>
                </div>
                <div className="info-card">
                  <div className="info-icon">🔍</div>
                  <h4>Smart Comparison</h4>
                  <p>Compares categories using normalization rules to handle naming variations accurately.</p>
                </div>
                <div className="info-card">
                  <div className="info-icon">📈</div>
                  <h4>Detailed Report</h4>
                  <p>Get accuracy metrics, mismatch patterns, and a downloadable Excel report.</p>
                </div>
              </div>
            </>
          )}
        </div>
      </main>

      {/* Footer */}
      <footer className="footer">
        <p>AWACS AI Annotation System v3.1 • Powered by Gemini AI</p>
      </footer>
    </div>
  );
}

export default App;
