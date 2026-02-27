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
      className={`tab-btn ${activeTab === 'dbfetch' ? 'active' : ''}`}
      onClick={() => onTabChange('dbfetch')}
    >
      <span className="tab-icon">🗄️</span>
      DB Fetch
    </button>
    <button
      className={`tab-btn ${activeTab === 'audit' ? 'active' : ''}`}
      onClick={() => onTabChange('audit')}
    >
      <span className="tab-icon">📊</span>
      Accuracy Audit
    </button>
    <button
      className={`tab-btn ${activeTab === 'dbupdate' ? 'active' : ''}`}
      onClick={() => onTabChange('dbupdate')}
    >
      <span className="tab-icon">🔄</span>
      DB Update
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

// Preview Modal Component
const PreviewModal = ({ fetchResult, onClose, onStartAnnotation, isStarting }) => {
  if (!fetchResult) return null;

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-content" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h2>📊 Fetched Data Preview</h2>
          <button className="modal-close" onClick={onClose}>✕</button>
        </div>

        <div className="modal-body">
          {/* Summary Stats */}
          <div className="preview-stats">
            <div className="preview-stat">
              <span className="preview-stat-value">{fetchResult.total_trucks}</span>
              <span className="preview-stat-label">Total Trucks</span>
            </div>
            <div className="preview-stat">
              <span className="preview-stat-value">{fetchResult.filename}</span>
              <span className="preview-stat-label">File Name</span>
            </div>
          </div>

          {/* Preview Table */}
          <div className="preview-table-container">
            <h3>First 10 Records Preview:</h3>
            <div className="preview-table-wrapper">
              <table className="preview-table">
                <thead>
                  <tr>
                    <th>Ad ID</th>
                    <th>Breadcrumb Top1</th>
                    <th>Breadcrumb Top2</th>
                    <th>Breadcrumb Top3</th>
                    <th>Images</th>
                  </tr>
                </thead>
                <tbody>
                  {fetchResult.preview_data && fetchResult.preview_data.map((row, idx) => (
                    <tr key={idx}>
                      <td>{row['Ad ID']}</td>
                      <td>{row['Breadcrumb_Top1'] || '-'}</td>
                      <td>{row['Breadcrumb_Top2'] || '-'}</td>
                      <td>{row['Breadcrumb_Top3'] || '-'}</td>
                      <td>{row['Image_URLs'] ? row['Image_URLs'].split(',').length : 0}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Pagination Guidance */}
          {fetchResult.pagination && (
            <div style={{
              marginTop: '1rem',
              padding: '1rem',
              background: fetchResult.pagination.has_more_data
                ? 'rgba(255, 165, 0, 0.15)'
                : 'rgba(50, 205, 50, 0.15)',
              border: `1px solid ${fetchResult.pagination.has_more_data
                ? 'rgba(255, 165, 0, 0.4)'
                : 'rgba(50, 205, 50, 0.4)'}`,
              borderRadius: '8px'
            }}>
              <h3 style={{
                margin: '0 0 0.75rem 0',
                color: fetchResult.pagination.has_more_data ? 'var(--accent-orange)' : 'var(--accent-green)',
                fontSize: '1rem'
              }}>
                📊 Pagination Info
              </h3>

              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.5rem', fontSize: '0.9rem' }}>
                <div>
                  <strong style={{ color: 'var(--accent-green)' }}>First Ad ID:</strong>
                  <span style={{ marginLeft: '0.5rem', fontFamily: 'monospace' }}>{fetchResult.pagination.first_ad_id}</span>
                </div>
                <div>
                  <strong style={{ color: 'var(--accent-orange)' }}>Last Ad ID:</strong>
                  <span style={{ marginLeft: '0.5rem', fontFamily: 'monospace' }}>{fetchResult.pagination.last_ad_id}</span>
                </div>
                <div>
                  <strong>Listing Range:</strong>
                  <span style={{ marginLeft: '0.5rem' }}>{fetchResult.pagination.listing_start} - {fetchResult.pagination.listing_end}</span>
                </div>
                <div>
                  <strong>Total in DB:</strong>
                  <span style={{ marginLeft: '0.5rem' }}>{fetchResult.pagination.total_available}</span>
                </div>
              </div>

              {/* Filter-specific info */}
              {fetchResult.pagination.filters_applied && (
                <div style={{
                  marginTop: '0.75rem',
                  padding: '0.5rem',
                  background: 'rgba(147, 112, 219, 0.2)',
                  border: '1px solid rgba(147, 112, 219, 0.4)',
                  borderRadius: '4px',
                  fontSize: '0.85rem'
                }}>
                  <div style={{ fontWeight: '600', color: '#b388ff', marginBottom: '0.5rem' }}>
                    🔍 Category Filter Applied
                  </div>
                  <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.25rem' }}>
                    <div>
                      <span style={{ color: 'var(--text-muted)' }}>Fetched (unfiltered):</span>
                      <span style={{ marginLeft: '0.5rem', fontWeight: '600' }}>{fetchResult.pagination.fetched_before_filter}</span>
                    </div>
                    <div>
                      <span style={{ color: 'var(--accent-green)' }}>Matched (filtered):</span>
                      <span style={{ marginLeft: '0.5rem', fontWeight: '600' }}>{fetchResult.pagination.matched_after_filter}</span>
                    </div>
                  </div>
                  <div style={{ marginTop: '0.5rem', fontSize: '0.8rem', color: 'var(--text-muted)' }}>
                    Filters: {fetchResult.pagination.category_filters?.slice(0, 2).join(', ')}
                    {fetchResult.pagination.category_filters?.length > 2 && ` +${fetchResult.pagination.category_filters.length - 2} more`}
                  </div>
                </div>
              )}

              {/* Next fetch suggestion */}
              <div style={{
                marginTop: '1rem',
                paddingTop: '0.75rem',
                borderTop: '1px solid rgba(255,255,255,0.1)'
              }}>
                {fetchResult.pagination.has_more_data ? (
                  <div>
                    <strong style={{ color: 'var(--accent-orange)' }}>🔄 Next Batch:</strong>
                    <span style={{ marginLeft: '0.5rem' }}>
                      Start from <strong>{fetchResult.pagination.next_suggested_start}</strong> to <strong>{fetchResult.pagination.next_suggested_end}</strong>
                    </span>
                    <div style={{ marginTop: '0.25rem', fontSize: '0.85rem', color: 'var(--text-muted)' }}>
                      Remaining listings (unfiltered): {fetchResult.pagination.remaining_listings}
                      {fetchResult.pagination.filters_applied && (
                        <span style={{ color: '#b388ff' }}> • Actual matching count will vary with filters</span>
                      )}
                    </div>
                  </div>
                ) : (
                  <div style={{ color: 'var(--accent-green)', fontWeight: '600' }}>
                    ✅ This was the LAST batch! No more listings to fetch for this date range.
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Message */}
          <div className="preview-message">
            <p>✅ Data has been fetched and saved to: <strong>{fetchResult.filename}</strong></p>
            <p>Click "Download Data" to save the fetched data, or "Start Annotation" to begin AI classification.</p>
          </div>
        </div>

        <div className="modal-footer">
          <button className="btn btn-secondary" onClick={onClose} disabled={isStarting}>
            <span>❌</span> Cancel
          </button>
          <button
            className="btn btn-download"
            onClick={() => {
              const fetchId = fetchResult.fetch_id;
              if (fetchId) {
                window.open(`${API_BASE}/api/db-fetch/${fetchId}/download`, '_blank');
              }
            }}
            style={{
              background: 'linear-gradient(135deg, #2196F3, #1976D2)',
              color: '#fff',
              border: 'none',
              padding: '0.75rem 1.5rem',
              borderRadius: '8px',
              cursor: 'pointer',
              fontWeight: '600',
              fontSize: '0.95rem',
              display: 'flex',
              alignItems: 'center',
              gap: '0.5rem',
              transition: 'all 0.2s ease',
              boxShadow: '0 2px 8px rgba(33, 150, 243, 0.3)'
            }}
          >
            <span>📥</span> Download Data
          </button>
          <button
            className="btn btn-primary"
            onClick={onStartAnnotation}
            disabled={isStarting}
          >
            {isStarting ? (
              <>
                <div className="btn-spinner"></div>
                Starting...
              </>
            ) : (
              <>
                <span>🤖</span> Start Annotation
              </>
            )}
          </button>
        </div>
      </div>
    </div>
  );
};

// DB Fetch Section Component
const DBFetchSection = ({ onJobCreated }) => {
  const [fetchMode, setFetchMode] = useState('daterange'); // 'daterange' or 'adids'
  const [clientId, setClientId] = useState('');
  const [clientSecret, setClientSecret] = useState('');
  const [grantType, setGrantType] = useState('client_credentials');
  const [fromTimestamp, setFromTimestamp] = useState('');
  const [toTimestamp, setToTimestamp] = useState('');
  const [listingStart, setListingStart] = useState('0');
  const [listingEnd, setListingEnd] = useState('1000');
  const [isFetching, setIsFetching] = useState(false);
  const [fetchResult, setFetchResult] = useState(null);
  const [showPreview, setShowPreview] = useState(false);
  const [isStartingAnnotation, setIsStartingAnnotation] = useState(false);
  const [error, setError] = useState(null);
  const [credentialsFromConfig, setCredentialsFromConfig] = useState(false);
  const [selectedCategories, setSelectedCategories] = useState([]);

  // State for Ad IDs mode
  const [uploadedFile, setUploadedFile] = useState(null);
  const [isUploadingIds, setIsUploadingIds] = useState(false);

  // Available categories for filtering
  const AVAILABLE_CATEGORIES = [
    'Pickup Truck',
    'Cab-Chassis',
    'Flatbed Truck',
    'Dump Truck',
    'Box Truck - Straight Truck',
    'Utility Truck - Service Truck',
    'Bucket Truck - Boom Truck',
    'Conventional - Day Cab',
    'Conventional - Sleeper Truck',
    'Reefer/Refrigerated Truck',
    'Tanker Truck',
    'Crane Truck',
    'Rollback Tow Truck',
    'Wrecker Tow Truck',
    'Mechanics Truck',
    'Cutaway-Cube Van',
    'Cargo Van',
    'Stepvan',
    'Garbage Truck',
    'Mixer Truck - Concrete Truck',
    'Flatbed Dump',
    'Farm Truck - Grain Truck',
    'Fuel Truck - Lube Truck',
    'Vacuum Truck',
    'Water Truck',
    'Grapple Truck',
    'Knucklebooms',
    'Chipper Truck',
    'Digger Derrick'
  ];

  // Load DB API credentials from config on mount
  useEffect(() => {
    const loadConfig = async () => {
      try {
        const res = await fetch(`${API_BASE}/api/config`);
        const data = await res.json();

        if (data.db_api_configured) {
          setClientId(data.db_api_client_id);
          setGrantType(data.db_api_grant_type);
          setCredentialsFromConfig(true);
          console.log('✅ DB API credentials loaded from config.ini');
        }
      } catch (err) {
        console.error('Failed to load config:', err);
      }
    };

    loadConfig();
  }, []);

  // Helper to convert date to Unix timestamp (using UTC to avoid timezone issues)
  const dateToTimestamp = (dateString) => {
    if (!dateString) return '';
    // Parse as UTC to avoid local timezone offset issues
    return Math.floor(Date.parse(dateString + 'T00:00:00Z') / 1000).toString();
  };

  // Helper to convert Unix timestamp to date (returns UTC date)
  const timestampToDate = (timestamp) => {
    if (!timestamp) return '';
    const date = new Date(parseInt(timestamp) * 1000);
    // Return UTC date to match the dateToTimestamp function
    return date.toISOString().split('T')[0];
  };

  // Helper to convert Unix timestamp to detailed datetime string with hours:minutes:seconds
  const timestampToDetailedDateTime = (timestamp) => {
    if (!timestamp) return '';
    const date = new Date(parseInt(timestamp) * 1000);
    // Format: YYYY-MM-DD HH:MM:SS UTC
    const year = date.getUTCFullYear();
    const month = String(date.getUTCMonth() + 1).padStart(2, '0');
    const day = String(date.getUTCDate()).padStart(2, '0');
    const hours = String(date.getUTCHours()).padStart(2, '0');
    const minutes = String(date.getUTCMinutes()).padStart(2, '0');
    const seconds = String(date.getUTCSeconds()).padStart(2, '0');
    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds} UTC`;
  };

  // Allow fetch if either client secret is provided OR credentials are loaded from config
  const canFetch = clientId && (clientSecret || credentialsFromConfig) && grantType && fromTimestamp && toTimestamp && listingStart && listingEnd && !isFetching;

  const handleFetch = async () => {
    if (!canFetch) {
      console.warn('Cannot fetch: Missing required fields', {
        clientId: !!clientId,
        clientSecret: !!clientSecret,
        credentialsFromConfig,
        grantType: !!grantType,
        fromTimestamp: !!fromTimestamp,
        toTimestamp: !!toTimestamp,
        listingStart: !!listingStart,
        listingEnd: !!listingEnd,
        isFetching
      });
      return;
    }

    console.log('🗄️ Starting DB Fetch...', {
      credentialsFromConfig,
      hasClientId: !!clientId,
      hasClientSecret: !!clientSecret,
      fromTimestamp,
      toTimestamp,
      listingRange: `${listingStart}-${listingEnd}`
    });

    setIsFetching(true);
    setError(null);

    try {
      // Build payload - only include credentials if they're provided (otherwise backend uses config)
      const payload = {
        min_last_update: parseInt(fromTimestamp),
        max_last_update: parseInt(toTimestamp),
        listing_start: parseInt(listingStart),
        listing_end: parseInt(listingEnd)
      };

      // Add category filters if any selected
      if (selectedCategories.length > 0) {
        payload.category_filters = selectedCategories;
      }

      // Only add credentials if explicitly provided (not from config)
      if (clientId && !credentialsFromConfig) {
        payload.client_id = clientId;
      }
      if (clientSecret) {
        payload.client_secret = clientSecret;
      }
      if (grantType) {
        payload.grant_type = grantType;
      }

      console.log('📤 Sending POST request to:', `${API_BASE}/api/db-fetch`);
      console.log('📦 Payload:', payload);

      const res = await fetch(`${API_BASE}/api/db-fetch`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      });

      console.log('📥 Response status:', res.status, res.statusText);

      if (!res.ok) {
        const errData = await res.json();
        console.error('❌ Error response:', errData);
        throw new Error(errData.detail || 'DB Fetch failed');
      }

      const data = await res.json();
      console.log('✅ Success response:', data);

      // Show preview modal
      setFetchResult(data);
      setShowPreview(true);

    } catch (err) {
      console.error('❌ Fetch error:', err);
      setError(err.message);

      // Show user-friendly error message
      if (err.message.includes('No trucks found')) {
        setError('No trucks found for the selected date range. Please try a different date range or listing range.');
      }
    } finally {
      setIsFetching(false);
    }
  };

  const handleStartAnnotation = async () => {
    if (!fetchResult || !fetchResult.fetch_id) return;

    setIsStartingAnnotation(true);
    setError(null);

    try {
      const res = await fetch(`${API_BASE}/api/db-fetch/${fetchResult.fetch_id}/start-annotation`, {
        method: 'POST'
      });

      if (!res.ok) {
        const errData = await res.json();
        throw new Error(errData.detail || 'Failed to start annotation');
      }

      const data = await res.json();

      // Close preview and create job
      setShowPreview(false);
      onJobCreated({
        id: data.job_id,
        filename: fetchResult.filename,
        status: data.status,
        is_db_fetch: true,
        adCount: fetchResult.total_trucks
      });

    } catch (err) {
      setError(err.message);
    } finally {
      setIsStartingAnnotation(false);
    }
  };

  const handleClosePreview = () => {
    setShowPreview(false);
    // Reset form to allow new fetch
    setFetchResult(null);
    setUploadedFile(null);  // Also reset uploaded file for Ad IDs mode
  };

  const handleUploadAdIds = async (file) => {
    if (!file) return;

    setIsUploadingIds(true);
    setError(null);
    setUploadedFile(file);

    try {
      const formData = new FormData();
      formData.append('file', file);

      const res = await fetch(`${API_BASE}/api/db-fetch-by-ids`, {
        method: 'POST',
        body: formData
      });

      if (!res.ok) {
        const errData = await res.json();
        throw new Error(errData.detail || 'Upload failed');
      }

      const data = await res.json();
      const fetchId = data.fetch_id;

      // Poll for fetch status
      console.log(`🔄 Polling for fetch status (ID: ${fetchId})...`);

      const pollInterval = setInterval(async () => {
        try {
          const statusRes = await fetch(`${API_BASE}/api/db-fetch-by-ids/${fetchId}/status`);
          if (!statusRes.ok) {
            clearInterval(pollInterval);
            throw new Error('Failed to check fetch status');
          }

          const statusData = await statusRes.json();
          console.log(`📊 Fetch status: ${statusData.status}`);

          if (statusData.status === 'fetched') {
            // Fetch complete! Show preview modal
            clearInterval(pollInterval);
            setFetchResult(statusData);
            setShowPreview(true);
            setIsUploadingIds(false);
          } else if (statusData.status === 'failed') {
            // Fetch failed
            clearInterval(pollInterval);
            throw new Error(statusData.error || 'Fetch failed');
          }
          // If still 'fetching', keep polling

        } catch (err) {
          clearInterval(pollInterval);
          setError(err.message);
          setUploadedFile(null);
          setIsUploadingIds(false);
        }
      }, 2000); // Poll every 2 seconds

    } catch (err) {
      setError(err.message);
      setUploadedFile(null);
      setIsUploadingIds(false);
    }
  };

  return (
    <div className="dbfetch-section">
      {/* Mode Switcher */}
      <div style={{
        display: 'flex',
        gap: '1rem',
        marginBottom: '2rem',
        padding: '0.5rem',
        background: 'rgba(0, 0, 0, 0.3)',
        borderRadius: 'var(--border-radius)',
        border: '1px solid rgba(255, 255, 255, 0.1)'
      }}>
        <button
          onClick={() => setFetchMode('daterange')}
          style={{
            flex: 1,
            padding: '0.75rem 1rem',
            background: fetchMode === 'daterange' ? '#ff9800' : 'transparent',
            color: fetchMode === 'daterange' ? '#000' : '#e0e0e0',
            border: 'none',
            borderRadius: '8px',
            cursor: 'pointer',
            fontWeight: '600',
            fontSize: '0.95rem',
            transition: 'all 0.2s ease',
            boxShadow: fetchMode === 'daterange' ? '0 2px 8px rgba(255, 152, 0, 0.3)' : 'none'
          }}
        >
          📅 Fetch by Date Range
        </button>
        <button
          onClick={() => setFetchMode('adids')}
          style={{
            flex: 1,
            padding: '0.75rem 1rem',
            background: fetchMode === 'adids' ? '#4caf50' : 'transparent',
            color: fetchMode === 'adids' ? '#000' : '#e0e0e0',
            border: 'none',
            borderRadius: '8px',
            cursor: 'pointer',
            fontWeight: '600',
            fontSize: '0.95rem',
            transition: 'all 0.2s ease',
            boxShadow: fetchMode === 'adids' ? '0 2px 8px rgba(76, 175, 80, 0.3)' : 'none'
          }}
        >
          ⚡ Fetch by Ad IDs (Faster!)
        </button>
      </div>

      {/* Preview Modal - Shared by both modes */}
      {showPreview && (
        <PreviewModal
          fetchResult={fetchResult}
          onClose={handleClosePreview}
          onStartAnnotation={handleStartAnnotation}
          isStarting={isStartingAnnotation}
        />
      )}

      {/* Fetch by Ad IDs Mode */}
      {fetchMode === 'adids' && (
        <div className="dbfetch-adids-mode">
          <div className="dbfetch-card">
            <h3>⚡ Fetch by Ad IDs - Much Faster!</h3>
            <div style={{
              padding: '1rem',
              background: 'rgba(76, 175, 80, 0.15)',
              border: '1px solid rgba(76, 175, 80, 0.4)',
              borderRadius: 'var(--border-radius)',
              marginBottom: '1.5rem'
            }}>
              <div style={{ fontSize: '0.95rem', marginBottom: '0.5rem', fontWeight: '600', color: 'var(--accent-green)' }}>
                🚀 Why this is faster:
              </div>
              <ul style={{ margin: '0.5rem 0', paddingLeft: '1.5rem', fontSize: '0.85rem', lineHeight: '1.6' }}>
                <li>Direct API fetch: ~1-2 seconds per truck</li>
                <li>No browser needed, no page loading</li>
                <li>Parallel processing: Fetches multiple trucks simultaneously</li>
                <li><strong>10x faster than web scraping!</strong></li>
              </ul>
            </div>

            <div style={{ marginBottom: '1.5rem' }}>
              <h4 style={{ fontSize: '0.9rem', marginBottom: '0.75rem', color: 'var(--text-muted)' }}>
                📋 Instructions:
              </h4>
              <ol style={{ margin: '0', paddingLeft: '1.5rem', fontSize: '0.85rem', lineHeight: '1.8', color: 'var(--text-muted)' }}>
                <li>Prepare an Excel file with an "Ad ID" column</li>
                <li>Upload the file below</li>
                <li>System will fetch all truck data directly from the database</li>
                <li>Data will be ready for AI annotation in seconds!</li>
              </ol>
            </div>

            {/* File Upload */}
            <div style={{
              border: '2px dashed rgba(76, 175, 80, 0.4)',
              borderRadius: 'var(--border-radius)',
              padding: '2rem',
              textAlign: 'center',
              background: 'rgba(0, 0, 0, 0.2)',
              cursor: uploadedFile ? 'default' : 'pointer',
              transition: 'all 0.2s ease'
            }}
              onClick={() => !uploadedFile && !isUploadingIds && document.getElementById('adids-file-input').click()}
              onDragOver={(e) => {
                e.preventDefault();
                e.currentTarget.style.borderColor = 'var(--accent-green)';
                e.currentTarget.style.background = 'rgba(76, 175, 80, 0.1)';
              }}
              onDragLeave={(e) => {
                e.currentTarget.style.borderColor = 'rgba(76, 175, 80, 0.4)';
                e.currentTarget.style.background = 'rgba(0, 0, 0, 0.2)';
              }}
              onDrop={(e) => {
                e.preventDefault();
                e.currentTarget.style.borderColor = 'rgba(76, 175, 80, 0.4)';
                e.currentTarget.style.background = 'rgba(0, 0, 0, 0.2)';
                if (!uploadedFile && !isUploadingIds && e.dataTransfer.files[0]) {
                  handleUploadAdIds(e.dataTransfer.files[0]);
                }
              }}
            >
              <input
                id="adids-file-input"
                type="file"
                accept=".xlsx,.xls"
                onChange={(e) => e.target.files[0] && handleUploadAdIds(e.target.files[0])}
                style={{ display: 'none' }}
                disabled={isUploadingIds || uploadedFile}
              />

              {isUploadingIds ? (
                <>
                  <div className="btn-spinner" style={{ margin: '0 auto 1rem' }}></div>
                  <div style={{ fontSize: '1rem', fontWeight: '600', color: 'var(--accent-green)' }}>
                    🗄️ Fetching from Database...
                  </div>
                  <div style={{ fontSize: '0.85rem', color: 'var(--text-muted)', marginTop: '0.5rem' }}>
                    Reading Ad IDs and fetching truck data from API
                  </div>
                  <div style={{ fontSize: '0.85rem', color: 'var(--accent-blue)', marginTop: '0.5rem' }}>
                    ⚡ This is 10x faster than scraping!
                  </div>
                </>
              ) : (
                <>
                  <div style={{ fontSize: '2rem', marginBottom: '0.5rem' }}>📤</div>
                  <div style={{ fontSize: '1rem', fontWeight: '600', marginBottom: '0.5rem' }}>
                    Drop Excel file here or click to browse
                  </div>
                  <div style={{ fontSize: '0.85rem', color: 'var(--text-muted)' }}>
                    Must have "Ad ID" column • .xlsx or .xls files only
                  </div>
                </>
              )}
            </div>
          </div>

          {/* Error message */}
          {error && (
            <div className="audit-error" style={{ marginTop: '1rem' }}>
              <span className="error-icon">❌</span>
              <span>{error}</span>
            </div>
          )}
        </div>
      )}

      {/* Fetch by Date Range Mode (existing) */}
      {fetchMode === 'daterange' && (
        <>
          {/* Credentials Section */}
          <div className="dbfetch-card">
            <h3>🔑 API Credentials</h3>
            {credentialsFromConfig && (
              <div className="config-loaded-badge">
                ✅ Credentials loaded from config.ini (you can override them below)
              </div>
            )}
            <div className="form-grid">
              <div className="form-group">
                <label>Client ID {credentialsFromConfig && <span className="config-indicator">📋 from config</span>}</label>
                <input
                  type="text"
                  value={clientId}
                  onChange={(e) => {
                    setClientId(e.target.value);
                    setCredentialsFromConfig(false);
                  }}
                  placeholder="Enter client ID or load from config.ini"
                  className="form-input"
                />
              </div>
              <div className="form-group">
                <label>Client Secret {credentialsFromConfig && <span className="config-indicator">🔒 using config</span>}</label>
                <input
                  type="password"
                  value={clientSecret}
                  onChange={(e) => {
                    setClientSecret(e.target.value);
                    setCredentialsFromConfig(false);
                  }}
                  placeholder={credentialsFromConfig ? "••••••••••••••••  (stored in config)" : "Enter client secret or add to config.ini"}
                  className="form-input"
                  disabled={credentialsFromConfig}
                />
                {credentialsFromConfig && (
                  <small className="form-hint" style={{ color: 'var(--accent-green)' }}>
                    ✅ Using secure credentials from config.ini
                  </small>
                )}
              </div>
              <div className="form-group">
                <label>Grant Type {credentialsFromConfig && <span className="config-indicator">📋 from config</span>}</label>
                <input
                  type="text"
                  value={grantType}
                  onChange={(e) => setGrantType(e.target.value)}
                  placeholder="client_credentials"
                  className="form-input"
                />
              </div>
            </div>
          </div>

          {/* Date Range Section */}
          <div className="dbfetch-card">
            <h3>📅 Date Range</h3>
            {fromTimestamp && toTimestamp && fromTimestamp === toTimestamp && (
              <div style={{
                padding: '0.75rem',
                background: 'rgba(50, 205, 50, 0.15)',
                border: '1px solid rgba(50, 205, 50, 0.4)',
                borderRadius: 'var(--border-radius)',
                marginBottom: '1rem',
                fontSize: '0.85rem',
                color: 'var(--accent-green)'
              }}>
                ℹ️ Same date selected - will automatically fetch all trucks for the full 24-hour period.
              </div>
            )}
            <div className="form-grid">
              <div className="form-group">
                <label>From Date</label>
                <input
                  type="date"
                  value={timestampToDate(fromTimestamp)}
                  onChange={(e) => setFromTimestamp(dateToTimestamp(e.target.value))}
                  className="form-input"
                />
                <small className="form-hint">Unix: {fromTimestamp || 'Not set'}</small>
              </div>
              <div className="form-group">
                <label>To Date</label>
                <input
                  type="date"
                  value={timestampToDate(toTimestamp)}
                  onChange={(e) => setToTimestamp(dateToTimestamp(e.target.value))}
                  className="form-input"
                />
                <small className="form-hint">Unix: {toTimestamp || 'Not set'}</small>
              </div>
            </div>
            <div className="form-row">
              <div className="form-group full-width">
                <label>Or Enter Timestamps Manually</label>
                <div className="timestamp-inputs">
                  <input
                    type="number"
                    value={fromTimestamp}
                    onChange={(e) => setFromTimestamp(e.target.value)}
                    placeholder="From timestamp (e.g., 1768262400)"
                    className="form-input"
                  />
                  <span className="separator">→</span>
                  <input
                    type="number"
                    value={toTimestamp}
                    onChange={(e) => setToTimestamp(e.target.value)}
                    placeholder="To timestamp (e.g., 1768348799)"
                    className="form-input"
                  />
                </div>
                <small className="form-hint" style={{ marginTop: '0.5rem', display: 'block' }}>
                  💡 Tip: Use <a href="https://www.unixtimestamp.com/" target="_blank" rel="noopener noreferrer" style={{ color: 'var(--accent-orange)' }}>unixtimestamp.com</a> to convert dates. Use the same timestamp for both to get a full day, or different timestamps for a range.
                </small>
                {fromTimestamp && toTimestamp && (() => {
                  // Calculate the ACTUAL timestamps that backend will use
                  // If same timestamp, backend expands max to include full 24 hours (+86399 seconds)
                  const actualFromTs = parseInt(fromTimestamp);
                  const actualToTs = fromTimestamp === toTimestamp
                    ? parseInt(toTimestamp) + 86399  // Add 23:59:59 for full day coverage
                    : parseInt(toTimestamp);

                  return (
                    <div style={{
                      marginTop: '0.75rem',
                      padding: '0.75rem',
                      background: 'rgba(100, 150, 255, 0.1)',
                      border: '1px solid rgba(100, 150, 255, 0.3)',
                      borderRadius: '4px',
                      fontSize: '0.85rem'
                    }}>
                      <div style={{ marginBottom: '0.5rem', fontWeight: '600', color: 'var(--accent-blue)' }}>
                        🕐 <strong>Exact Fetch Time Range:</strong>
                      </div>
                      <div style={{
                        display: 'flex',
                        flexDirection: 'column',
                        gap: '0.4rem',
                        padding: '0.5rem',
                        background: 'rgba(0, 0, 0, 0.2)',
                        borderRadius: '4px',
                        fontFamily: 'monospace'
                      }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                          <span style={{ color: 'var(--accent-green)', fontWeight: '600' }}>FROM:</span>
                          <span>{timestampToDetailedDateTime(actualFromTs.toString())}</span>
                        </div>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                          <span style={{ color: 'var(--accent-orange)', fontWeight: '600' }}>TO:</span>
                          <span style={{ marginLeft: '1.1rem' }}>{timestampToDetailedDateTime(actualToTs.toString())}</span>
                        </div>
                      </div>
                      <div style={{ marginTop: '0.5rem', fontSize: '0.75rem', color: 'var(--text-muted)' }}>
                        {fromTimestamp === toTimestamp
                          ? '✅ Same date selected - automatically expanded to full 24-hour period (00:00:00 to 23:59:59 UTC)'
                          : 'Data will be fetched for ads updated within this exact time window.'
                        }
                      </div>
                    </div>
                  );
                })()}
              </div>
            </div>
          </div>
          {/* Category Filter Section */}
          <div className="dbfetch-card">
            <h3>🏷️ Category Filter (Optional)</h3>
            <p style={{ fontSize: '0.85rem', color: 'var(--text-muted)', marginBottom: '0.75rem' }}>
              Select specific categories to fetch. Leave empty to fetch all categories.
            </p>

            {/* Quick actions */}
            <div style={{ marginBottom: '0.75rem', display: 'flex', gap: '0.5rem' }}>
              <button
                className="range-btn"
                style={{ padding: '0.4rem 0.75rem', fontSize: '0.8rem' }}
                onClick={() => setSelectedCategories([...AVAILABLE_CATEGORIES])}
              >
                Select All
              </button>
              <button
                className="range-btn"
                style={{ padding: '0.4rem 0.75rem', fontSize: '0.8rem' }}
                onClick={() => setSelectedCategories([])}
              >
                Clear All
              </button>
              {selectedCategories.length > 0 && (
                <span style={{
                  marginLeft: 'auto',
                  color: 'var(--accent-orange)',
                  fontSize: '0.85rem',
                  fontWeight: '600'
                }}>
                  {selectedCategories.length} selected
                </span>
              )}
            </div>

            {/* Category checkboxes */}
            <div style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))',
              gap: '0.5rem',
              maxHeight: '250px',
              overflowY: 'auto',
              padding: '0.5rem',
              background: 'rgba(0, 0, 0, 0.2)',
              borderRadius: '4px'
            }}>
              {AVAILABLE_CATEGORIES.map(category => (
                <label
                  key={category}
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.5rem',
                    padding: '0.4rem 0.5rem',
                    background: selectedCategories.includes(category)
                      ? 'rgba(255, 165, 0, 0.2)'
                      : 'transparent',
                    borderRadius: '4px',
                    cursor: 'pointer',
                    fontSize: '0.85rem',
                    border: selectedCategories.includes(category)
                      ? '1px solid rgba(255, 165, 0, 0.4)'
                      : '1px solid transparent',
                    transition: 'all 0.2s ease'
                  }}
                >
                  <input
                    type="checkbox"
                    checked={selectedCategories.includes(category)}
                    onChange={(e) => {
                      if (e.target.checked) {
                        setSelectedCategories([...selectedCategories, category]);
                      } else {
                        setSelectedCategories(selectedCategories.filter(c => c !== category));
                      }
                    }}
                    style={{ accentColor: 'var(--accent-orange)' }}
                  />
                  {category}
                </label>
              ))}
            </div>

            {selectedCategories.length > 0 && (
              <div style={{
                marginTop: '0.75rem',
                padding: '0.5rem',
                background: 'rgba(255, 165, 0, 0.15)',
                border: '1px solid rgba(255, 165, 0, 0.3)',
                borderRadius: '4px',
                fontSize: '0.85rem'
              }}>
                <strong style={{ color: 'var(--accent-orange)' }}>🔍 Filtering by:</strong>
                <span style={{ marginLeft: '0.5rem' }}>
                  {selectedCategories.slice(0, 3).join(', ')}
                  {selectedCategories.length > 3 && ` +${selectedCategories.length - 3} more`}
                </span>
              </div>
            )}
          </div>

          {/* Listing Range Section */}
          <div className="dbfetch-card">
            <h3>📊 Listing Range</h3>
            <div className="listing-range-options">
              <button
                className={`range-btn ${listingStart === '0' && listingEnd === '1000' ? 'active' : ''}`}
                onClick={() => { setListingStart('0'); setListingEnd('1000'); }}
              >
                First 1000 (0-1000)
              </button>
              <button
                className={`range-btn ${listingStart === '1000' && listingEnd === '2000' ? 'active' : ''}`}
                onClick={() => { setListingStart('1000'); setListingEnd('2000'); }}
              >
                1000-2000
              </button>
              <button
                className={`range-btn ${listingStart === '2000' && listingEnd === '3000' ? 'active' : ''}`}
                onClick={() => { setListingStart('2000'); setListingEnd('3000'); }}
              >
                2000-3000
              </button>
            </div>
            <div className="form-row">
              <div className="form-group">
                <label>Custom Start</label>
                <input
                  type="number"
                  value={listingStart}
                  onChange={(e) => setListingStart(e.target.value)}
                  placeholder="0"
                  className="form-input"
                  min="0"
                />
              </div>
              <div className="form-group">
                <label>Custom End</label>
                <input
                  type="number"
                  value={listingEnd}
                  onChange={(e) => setListingEnd(e.target.value)}
                  placeholder="1000"
                  className="form-input"
                  min="0"
                />
              </div>
            </div>
            <div className="listing-count">
              Total Listings: {parseInt(listingEnd || 0) - parseInt(listingStart || 0)}
            </div>
          </div>

          {/* Fetch Button */}
          <div className="dbfetch-actions">
            <button
              className={`btn btn-primary ${canFetch ? '' : 'disabled'}`}
              onClick={handleFetch}
              disabled={!canFetch}
            >
              {isFetching ? (
                <>
                  <div className="btn-spinner"></div>
                  Fetching Data...
                </>
              ) : (
                <>
                  <span>🗄️</span> Fetch from Database
                </>
              )}
            </button>
          </div>

          {/* Error message */}
          {error && fetchMode === 'daterange' && (
            <div className="audit-error">
              <span className="error-icon">❌</span>
              <span>{error}</span>
            </div>
          )}
        </>
      )}
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
            <button className="btn btn-danger" onClick={handleDownload}>
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

// DB Update Section Component
const DBUpdateSection = () => {
  const [updateFile, setUpdateFile] = useState(null);
  const [clientSecret, setClientSecret] = useState('');
  const [isUpdating, setIsUpdating] = useState(false);
  const [updateResult, setUpdateResult] = useState(null);
  const [error, setError] = useState(null);
  const [dbUpdateConfig, setDbUpdateConfig] = useState(null);
  const [dragActive, setDragActive] = useState(false);
  const [showDetails, setShowDetails] = useState(false);
  const fileInputRef = useRef(null);

  // Load DB Update config on mount
  useEffect(() => {
    fetch(`${API_BASE}/api/db-update-config`)
      .then(res => res.json())
      .then(data => setDbUpdateConfig(data))
      .catch(err => console.error('Failed to fetch DB Update config:', err));
  }, []);

  const canUpdate = updateFile && !isUpdating;

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
      setUpdateFile(e.dataTransfer.files[0]);
      setUpdateResult(null);
      setError(null);
    }
  };

  const handleFileChange = (e) => {
    if (e.target.files && e.target.files[0]) {
      setUpdateFile(e.target.files[0]);
      setUpdateResult(null);
      setError(null);
    }
  };

  const handleUpdate = async () => {
    if (!canUpdate) return;

    setIsUpdating(true);
    setError(null);
    setUpdateResult(null);

    try {
      const formData = new FormData();
      formData.append('file', updateFile);
      if (clientSecret) {
        formData.append('client_secret', clientSecret);
      }

      const res = await fetch(`${API_BASE}/api/db-update`, {
        method: 'POST',
        body: formData
      });

      if (!res.ok) {
        let errorMessage = `DB Update failed with status ${res.status}`;
        try {
          const errData = await res.json();
          if (errData.detail) errorMessage = errData.detail;
        } catch (_) {
          // Response was not JSON (e.g. proxy error page) — use default message
        }
        throw new Error(errorMessage);
      }

      const data = await res.json();
      setUpdateResult(data);

    } catch (err) {
      setError(err.message);
    } finally {
      setIsUpdating(false);
    }
  };

  const handleReset = () => {
    setUpdateFile(null);
    setUpdateResult(null);
    setError(null);
    setClientSecret('');
    setShowDetails(false);
  };

  return (
    <div className="db-update-section">
      {!updateResult && (
        <>
          {/* File upload area */}
          <div
            className={`db-update-upload-zone ${dragActive ? 'drag-active' : ''} ${updateFile ? 'has-file' : ''}`}
            onDragEnter={handleDrag}
            onDragLeave={handleDrag}
            onDragOver={handleDrag}
            onDrop={handleDrop}
            onClick={() => fileInputRef.current?.click()}
          >
            <input
              ref={fileInputRef}
              type="file"
              accept=".xlsx,.xls"
              onChange={handleFileChange}
              style={{ display: 'none' }}
            />
            {updateFile ? (
              <div className="db-update-file-info">
                <span className="file-icon">📄</span>
                <div>
                  <p className="file-name">{updateFile.name}</p>
                  <p className="file-size">{(updateFile.size / 1024).toFixed(1)} KB</p>
                </div>
                <button
                  className="file-remove-btn"
                  onClick={(e) => { e.stopPropagation(); setUpdateFile(null); }}
                >
                  ✕
                </button>
              </div>
            ) : (
              <div className="db-update-upload-prompt">
                <span className="upload-icon">📤</span>
                <p className="upload-text">
                  {dragActive ? 'Drop your Excel file here' : 'Drag & drop AI Output Excel file here'}
                </p>
                <p className="upload-hint">or click to browse (.xlsx, .xls)</p>
              </div>
            )}
          </div>

          {/* Client Secret input */}
          <div className="db-update-credentials">
            <div className="form-group">
              <label>
                Client Secret
                {dbUpdateConfig?.has_secret && (
                  <span className="config-hint"> (configured in config.ini)</span>
                )}
              </label>
              <input
                type="password"
                value={clientSecret}
                onChange={(e) => setClientSecret(e.target.value)}
                placeholder={dbUpdateConfig?.has_secret ? 'Using config.ini value (override here if needed)' : 'Enter client secret'}
                className="form-input"
              />
            </div>
            {dbUpdateConfig && (
              <div className="db-update-config-info">
                <p><strong>Token URL:</strong> {dbUpdateConfig.token_url || 'Not configured'}</p>
                <p><strong>Client ID:</strong> {dbUpdateConfig.client_id || 'Not configured'}</p>
              </div>
            )}
          </div>

          {/* Update button */}
          <div className="db-update-actions">
            <button
              className={`btn btn-db-update ${canUpdate ? '' : 'disabled'}`}
              onClick={handleUpdate}
              disabled={!canUpdate}
            >
              {isUpdating ? (
                <>
                  <div className="btn-spinner"></div>
                  Updating Database...
                </>
              ) : (
                <>
                  <span>🔄</span> Update Database Categories
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
      {updateResult && (
        <div className="db-update-results">
          <div className="db-update-result-header">
            <div className="result-icon">
              {updateResult.failed_count === 0 ? '✅' : '⚠️'}
            </div>
            <h3>DB Update Complete!</h3>
          </div>

          {/* Summary Stats */}
          <div className="db-update-stats">
            <div className="db-update-stat-card main">
              <div className="stat-value">{updateResult.success_count}</div>
              <div className="stat-label-text">Successfully Updated</div>
            </div>
            <div className="db-update-stat-card">
              <div className="stat-value red">{updateResult.failed_count}</div>
              <div className="stat-label-text">Failed</div>
            </div>
            <div className="db-update-stat-card">
              <div className="stat-value gray">{updateResult.skipped_count}</div>
              <div className="stat-label-text">Skipped</div>
            </div>
            <div className="db-update-stat-card">
              <div className="stat-value">{updateResult.total_rows}</div>
              <div className="stat-label-text">Total Rows</div>
            </div>
            {updateResult.patched_count > 0 && (
              <div className="db-update-stat-card" style={{ borderLeft: '3px solid #ff9800' }}>
                <div className="stat-value" style={{ color: '#ff9800' }}>{updateResult.patched_count}</div>
                <div className="stat-label-text">Patched Ads</div>
              </div>
            )}
          </div>

          {/* Message */}
          <div className="db-update-message">
            <p>{updateResult.message}</p>
          </div>

          {/* Details toggle */}
          {updateResult.results && updateResult.results.length > 0 && (
            <div className="db-update-details-section">
              <button
                className="btn btn-secondary db-update-toggle-btn"
                onClick={() => setShowDetails(!showDetails)}
              >
                {showDetails ? '▼ Hide Details' : '▶ Show Details'} ({updateResult.results.length} ads)
              </button>

              {showDetails && (
                <div className="db-update-details-list">
                  {updateResult.results.map((result, idx) => (
                    <div key={idx} className={`db-update-detail-item ${result.success ? 'success' : 'failed'}`}>
                      <span className="detail-status">{result.success ? '✅' : '❌'}</span>
                      <span className="detail-ad-id">Ad {result.ad_id}</span>
                      <span className="detail-categories">
                        {result.categories && result.categories.length > 0
                          ? result.categories.join(', ')
                          : 'No categories'}
                      </span>
                      {result.error && (
                        <span className="detail-error">{result.error}</span>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          {/* Patch Summary Download */}
          {((updateResult.patched_count > 0) || (updateResult.patches_created > 0) || (updateResult.patches_updated > 0)) && updateResult.patch_report_id && (
            <div className="db-update-patch-download" style={{ margin: '1rem 0', padding: '1rem', background: 'rgba(255, 152, 0, 0.08)', borderRadius: '10px', border: '1px solid rgba(255, 152, 0, 0.25)' }}>
              <p style={{ margin: '0 0 0.75rem 0', fontSize: '0.9rem', color: '#ff9800' }}>
                🩹 <strong>{updateResult.patches_created} patch(es) created, {updateResult.patches_updated} updated{updateResult.patches_failed > 0 ? `, ${updateResult.patches_failed} failed` : ''}.</strong> Download the patch summary for the data team to review.
              </p>
              <button
                className="btn"
                onClick={() => {
                  window.open(`${API_BASE}/api/db-update/patch-report/${updateResult.patch_report_id}/download`, '_blank');
                }}
                style={{
                  background: 'linear-gradient(135deg, #ff9800, #f57c00)',
                  color: '#fff',
                  border: 'none',
                  padding: '0.6rem 1.25rem',
                  borderRadius: '8px',
                  cursor: 'pointer',
                  fontWeight: '600',
                  fontSize: '0.9rem',
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: '0.5rem',
                  boxShadow: '0 2px 8px rgba(255, 152, 0, 0.3)',
                }}
              >
                <span>📥</span> Download Patch Summary Excel
              </button>
            </div>
          )}

          {/* Action buttons */}
          <div className="db-update-result-actions">
            <button className="btn btn-secondary" onClick={handleReset}>
              <span>🔄</span> New Update
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
  const [batches, setBatches] = useState([]);
  const pollIntervalRef = useRef(null);
  const lastBatchCountRef = useRef(0);

  // Fetch config on mount
  useEffect(() => {
    fetch(`${API_BASE}/api/config`)
      .then(res => res.json())
      .then(data => setConfig(data))
      .catch(err => console.error('Failed to fetch config:', err));
  }, []);

  const prevStatusRef = useRef(null);

  // Poll for job updates (including batch progress)
  useEffect(() => {
    // Poll during scraping, processing, AND dually verification phases
    if (job?.status === 'scraping' || job?.status === 'processing' || job?.status === 'verifying_dually') {
      pollIntervalRef.current = setInterval(async () => {
        try {
          const res = await fetch(`${API_BASE}/api/jobs/${job.id}`);
          const data = await res.json();
          setJob(prev => ({ ...prev, ...data }));

          // Fetch batch list when new batches complete OR on status change
          const newBatchCount = data.batch_count || 0;
          const statusChanged = data.status !== prevStatusRef.current;
          prevStatusRef.current = data.status;

          if (newBatchCount > lastBatchCountRef.current || statusChanged) {
            lastBatchCountRef.current = newBatchCount;
            try {
              const batchRes = await fetch(`${API_BASE}/api/jobs/${job.id}/batches`);
              const batchData = await batchRes.json();
              setBatches(batchData.batches || []);
            } catch (batchErr) {
              console.error('Failed to fetch batches:', batchErr);
            }
          }

          // On completion/failure, do a final batch fetch and stop polling
          if (data.status === 'completed' || data.status === 'failed') {
            clearInterval(pollIntervalRef.current);
            // Final batch list fetch
            try {
              const batchRes = await fetch(`${API_BASE}/api/jobs/${job.id}/batches`);
              const batchData = await batchRes.json();
              setBatches(batchData.batches || []);
            } catch (batchErr) {
              console.error('Failed to fetch final batches:', batchErr);
            }
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
    setBatches([]);
    lastBatchCountRef.current = 0;
  };

  // Handle DB Fetch job creation
  const handleDBFetchJobCreated = (jobData) => {
    setJob(jobData);
    setActiveTab('annotation'); // Switch to annotation tab to show job dashboard
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
          <source src="/videos/American_Truck_Background_Video_Creation.mp4" type="video/mp4" />
        </video>
        <div className="bg-gradient"></div>
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
                <h2>{job?.is_db_fetch ? 'DB Fetch → Auto Parallel AI' : 'Scraper → Auto Parallel AI'}</h2>
                <p className="mode-badge">
                  {job?.is_db_fetch ? 'Database API → AI Annotation' : 'High Accuracy Mode • No Vision v2'}
                </p>
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
                          <button className="btn btn-danger" onClick={handleDownload}>
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
                          ? (job.is_db_fetch ? '🗄️ Fetching from Database...' : '🌐 Scraping Ads...')
                          : job.status === 'verifying_dually'
                            ? '🔍 Verifying Dually Detections...'
                            : '🤖 AI Classification in Progress...'}
                      </h3>
                      <p>
                        {job.status === 'scraping'
                          ? (job.is_db_fetch
                            ? 'Fetching truck data directly from database API with breadcrumbs and images...'
                            : 'Extracting breadcrumbs and images from Commercial Truck Trader...')
                          : job.status === 'verifying_dually'
                            ? `Double-checking Dually annotations using LLM verification (${job.dually_verification?.verified || 0}/${job.dually_verification?.total || 0})...`
                            : 'Classifying vehicles using Gemini AI...'}
                      </p>
                      <span className="processing-hint">
                        {job.status === 'verifying_dually'
                          ? 'Reducing false positives. Almost done!'
                          : job.total_batches > 0
                            ? `Processing in batches of 500. Batch ${job.current_batch || 1} of ${job.total_batches}. Completed batches will appear below for download.`
                            : 'This may take a few minutes. Please wait...'}
                      </span>
                    </div>
                  )}

                  {/* Batch Downloads List — shown during processing and after completion */}
                  {batches.length > 0 && (
                    <div style={{
                      marginTop: '1.5rem',
                      padding: '1rem',
                      background: 'rgba(0, 0, 0, 0.3)',
                      borderRadius: 'var(--border-radius)',
                      border: '1px solid rgba(76, 175, 80, 0.3)'
                    }}>
                      <h4 style={{ margin: '0 0 0.75rem 0', color: '#4caf50', fontSize: '1rem' }}>
                        📦 Completed Batches ({batches.length}/{job.total_batches || '?'})
                      </h4>
                      <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                        {batches.map((batch) => (
                          <div
                            key={batch.batch_index}
                            style={{
                              display: 'flex',
                              alignItems: 'center',
                              justifyContent: 'space-between',
                              padding: '0.6rem 1rem',
                              background: 'rgba(76, 175, 80, 0.12)',
                              border: '1px solid rgba(76, 175, 80, 0.25)',
                              borderRadius: '8px',
                              fontSize: '0.9rem'
                            }}
                          >
                            <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', flexWrap: 'wrap' }}>
                              <span style={{ color: '#4caf50', fontWeight: '700' }}>✅ Batch {batch.batch_num}</span>
                              <span style={{ color: '#a8a8a8' }}>Listings {batch.start}–{batch.end}</span>
                              <span style={{ color: '#b388ff', fontSize: '0.85rem' }}>{batch.row_count} rows</span>
                              {batch.total_cost > 0 && (
                                <span style={{ color: '#ffb74d', fontSize: '0.8rem' }}>💰 {batch.total_cost.toFixed(1)}¢</span>
                              )}
                            </div>
                            <button
                              onClick={() => window.open(`${API_BASE}/api/jobs/${job.id}/batches/${batch.batch_index}/download`, '_blank')}
                              style={{
                                background: 'linear-gradient(135deg, #4caf50, #388e3c)',
                                color: '#fff',
                                border: 'none',
                                padding: '0.4rem 1rem',
                                borderRadius: '6px',
                                cursor: 'pointer',
                                fontWeight: '600',
                                fontSize: '0.85rem',
                                display: 'flex',
                                alignItems: 'center',
                                gap: '0.4rem',
                                transition: 'all 0.2s ease',
                                boxShadow: '0 2px 6px rgba(76, 175, 80, 0.3)',
                                whiteSpace: 'nowrap'
                              }}
                            >
                              📥 Download
                            </button>
                          </div>
                        ))}
                      </div>
                      {job.current_batch > batches.length && job.total_batches > batches.length && (
                        <div style={{
                          marginTop: '0.5rem',
                          padding: '0.5rem 1rem',
                          background: 'rgba(255, 152, 0, 0.12)',
                          border: '1px solid rgba(255, 152, 0, 0.25)',
                          borderRadius: '8px',
                          fontSize: '0.85rem',
                          color: '#ffb74d',
                          display: 'flex',
                          alignItems: 'center',
                          gap: '0.5rem'
                        }}>
                          <div className="btn-spinner" style={{ width: '14px', height: '14px', borderWidth: '2px' }}></div>
                          Batch {job.current_batch} of {job.total_batches} processing...
                        </div>
                      )}
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

          {/* DB FETCH TAB */}
          {activeTab === 'dbfetch' && (
            <>
              <div className="section-header">
                <h2>Database Fetch + AI Annotation</h2>
                <p className="mode-badge">Fetch from Database → AI Annotation → Annotated Excel</p>
              </div>

              <DBFetchSection onJobCreated={handleDBFetchJobCreated} />

              {/* Info cards for DB fetch */}
              <div className="info-section">
                <div className="info-card">
                  <div className="info-icon">🗄️</div>
                  <h4>Database Fetch</h4>
                  <p>Fetches truck data directly from database API with breadcrumbs and images. No web scraping needed.</p>
                </div>
                <div className="info-card">
                  <div className="info-icon">🤖</div>
                  <h4>AI Annotation</h4>
                  <p>Automatically runs AI classification on fetched data using the same parallel processing as scraping.</p>
                </div>
                <div className="info-card">
                  <div className="info-icon">📊</div>
                  <h4>Same Output Format</h4>
                  <p>Generates identical annotated Excel files with all the same columns and features as the scraping method.</p>
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

          {/* DB UPDATE TAB */}
          {activeTab === 'dbupdate' && (
            <>
              <div className="section-header">
                <h2>Database Category Update</h2>
                <p className="mode-badge">Upload AI Output Excel → Update Categories in DB</p>
              </div>

              <DBUpdateSection />

              {/* Info cards for DB update */}
              <div className="info-section">
                <div className="info-card">
                  <div className="info-icon">📤</div>
                  <h4>Upload Output</h4>
                  <p>Upload the AI annotated Excel file. Only rows with "Require Update" status will be processed.</p>
                </div>
                <div className="info-card">
                  <div className="info-icon">🔄</div>
                  <h4>Auto Update</h4>
                  <p>Automatically updates ad categories in the database via the Trader API using AI predictions.</p>
                </div>
                <div className="info-card">
                  <div className="info-icon">⏭️</div>
                  <h4>Smart Skip</h4>
                  <p>Skips inactive ads, no-image cases, and unchanged categories. Only updates what needs updating.</p>
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
