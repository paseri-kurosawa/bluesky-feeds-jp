import { useState, useEffect } from 'react'
import './TrendHashtags.css'

export function TrendHashtags({ data }) {
  const [trends, setTrends] = useState({
    timestamp: null,
    stable_hashtags: [],
    recent_batches: [],
    selected_hot_tags: [],
    selection_method: null
  })
  const [loading, setLoading] = useState(true)
  const bucketUrl = 'https://bluesky-feed-dashboard-878311109818.s3.ap-northeast-1.amazonaws.com'

  useEffect(() => {
    const fetchTrendData = async () => {
      try {
        // Fetch stable hashtags (from raw posts)
        const stableUrl = `${bucketUrl}/components/stable_hashtags_from_raw_posts.json`
        const stableResponse = await fetch(stableUrl)
        let stableData = []
        let timestamp = null
        if (stableResponse.ok) {
          const json = await stableResponse.json()
          stableData = json.top_hashtags || []
          timestamp = json.generated_at
        }

        // Fetch recent batches (individual, past 3 batches)
        const recentBatchUrl = `${bucketUrl}/components/recent_batches.json`
        const recentBatchResponse = await fetch(recentBatchUrl)
        let recentBatches = []
        if (recentBatchResponse.ok) {
          const recentJson = await recentBatchResponse.json()
          recentBatches = (recentJson.batches || []).map(batch => ({
            timestamp: batch.timestamp,
            hashtags: Object.entries(batch.hashtags || {}).map(([tag, count]) => ({ tag, count }))
          }))
        }

        // Fetch selected hot hashtags
        const selectedHotUrl = `${bucketUrl}/components/selected_hot_hashtag.json`
        const selectedHotResponse = await fetch(selectedHotUrl)
        let selectedHotTags = []
        let selectionMethod = null
        if (selectedHotResponse.ok) {
          const hotJson = await selectedHotResponse.json()
          selectedHotTags = hotJson.selected_hot_tags || (hotJson.selected_hot_tag ? [hotJson.selected_hot_tag] : [])
          selectionMethod = hotJson.selection_method
        }

        setTrends({
          timestamp: timestamp || new Date().toISOString(),
          stable_hashtags: stableData,
          recent_batches: recentBatches,
          selected_hot_tags: selectedHotTags,
          selection_method: selectionMethod
        })
      } catch (err) {
        console.error('Error fetching trend data:', err)
      } finally {
        setLoading(false)
      }
    }

    fetchTrendData()
  }, [])

  if (loading || !trends) {
    return (
      <div className="trend-hashtags">
        <h2>Trend Hashtags</h2>
        <p>Loading...</p>
      </div>
    )
  }

  const renderTable = (hashtags) => {
    return (
      <div className="trend-table-container">
        <table className="trend-table">
          <thead>
            <tr>
              <th className="rank">Rank</th>
              <th className="hashtag">Hashtag</th>
              <th className="count">Count</th>
            </tr>
          </thead>
          <tbody>
            {hashtags.map((item, idx) => (
              <tr key={idx}>
                <td className="rank">{item.rank || idx + 1}</td>
                <td className="hashtag">#{item.tag}</td>
                <td className="count">{item.count}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    )
  }

  return (
    <div className="trend-hashtags">
      <div className="trend-header">
        <h2>Trend Hashtags</h2>
        <span className="timestamp">Updated: {trends.timestamp}</span>
      </div>

      {/* Selected Hot Hashtags Display */}
      <div className="selected-hot-hashtag-section">
        <div className="selected-hot-hashtag-content">
          <h3>Currently Selected Hot Hashtags (Per-Tag Query)</h3>
          {trends.selected_hot_tags && trends.selected_hot_tags.length > 0 ? (
            <div className="selected-hot-info">
              <div className="hashtag-display">
                {trends.selected_hot_tags.map((tag, idx) => (
                  <span key={idx} className="hashtag-name">#{tag}</span>
                ))}
              </div>
              <div className="selection-method">
                <span className="method-label">Selection Method:</span>
                <span className="method-value">{trends.selection_method}</span>
              </div>
            </div>
          ) : (
            <div className="selected-hot-info">
              <div className="no-selection">
                <span className="no-selection-text">No hot hashtags selected</span>
                {trends.selection_method && (
                  <span className="selection-method-fallback">({trends.selection_method})</span>
                )}
              </div>
            </div>
          )}
        </div>
      </div>

      <div className="trend-container">
        <div className="trend-section-batch">
          <h3>Recent Batches</h3>
          {trends.recent_batches && trends.recent_batches.length > 0 ? (
            trends.recent_batches.map((batch, idx) => (
              <div key={idx} className="batch-individual">
                <h4>Batch {idx + 1} ({batch.timestamp})</h4>
                {batch.hashtags.length > 0 ? (
                  renderTable(batch.hashtags)
                ) : (
                  <p className="no-data">No hashtags</p>
                )}
              </div>
            ))
          ) : (
            <p className="no-data">No batch data available</p>
          )}
        </div>
        <div className="trend-section-stable">
          <h3>Stable Hashtags</h3>
          {trends.stable_hashtags && trends.stable_hashtags.length > 0 ? (
            renderTable(trends.stable_hashtags)
          ) : (
            <p className="no-data">No stable hashtag data available</p>
          )}
        </div>
      </div>
    </div>
  )
}
