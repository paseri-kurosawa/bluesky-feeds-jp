import { useState, useEffect } from 'react'
import './TrendHashtags.css'

export function TrendHashtags({ data }) {
  const [trends, setTrends] = useState({
    timestamp: null,
    stable_hashtags: [],
    latest_batch: [],
    selected_hot_tag: null,
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

        // Fetch latest batch
        const latestBatchUrl = `${bucketUrl}/components/latest_batch.json`
        const latestBatchResponse = await fetch(latestBatchUrl)
        let latestBatchData = []
        if (latestBatchResponse.ok) {
          const batchJson = await latestBatchResponse.json()
          // Handle new format: {hashtags: {...}, selected_hot_tag, selection_method}
          const hashtags = batchJson.hashtags || batchJson
          // Convert {tag: count} to [{tag: ..., count: ...}]
          latestBatchData = Object.entries(hashtags).map(([tag, count]) => ({
            tag,
            count
          }))
        }

        // Fetch selected hot hashtag
        const selectedHotUrl = `${bucketUrl}/components/selected_hot_hashtag.json`
        const selectedHotResponse = await fetch(selectedHotUrl)
        let selectedHotTag = null
        let selectionMethod = null
        if (selectedHotResponse.ok) {
          const hotJson = await selectedHotResponse.json()
          selectedHotTag = hotJson.selected_hot_tag
          selectionMethod = hotJson.selection_method
        }

        setTrends({
          timestamp: timestamp || new Date().toISOString(),
          stable_hashtags: stableData,
          latest_batch: latestBatchData,
          selected_hot_tag: selectedHotTag,
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

      {/* Selected Hot Hashtag Display */}
      <div className="selected-hot-hashtag-section">
        <div className="selected-hot-hashtag-content">
          <h3>Currently Selected Hot Hashtag</h3>
          {trends.selected_hot_tag ? (
            <div className="selected-hot-info">
              <div className="hashtag-display">
                <span className="hashtag-name">#{trends.selected_hot_tag}</span>
              </div>
              <div className="selection-method">
                <span className="method-label">Selection Method:</span>
                <span className="method-value">{trends.selection_method}</span>
              </div>
            </div>
          ) : (
            <div className="selected-hot-info">
              <div className="no-selection">
                <span className="no-selection-text">No hot hashtag selected</span>
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
          <h3>Latest Batch</h3>
          {trends.latest_batch && trends.latest_batch.length > 0 ? (
            renderTable(trends.latest_batch)
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
