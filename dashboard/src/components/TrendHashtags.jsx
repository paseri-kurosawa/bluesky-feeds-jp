import { useState, useEffect } from 'react'
import './TrendHashtags.css'

export function TrendHashtags({ data }) {
  const [trends, setTrends] = useState({
    timestamp: null,
    stable_hashtags: [],
    top_hashtags_1h: [],
    last_fired_hot_tag: null,
    last_fired_hot_tag_timestamp: null
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

        // Fetch top hashtags 1H (from raw posts)
        const trendUrl = `${bucketUrl}/components/top_hashtags_1h_from_raw_posts.json`
        const trendResponse = await fetch(trendUrl)
        let trendData = []
        let lastFiredHotTag = null
        let lastFiredHotTagTimestamp = null
        if (trendResponse.ok) {
          const json = await trendResponse.json()
          trendData = json.top_hashtags_1h || []
          lastFiredHotTag = json.last_fired_hot_tag
          lastFiredHotTagTimestamp = json.last_fired_hot_tag_timestamp
        }

        setTrends({
          timestamp: timestamp || new Date().toISOString(),
          stable_hashtags: stableData,
          top_hashtags_1h: trendData,
          last_fired_hot_tag: lastFiredHotTag,
          last_fired_hot_tag_timestamp: lastFiredHotTagTimestamp
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

  const formatTimestamp = (isoTimestamp) => {
    if (!isoTimestamp) return ''
    const date = new Date(isoTimestamp)
    const year = date.getFullYear()
    const month = String(date.getMonth() + 1).padStart(2, '0')
    const day = String(date.getDate()).padStart(2, '0')
    const hours = String(date.getHours()).padStart(2, '0')
    const minutes = String(date.getMinutes()).padStart(2, '0')
    const seconds = String(date.getSeconds()).padStart(2, '0')
    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`
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
      <div className="trend-container">
        <div className="trend-section-1h">
          <h3>Hot Hashtags 1H</h3>
          {trends.last_fired_hot_tag && (
            <div className="current-hot-tag-box">
              Currently hot: <strong>#{trends.last_fired_hot_tag}</strong>
              {trends.last_fired_hot_tag_timestamp && (
                <span className="hot-tag-time"> ({formatTimestamp(trends.last_fired_hot_tag_timestamp)})</span>
              )}
            </div>
          )}
          {trends.top_hashtags_1h && trends.top_hashtags_1h.length > 0 ? (
            renderTable(trends.top_hashtags_1h)
          ) : (
            <p className="no-data">No hot hashtags available</p>
          )}
        </div>
        <div className="trend-section-all">
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
