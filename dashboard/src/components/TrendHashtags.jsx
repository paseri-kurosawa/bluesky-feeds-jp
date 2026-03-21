import { useState, useEffect } from 'react'
import './TrendHashtags.css'

export function TrendHashtags() {
  const [trends, setTrends] = useState(null)
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    const fetchTrends = async () => {
      try {
        setLoading(true)
        const dashboardUrl = `https://bluesky-feed-dashboard-878311109818.s3.ap-northeast-1.amazonaws.com/stats/summary/dashboard.json`
        const response = await fetch(dashboardUrl)
        if (!response.ok) {
          throw new Error(`Failed to fetch dashboard data: ${response.status}`)
        }
        const dashboardData = await response.json()

        // Extract top_hashtags from latest batch
        if (dashboardData.latest && dashboardData.latest.top_hashtags) {
          setTrends({
            timestamp: dashboardData.latest.timestamp,
            top_hashtags: dashboardData.latest.top_hashtags,
            top_hashtags_1h: dashboardData.latest.top_hashtags_1h || []
          })
        } else {
          setTrends(null)
        }
        setError(null)
      } catch (err) {
        console.error('Error fetching trends:', err)
        setError(err.message)
      } finally {
        setLoading(false)
      }
    }

    fetchTrends()
    const interval = setInterval(fetchTrends, 60000) // Poll every 60 seconds
    return () => clearInterval(interval)
  }, [])

  if (loading) {
    return (
      <div className="trend-hashtags">
        <h2>Trend Hashtags</h2>
        <p>Loading...</p>
      </div>
    )
  }

  if (error) {
    return (
      <div className="trend-hashtags">
        <h2>Trend Hashtags</h2>
        <p className="error">Error loading trends: {error}</p>
      </div>
    )
  }

  if (!trends || (!trends.top_hashtags || trends.top_hashtags.length === 0) && (!trends.top_hashtags_1h || trends.top_hashtags_1h.length === 0)) {
    return (
      <div className="trend-hashtags">
        <h2>Trend Hashtags</h2>
        <p>No trend data available</p>
      </div>
    )
  }

  const renderTable = (hashtags) => (
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
            <td className="rank">{item.rank}</td>
            <td className="hashtag">#{item.tag}</td>
            <td className="count">{item.count}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )

  return (
    <div className="trend-hashtags">
      <h2>Trend Hashtags (Top 10)</h2>
      <div className="timestamp">Updated: {trends.timestamp}</div>
      <div className="trend-container">
        <div className="trend-section-1h">
          <h3>Trend Hashtags 1H</h3>
          {trends.top_hashtags_1h && trends.top_hashtags_1h.length > 0 ? (
            renderTable(trends.top_hashtags_1h)
          ) : (
            <p className="no-data">No trend data available</p>
          )}
        </div>
        <div className="trend-section-all">
          <h3>Trend Hashtags ALL</h3>
          {trends.top_hashtags && trends.top_hashtags.length > 0 ? (
            renderTable(trends.top_hashtags)
          ) : (
            <p className="no-data">No trend data available</p>
          )}
        </div>
      </div>
    </div>
  )
}
