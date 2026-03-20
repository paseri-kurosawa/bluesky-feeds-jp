import { useState, useEffect } from 'react'
import './TrendHashtags.css'

export function TrendHashtags() {
  const [trends, setTrends] = useState(null)
  const [error, setError] = useState(null)

  useEffect(() => {
    const fetchTrends = async () => {
      try {
        const trendsUrl = `https://bluesky-feed-dashboard-878311109818.s3.ap-northeast-1.amazonaws.com/stats/trends/trends-current.json`
        const response = await fetch(trendsUrl)
        if (!response.ok) {
          throw new Error(`Failed to fetch trends: ${response.status}`)
        }
        const trendsData = await response.json()
        setTrends(trendsData)
        setError(null)
      } catch (err) {
        console.error('Error fetching trends:', err)
        setError(err.message)
      }
    }

    fetchTrends()
    const interval = setInterval(fetchTrends, 60000) // Poll every 60 seconds
    return () => clearInterval(interval)
  }, [])

  if (error) {
    return (
      <div className="trend-hashtags">
        <h2>Trend Hashtags</h2>
        <p className="error">Error loading trends: {error}</p>
      </div>
    )
  }

  if (!trends || !trends.top_hashtags || trends.top_hashtags.length === 0) {
    return (
      <div className="trend-hashtags">
        <h2>Trend Hashtags</h2>
        <p>No trend data available</p>
      </div>
    )
  }

  return (
    <div className="trend-hashtags">
      <h2>Trend Hashtags (Top 10)</h2>
      <div className="timestamp">Updated: {trends.timestamp}</div>
      <table className="trend-table">
        <thead>
          <tr>
            <th className="rank">Rank</th>
            <th className="hashtag">Hashtag</th>
            <th className="count">Count</th>
          </tr>
        </thead>
        <tbody>
          {trends.top_hashtags.map((item, idx) => (
            <tr key={idx} className={idx === 0 ? 'top-trend' : ''}>
              <td className="rank">{item.rank}</td>
              <td className="hashtag">#{item.tag}</td>
              <td className="count">{item.count}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
