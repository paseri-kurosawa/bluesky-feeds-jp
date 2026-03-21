import './TrendHashtags.css'

export function TrendHashtags({ data }) {
  if (!data) {
    return (
      <div className="trend-hashtags">
        <h2>Trend Hashtags</h2>
        <p>No data available</p>
      </div>
    )
  }

  const trends = {
    timestamp: data.timestamp,
    top_hashtags: data.top_hashtags || [],
    top_hashtags_1h: data.top_hashtags_1h || []
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
