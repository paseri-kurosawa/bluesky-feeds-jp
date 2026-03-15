import { useState, useEffect } from 'react'
import { LineChart } from './components/LineChart'
import { LatestReport } from './components/LatestReport'
import { DistributionChart } from './components/DistributionChart'
import './App.css'

export default function App() {
  const [stats, setStats] = useState([])
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(true)

  const parseStats = async (file) => {
    const jsonResponse = await fetch(
      `https://bluesky-feed-dashboard-878311109818.s3.ap-northeast-1.amazonaws.com/${file}`
    )
    if (!jsonResponse.ok) {
      throw new Error(`Failed to fetch ${file}: ${jsonResponse.status}`)
    }
    const jsonData = await jsonResponse.json()
    return {
      ...jsonData,
      filename: file,
      timestamp: jsonData.execution_time
    }
  }

  const fetchStats = async () => {
    try {
      // Get list of stat files from index JSON
      const indexResponse = await fetch(
        `https://bluesky-feed-dashboard-878311109818.s3.ap-northeast-1.amazonaws.com/stats-index.json`
      )

      if (!indexResponse.ok) {
        throw new Error(`HTTP error! status: ${indexResponse.status}`)
      }

      const files = await indexResponse.json()
      const sortedFiles = files.sort().reverse().slice(0, 50) // Get last 50 files

      // Fetch content of each file
      const statsData = []
      for (const file of sortedFiles) {
        try {
          const parsed = await parseStats(file)
          statsData.push(parsed)
        } catch (e) {
          console.error(`Failed to fetch ${file}:`, e)
        }
      }

      setStats(statsData.reverse()) // Oldest first
      setError(null)
    } catch (err) {
      console.error('Error fetching stats:', err)
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchStats()
    const interval = setInterval(fetchStats, 60000) // Poll every 60 seconds
    return () => clearInterval(interval)
  }, [])

  if (loading && stats.length === 0) {
    return <div className="app-container"><p>Loading...</p></div>
  }

  return (
    <div className="app-container">
      <header className="app-header">
        <h1>Bluesky Feed Statistics Dashboard</h1>
        <div className="refresh-info">
          Last updated: {stats.length > 0 ? stats[stats.length - 1].timestamp : 'N/A'}
          {error && <div className="error-message">{error}</div>}
        </div>
      </header>

      <main className="dashboard-grid">
        {stats.length > 0 && (
          <>
            <>
              <div className="latest-report-header">
                <h2 className="latest-report-title">Latest Report</h2>
                <span className="latest-report-timestamp">
                  Executed: {stats.length > 0 ? stats[stats.length - 1].timestamp : 'N/A'}
                </span>
              </div>
              <section className="section latest-report">
                <LatestReport data={stats[stats.length - 1]} showTitle={false} />
              </section>
            </>

            <section className="section distributions">
              <h2>Distribution & Stats</h2>
              <DistributionChart data={stats[stats.length - 1]} />
            </section>

            <section className="section time-series">
              <h2>Processing Trends (Last 24h)</h2>
              <LineChart data={stats} />
            </section>
          </>
        )}
      </main>
    </div>
  )
}
