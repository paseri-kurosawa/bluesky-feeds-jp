import { useState, useEffect } from 'react'
import { LineChart } from './components/LineChart'
import { LatestReport } from './components/LatestReport'
import { DistributionChart } from './components/DistributionChart'
import { TrendHashtags } from './components/TrendHashtags'
import './App.css'

export default function App() {
  const [latestBatch, setLatestBatch] = useState(null)
  const [dailyStats, setDailyStats] = useState([])
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(true)

  const fetchDashboardData = async () => {
    try {
      // Fetch consolidated dashboard data (latest batch + daily stats)
      const dashboardUrl = `https://bluesky-feed-dashboard-878311109818.s3.ap-northeast-1.amazonaws.com/stats/summary/dashboard.json`

      const response = await fetch(dashboardUrl)
      if (!response.ok) {
        throw new Error(`Failed to fetch dashboard data: ${response.status}`)
      }

      const dashboardData = await response.json()

      // Set latest batch from dashboard data
      if (dashboardData.latest) {
        setLatestBatch(dashboardData.latest)
      }

      // Set daily stats from dashboard data
      if (dashboardData.daily && dashboardData.daily.length > 0) {
        setDailyStats(dashboardData.daily)
      }
    } catch (err) {
      console.error('Error fetching dashboard data:', err)
      setError(err.message)
    }
  }

  const fetchStats = async () => {
    try {
      // Fetch consolidated dashboard data
      await fetchDashboardData()
      setError(null)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchStats()
    const interval = setInterval(fetchStats, 60000) // Poll every 60 seconds
    return () => clearInterval(interval)
  }, [])

  if (loading && !latestBatch) {
    return <div className="app-container"><p>Loading...</p></div>
  }

  return (
    <div className="app-container">
      <header className="app-header">
        <h1>Bluesky Feed Statistics Dashboard</h1>
        <div className="refresh-info">
          Last updated: {latestBatch ? latestBatch.execution_time : 'N/A'}
          {error && <div className="error-message">{error}</div>}
        </div>
      </header>

      <main className="dashboard-grid">
        {latestBatch && (
          <>
            <>
              <div className="latest-report-header">
                <h2 className="latest-report-title">Latest Report</h2>
                <span className="latest-report-timestamp">
                  Executed: {latestBatch.execution_time || 'N/A'}
                </span>
              </div>
              <section className="section latest-report">
                <LatestReport data={latestBatch} showTitle={false} />
              </section>
            </>

            <section className="section distributions">
              <h2>Distribution & Stats</h2>
              <DistributionChart data={latestBatch} />
            </section>

            <section className="section trend-hashtags">
              <TrendHashtags />
            </section>

            <section className="section time-series">
              <h2>Processing Trends</h2>
              <LineChart data={dailyStats} />
            </section>
          </>
        )}
      </main>
    </div>
  )
}
