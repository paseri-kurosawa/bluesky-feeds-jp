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
  const bucketUrl = 'https://bluesky-feed-dashboard-878311109818.s3.ap-northeast-1.amazonaws.com'

  const fetchDashboardData = async () => {
    try {
      // Fetch latest report from components
      const latestUrl = `${bucketUrl}/components/latest_report.json`
      const latestResponse = await fetch(latestUrl)
      if (latestResponse.ok) {
        const latestData = await latestResponse.json()
        setLatestBatch(latestData)
      }

      // Fetch daily stats from components (or fallback to dashboard.json)
      const dailyUrl = `${bucketUrl}/components/processing_trends.json`
      const dailyResponse = await fetch(dailyUrl)
      if (dailyResponse.ok) {
        const dailyData = await dailyResponse.json()
        setDailyStats(Array.isArray(dailyData) ? dailyData : dailyData.daily || [])
      } else {
        // Fallback to dashboard.json for daily stats
        const dashboardUrl = `${bucketUrl}/stats/summary/dashboard.json`
        const dashboardResponse = await fetch(dashboardUrl)
        if (dashboardResponse.ok) {
          const dashboardData = await dashboardResponse.json()
          if (dashboardData.daily && dashboardData.daily.length > 0) {
            setDailyStats(dashboardData.daily)
          }
        }
      }

      setError(null)
    } catch (err) {
      console.error('Error fetching dashboard data:', err)
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchDashboardData()
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
            <section className="section latest-report-section">
              <div className="latest-report-header">
                <h2 className="latest-report-title">Latest Report</h2>
                <span className="latest-report-timestamp">
                  Executed: {latestBatch.execution_time || 'N/A'}
                </span>
              </div>
              <div className="latest-report">
                <LatestReport data={latestBatch} showTitle={false} />
              </div>
            </section>

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
