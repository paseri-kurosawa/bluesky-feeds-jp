import { useState, useEffect } from 'react'
import { LineChart } from './components/LineChart'
import { LatestReport } from './components/LatestReport'
import { DistributionChart } from './components/DistributionChart'
import './App.css'

export default function App() {
  const [latestBatch, setLatestBatch] = useState(null)
  const [dailyStats, setDailyStats] = useState([])
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(true)

  const fetchLatestBatch = async () => {
    try {
      // Fetch latest batch (single run)
      const latestBatchUrl = `https://bluesky-feed-dashboard-878311109818.s3.ap-northeast-1.amazonaws.com/stats/batch/latest.json`

      const batchResponse = await fetch(latestBatchUrl)
      if (!batchResponse.ok) {
        throw new Error(`Failed to fetch latest batch: ${batchResponse.status}`)
      }

      const batchData = await batchResponse.json()
      setLatestBatch(batchData)
    } catch (err) {
      console.error('Error fetching latest batch:', err)
      setError(err.message)
    }
  }

  const fetchDailyStats = async () => {
    try {
      // Fetch daily aggregated stats
      const summaryUrl = `https://bluesky-feed-dashboard-878311109818.s3.ap-northeast-1.amazonaws.com/stats/summary/dashboard.json`

      const summaryResponse = await fetch(summaryUrl)
      if (!summaryResponse.ok) {
        throw new Error(`Failed to fetch summary: ${summaryResponse.status}`)
      }

      const summaryData = await summaryResponse.json()

      if (summaryData.data && summaryData.data.length > 0) {
        setDailyStats(summaryData.data)
      } else {
        throw new Error('No data in summary')
      }
    } catch (err) {
      console.error('Error fetching daily stats:', err)
      setError(err.message)
    }
  }

  const fetchStats = async () => {
    try {
      // Fetch latest batch and daily stats in parallel
      await Promise.all([fetchLatestBatch(), fetchDailyStats()])
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
