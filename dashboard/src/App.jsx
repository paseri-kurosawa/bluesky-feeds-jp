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
      // Fetch latest batch stats from stats/batch/
      const bucketUrl = `https://bluesky-feed-dashboard-878311109818.s3.ap-northeast-1.amazonaws.com`

      // List batch files and get the latest
      const response = await fetch(`${bucketUrl}/stats/batch/?list-type=2&prefix=stats/batch/`)
      if (!response.ok) {
        throw new Error(`Failed to list batch files: ${response.status}`)
      }

      const text = await response.text()
      const parser = new DOMParser()
      const xmlDoc = parser.parseFromString(text, 'text/xml')
      const contents = xmlDoc.getElementsByTagName('Contents')

      if (contents.length === 0) {
        throw new Error('No batch files found')
      }

      // Get latest file (last one)
      const latestKey = contents[contents.length - 1].getElementsByTagName('Key')[0].textContent

      const batchResponse = await fetch(`${bucketUrl}/${latestKey}`)
      if (!batchResponse.ok) {
        throw new Error(`Failed to fetch ${latestKey}: ${batchResponse.status}`)
      }

      const batchData = await batchResponse.json()
      setLatestBatch({
        ...batchData,
        filename: latestKey
      })
    } catch (err) {
      console.error('Error fetching latest batch:', err)
      setError(err.message)
    }
  }

  const fetchDailyStats = async () => {
    try {
      // Fetch daily stats from stats/daily/stats-YYYY.json
      const year = new Date().getFullYear()
      const dailyUrl = `https://bluesky-feed-dashboard-878311109818.s3.ap-northeast-1.amazonaws.com/stats/daily/stats-${year}.json`

      const response = await fetch(dailyUrl)
      if (!response.ok) {
        if (response.status === 404) {
          console.info(`Daily stats file not found yet: ${dailyUrl}`)
          setDailyStats([])
          return
        }
        throw new Error(`Failed to fetch daily stats: ${response.status}`)
      }

      const data = await response.json()
      setDailyStats(data)
    } catch (err) {
      console.error('Error fetching daily stats:', err)
      setError(err.message)
    }
  }

  const fetchStats = async () => {
    try {
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
