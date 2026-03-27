import { useState, useEffect } from 'react'
import { LineChart } from './components/LineChart'
import { LatestReport } from './components/LatestReport'
import { DistributionChart } from './components/DistributionChart'
import { TrendHashtags } from './components/TrendHashtags'
import './App.css'

export default function App() {
  const [latestBatchRaw, setLatestBatchRaw] = useState(null)
  const [latestBatchStablehashtag, setLatestBatchStablehashtag] = useState(null)
  const [dailyStatsRaw, setDailyStatsRaw] = useState([])
  const [dailyStatsStablehashtag, setDailyStatsStablehashtag] = useState([])
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(true)
  const [processingTrendTab, setProcessingTrendTab] = useState('raw-dense') // 'raw-dense' or 'stablehashtag'
  const bucketUrl = 'https://bluesky-feed-dashboard-878311109818.s3.ap-northeast-1.amazonaws.com'

  const fetchDashboardData = async () => {
    try {
      // Fetch QUERY 1 (raw-dense) latest report
      const latestRawUrl = `${bucketUrl}/components/latest_report_raw-dense.json`
      const latestRawResponse = await fetch(latestRawUrl)
      if (latestRawResponse.ok) {
        const latestData = await latestRawResponse.json()
        setLatestBatchRaw(latestData)
      }

      // Fetch QUERY 2 (stablehashtag) latest report
      const latestStablehashtagUrl = `${bucketUrl}/components/latest_report_stablehashtag.json`
      const latestStablehashtagResponse = await fetch(latestStablehashtagUrl)
      if (latestStablehashtagResponse.ok) {
        const latestData = await latestStablehashtagResponse.json()
        setLatestBatchStablehashtag(latestData)
      }

      // Fetch QUERY 1 processing trends
      const dailyRawUrl = `${bucketUrl}/components/processing_trends_raw-dense.json`
      const dailyRawResponse = await fetch(dailyRawUrl)
      if (dailyRawResponse.ok) {
        const dailyData = await dailyRawResponse.json()
        setDailyStatsRaw(Array.isArray(dailyData) ? dailyData : dailyData.daily || [])
      }

      // Fetch QUERY 2 processing trends
      const dailyStablehashtagUrl = `${bucketUrl}/components/processing_trends_stablehashtag.json`
      const dailyStablehashtagResponse = await fetch(dailyStablehashtagUrl)
      if (dailyStablehashtagResponse.ok) {
        const dailyData = await dailyStablehashtagResponse.json()
        setDailyStatsStablehashtag(Array.isArray(dailyData) ? dailyData : dailyData.daily || [])
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

  if (loading && !latestBatchRaw && !latestBatchStablehashtag) {
    return <div className="app-container"><p>Loading...</p></div>
  }

  const latestTimestamp = latestBatchRaw?.execution_time || latestBatchStablehashtag?.execution_time || 'N/A'
  const currentDailyStats = processingTrendTab === 'raw-dense' ? dailyStatsRaw : dailyStatsStablehashtag

  return (
    <div className="app-container">
      <header className="app-header">
        <h1>Bluesky Feed Statistics Dashboard</h1>
        <div className="refresh-info">
          Last updated: {latestTimestamp}
          {error && <div className="error-message">{error}</div>}
        </div>
      </header>

      <main className="dashboard-grid">
        {/* Latest Report Section: 2 columns */}
        <section className="section latest-report-section latest-report-two-column">
          <div className="latest-report-header">
            <h2 className="latest-report-title">Latest Report</h2>
          </div>
          <div className="latest-report-row">
            {latestBatchRaw && (
              <div className="latest-report-column">
                <h3>Raw/Dense Feed</h3>
                <LatestReport data={latestBatchRaw} showTitle={false} />
              </div>
            )}
            {latestBatchStablehashtag && (
              <div className="latest-report-column">
                <h3>Stable Hashtag Feed</h3>
                <LatestReport data={latestBatchStablehashtag} showTitle={false} />
              </div>
            )}
          </div>
        </section>

        {/* Distribution Charts: 2 columns */}
        <section className="section distributions two-column">
          <div className="distribution-header">
            <h2>Distribution & Stats</h2>
          </div>
          <div className="distribution-row">
            {latestBatchRaw && (
              <div className="distribution-column">
                <h3>Raw/Dense Feed</h3>
                <DistributionChart data={latestBatchRaw} />
              </div>
            )}
            {latestBatchStablehashtag && (
              <div className="distribution-column">
                <h3>Stable Hashtag Feed</h3>
                <DistributionChart data={latestBatchStablehashtag} />
              </div>
            )}
          </div>
        </section>

        {/* Trend Hashtags */}
        <section className="section trend-hashtags">
          <TrendHashtags />
        </section>

        {/* Processing Trends with Tabs */}
        <section className="section time-series">
          <div className="trends-header">
            <h2>Processing Trends</h2>
            <div className="tab-buttons">
              <button
                className={`tab-button ${processingTrendTab === 'raw-dense' ? 'active' : ''}`}
                onClick={() => setProcessingTrendTab('raw-dense')}
              >
                Raw/Dense Feed
              </button>
              <button
                className={`tab-button ${processingTrendTab === 'stablehashtag' ? 'active' : ''}`}
                onClick={() => setProcessingTrendTab('stablehashtag')}
              >
                Stable Hashtag Feed
              </button>
            </div>
          </div>
          <LineChart data={currentDailyStats} />
        </section>
      </main>
    </div>
  )
}
