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
    // Try JSON first (better format)
    const jsonFile = file.replace('.md', '.json')
    try {
      const jsonResponse = await fetch(
        `https://bluesky-feed-statistics-878311109818.s3.ap-northeast-1.amazonaws.com/${jsonFile}`
      )
      if (jsonResponse.ok) {
        const jsonData = await jsonResponse.json()
        console.log(`[INFO] Parsed JSON: ${jsonFile}`)
        return {
          ...jsonData,
          filename: file,
          timestamp: jsonData.execution_time,
          format: 'json'
        }
      }
    } catch (e) {
      console.log(`[INFO] JSON not available for ${file}, falling back to markdown: ${e.message}`)
    }

    // Fallback to markdown parsing
    try {
      const mdResponse = await fetch(
        `https://bluesky-feed-statistics-878311109818.s3.ap-northeast-1.amazonaws.com/${file}`
      )
      if (!mdResponse.ok) {
        throw new Error(`Failed to fetch ${file}: ${mdResponse.status}`)
      }
      const mdContent = await mdResponse.text()
      const parsed = parseMarkdownStats(mdContent)
      console.log(`[INFO] Parsed Markdown: ${file}`)
      return {
        ...parsed,
        filename: file,
        format: 'markdown'
      }
    } catch (e) {
      console.error(`[ERROR] Failed to parse both JSON and Markdown for ${file}: ${e.message}`)
      throw e
    }
  }

  const parseMarkdownStats = (mdContent) => {
    const lines = mdContent.split('\n')
    const data = {}
    const tables = {}

    let currentSection = null
    let headerRow = null

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].trim()

      // Section header
      if (line.startsWith('## ')) {
        currentSection = line.replace('## ', '').trim()
        tables[currentSection] = []
        headerRow = null
      }

      // Table row
      if (line.startsWith('| ') && currentSection) {
        // Skip separator rows (|---|---|...)
        if (line.includes('---')) {
          continue
        }

        const cells = line.split('|').map(cell => cell.trim()).filter(cell => cell)

        // First data row is header
        if (!headerRow) {
          headerRow = cells
          continue
        }

        // Subsequent rows are data
        tables[currentSection].push(cells)
      }

      if (line.startsWith('**Execution Time:**')) {
        data.executionTime = line.replace('**Execution Time:**', '').trim()
      }
    }

    return { ...data, tables }
  }

  const fetchStats = async () => {
    try {
      // Get list of stat files from index JSON
      const indexResponse = await fetch(
        `https://bluesky-feed-statistics-878311109818.s3.ap-northeast-1.amazonaws.com/stats-index.json`
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
          const timestamp = extractTimestamp(file)
          parsed.timestamp = parsed.timestamp || timestamp
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

  const extractTimestamp = (filename) => {
    const match = filename.match(/stats_(\d{8})_(\d{6})/)
    if (match) {
      const date = match[1]
      const time = match[2]
      return `${date.slice(0, 4)}-${date.slice(4, 6)}-${date.slice(6, 8)} ${time.slice(0, 2)}:${time.slice(2, 4)}:${time.slice(4, 6)}`
    }
    return 'Unknown'
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
            <section className="section latest-report">
              <h2>Latest Report</h2>
              <LatestReport data={stats[stats.length - 1]} />
            </section>

            <section className="section time-series">
              <h2>Processing Trends (Last 24h)</h2>
              <LineChart data={stats} />
            </section>

            <section className="section distributions">
              <h2>Distribution & Stats</h2>
              <DistributionChart data={stats[stats.length - 1]} />
            </section>
          </>
        )}
      </main>
    </div>
  )
}
