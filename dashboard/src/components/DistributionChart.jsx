import { Pie, Bar } from 'react-chartjs-2'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  ArcElement,
  Title,
  Tooltip,
  Legend
} from 'chart.js'
import './DistributionChart.css'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  ArcElement,
  Title,
  Tooltip,
  Legend
)

export function DistributionChart({ data }) {
  if (!data) {
    return <p>No data available</p>
  }

  // Check for dense_feed data
  if (!data.dense_feed) {
    return <p>No data available</p>
  }

  // Parse data from JSON format
  const totalItems = data.dense_feed.total_items
  const textOnlyShort = data.dense_feed.text_only_short
  const densePosts = data.dense_feed.dense_posts
  const totalFetched = data.processing_summary.total_fetched
  const modLabels = data.processing_summary.moderation_labels
  const nonJapanese = data.processing_summary.non_japanese
  const passed = data.processing_summary.passed_filters

  const notDense = totalItems - densePosts

  // Dense/Not Dense Pie Chart
  const denseChartData = {
    labels: ['Dense Posts', 'Other Posts'],
    datasets: [{
      data: [densePosts, notDense],
      backgroundColor: [
        '#667eea',
        '#e5e7eb'
      ],
      borderColor: ['#667eea', '#d1d5db'],
      borderWidth: 2
    }]
  }

  // Filter breakdown pie chart
  const filterChartData = {
    labels: ['Passed Filters', 'Moderation Labels', 'Non-Japanese'],
    datasets: [{
      data: [passed, modLabels, nonJapanese],
      backgroundColor: [
        '#86efac',
        '#ef4444',
        '#e5e7eb'
      ],
      borderColor: ['#4ade80', '#dc2626', '#d1d5db'],
      borderWidth: 2
    }]
  }

  const denseOptions = {
    responsive: true,
    maintainAspectRatio: true,
    plugins: {
      legend: {
        position: 'bottom'
      },
      tooltip: {
        callbacks: {
          label: function(context) {
            const label = context.label || ''
            const value = context.parsed
            const total = context.dataset.data.reduce((a, b) => a + b, 0)
            const percentage = ((value / total) * 100).toFixed(1)
            return `${label}: ${value} (${percentage}%)`
          }
        }
      }
    }
  }

  const filterOptions = {
    responsive: true,
    maintainAspectRatio: true,
    plugins: {
      legend: {
        position: 'bottom'
      },
      tooltip: {
        callbacks: {
          label: function(context) {
            const label = context.label || ''
            const value = context.parsed
            const total = context.dataset.data.reduce((a, b) => a + b, 0)
            const percentage = ((value / total) * 100).toFixed(1)
            return `${label}: ${value} (${percentage}%)`
          }
        }
      }
    }
  }

  return (
    <div className="distribution-container">
      <div className="chart-item">
        <h3>Dense Feed Ratio</h3>
        <div className="pie-chart">
          <Pie data={denseChartData} options={denseOptions} />
        </div>
        <p className="chart-stat">
          Dense Rate: <strong>{((densePosts / totalItems) * 100).toFixed(1)}%</strong>
        </p>
      </div>

      <div className="chart-item">
        <h3>Filter Breakdown</h3>
        <div className="pie-chart">
          <Pie data={filterChartData} options={filterOptions} />
        </div>
      </div>
    </div>
  )
}
