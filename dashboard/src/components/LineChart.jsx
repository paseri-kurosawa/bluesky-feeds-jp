import { Line } from 'react-chartjs-2'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
} from 'chart.js'
import './LineChart.css'

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
)

export function LineChart({ data }) {
  if (!data || data.length === 0) {
    return <p>No data available</p>
  }

  // Extract metrics from JSON format
  const metrics = data.map(d => ({
    timestamp: d.timestamp,
    totalFetched: d.processing_summary.total_fetched,
    passed: d.processing_summary.passed_filters,
    denseRate: d.dense_feed.dense_rate
  }))

  const labels = metrics.map(m => new Date(m.timestamp).toLocaleTimeString())

  const chartData = {
    labels,
    datasets: [
      {
        label: 'Total Fetched',
        data: metrics.map(m => m.totalFetched),
        borderColor: '#667eea',
        backgroundColor: 'rgba(102, 126, 234, 0.1)',
        tension: 0.4,
        fill: true
      },
      {
        label: 'Passed Filters',
        data: metrics.map(m => m.passed),
        borderColor: '#22c55e',
        backgroundColor: 'rgba(34, 197, 94, 0.1)',
        tension: 0.4,
        fill: true
      },
      {
        label: 'Dense Rate (%)',
        data: metrics.map(m => m.denseRate),
        borderColor: '#f59e0b',
        backgroundColor: 'rgba(245, 158, 11, 0.1)',
        tension: 0.4,
        fill: true,
        yAxisID: 'y1'
      }
    ]
  }

  const options = {
    responsive: true,
    maintainAspectRatio: true,
    interaction: {
      mode: 'index',
      intersect: false
    },
    plugins: {
      legend: {
        display: true,
        position: 'top'
      },
      tooltip: {
        backgroundColor: 'rgba(0, 0, 0, 0.8)',
        padding: 12,
        titleFont: { size: 13 }
      }
    },
    scales: {
      y: {
        type: 'linear',
        display: true,
        position: 'left',
        title: {
          display: true,
          text: 'Count'
        }
      },
      y1: {
        type: 'linear',
        display: true,
        position: 'right',
        title: {
          display: true,
          text: 'Dense Rate (%)'
        },
        grid: {
          drawOnChartArea: false
        }
      }
    }
  }

  return (
    <div className="line-chart-container">
      <Line data={chartData} options={options} />
    </div>
  )
}
