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
  if (!data || !data.tables) {
    return <p>No data available</p>
  }

  const denseTable = data.tables['Dense Feed Statistics'] || []
  const processingTable = data.tables['Processing Summary'] || []

  // Parse dense feed data (denseTable[0] is header, [1]+ is data)
  const totalItems = parseInt(denseTable[0]?.[1]) || 0      // Total Items
  const textOnlyShort = parseInt(denseTable[1]?.[1]) || 0   // Text Only Short
  const densePosts = parseInt(denseTable[2]?.[1]) || 0      // Dense Posts
  const notDense = totalItems - densePosts

  // Parse filter data (processingTable[0] is header, [1]+ is data)
  const totalFetched = parseInt(processingTable[0]?.[1]) || 0    // Total Fetched
  const modLabels = parseInt(processingTable[2]?.[1]) || 0       // Moderation Labels
  const nonJapanese = parseInt(processingTable[3]?.[1]) || 0     // Non-Japanese
  const passed = parseInt(processingTable[4]?.[1]) || 0          // Passed Filters

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

  // Filter breakdown bar chart
  const filterChartData = {
    labels: ['Moderation\nLabels', 'Non-Japanese', 'Passed\nFilters'],
    datasets: [{
      label: 'Count',
      data: [modLabels, nonJapanese, passed],
      backgroundColor: [
        '#ef4444',
        '#f97316',
        '#22c55e'
      ],
      borderColor: ['#dc2626', '#ea580c', '#16a34a'],
      borderWidth: 1
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
    indexAxis: 'y',
    plugins: {
      legend: {
        display: false
      }
    },
    scales: {
      x: {
        beginAtZero: true,
        max: totalFetched
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
        <div className="bar-chart">
          <Bar data={filterChartData} options={filterOptions} />
        </div>
      </div>
    </div>
  )
}
