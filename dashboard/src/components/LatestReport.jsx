import './LatestReport.css'

export function LatestReport({ data }) {
  if (!data || !data.tables) {
    return <p>No data available</p>
  }

  const processingTable = data.tables['Processing Summary'] || []
  const badwordTable = data.tables['Badword Analysis'] || []
  const denseTable = data.tables['Dense Feed Statistics'] || []

  return (
    <div className="latest-report">
      <div className="report-section">
        <h3>Processing Summary</h3>
        <table className="metrics-table">
          <thead>
            <tr>
              <th>Item</th>
              <th>Count</th>
              <th>Rate</th>
            </tr>
          </thead>
          <tbody>
            {processingTable.slice(1).map((row, idx) => (
              <tr key={idx} className={row[2]?.includes('91.8') ? 'passed' : ''}>
                <td>{row[0]}</td>
                <td className="number">{row[1]}</td>
                <td className="percentage">{row[2]}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="report-section">
        <h3>Badword Analysis</h3>
        <table className="metrics-table">
          <thead>
            <tr>
              <th>Metric</th>
              <th>Value</th>
            </tr>
          </thead>
          <tbody>
            {badwordTable.map((row, idx) => (
              <tr key={idx}>
                <td>{row[0]}</td>
                <td className="number">{row[1]}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="report-section">
        <h3>Dense Feed Statistics</h3>
        <table className="metrics-table">
          <thead>
            <tr>
              <th>Metric</th>
              <th>Value</th>
            </tr>
          </thead>
          <tbody>
            {denseTable.map((row, idx) => (
              <tr key={idx}>
                <td>{row[0]}</td>
                <td className="number">{row[1]}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
