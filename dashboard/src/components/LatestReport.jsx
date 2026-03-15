import './LatestReport.css'

export function LatestReport({ data }) {
  if (!data) {
    return <p>No data available</p>
  }

  // Handle both JSON and Markdown formats
  if (data.format === 'json') {
    return <LatestReportJSON data={data} />
  } else {
    return <LatestReportMarkdown data={data} />
  }
}

function LatestReportJSON({ data }) {
  const ps = data.processing_summary
  const ba = data.badword_analysis
  const df = data.dense_feed

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
            <tr>
              <td>Total Fetched</td>
              <td className="number">{ps.total_fetched}</td>
              <td className="percentage">100.0%</td>
            </tr>
            <tr>
              <td>Invalid Fields</td>
              <td className="number">{ps.invalid_fields}</td>
              <td className="percentage">{ps.rates.invalid_fields_rate}%</td>
            </tr>
            <tr>
              <td>Moderation Labels</td>
              <td className="number">{ps.moderation_labels}</td>
              <td className="percentage">{ps.rates.moderation_labels_rate}%</td>
            </tr>
            <tr>
              <td>Non-Japanese</td>
              <td className="number">{ps.non_japanese}</td>
              <td className="percentage">{ps.rates.non_japanese_rate}%</td>
            </tr>
            <tr className="passed">
              <td><strong>Passed Filters</strong></td>
              <td className="number"><strong>{ps.passed_filters}</strong></td>
              <td className="percentage"><strong>{ps.rates.passed_filters_rate}%</strong></td>
            </tr>
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
            <tr>
              <td>Posts with Badwords</td>
              <td className="number">{ba.posts_with_badwords}</td>
            </tr>
            <tr>
              <td>Hit Rate</td>
              <td className="number">{ba.hit_rate}%</td>
            </tr>
            <tr>
              <td>Total Matches</td>
              <td className="number">{ba.total_matches}</td>
            </tr>
            <tr>
              <td>Avg Matches per Hit</td>
              <td className="number">{ba.avg_matches_per_hit}</td>
            </tr>
          </tbody>
        </table>

        {ba.matched_words && ba.matched_words.length > 0 && (
          <div className="matched-words">
            <h4>Matched Badwords</h4>
            <ul>
              {ba.matched_words.slice(0, 10).map((item, idx) => (
                <li key={idx}>
                  <strong>{item.word}</strong>: {item.count} match(es)
                </li>
              ))}
            </ul>
          </div>
        )}
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
            <tr>
              <td>Total Items</td>
              <td className="number">{df.total_items}</td>
            </tr>
            <tr>
              <td>Text Only Short</td>
              <td className="number">{df.text_only_short}</td>
            </tr>
            <tr>
              <td>Dense Posts</td>
              <td className="number">{df.dense_posts}</td>
            </tr>
            <tr>
              <td>Dense Rate</td>
              <td className="number">{df.dense_rate}%</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  )
}

function LatestReportMarkdown({ data }) {
  // Backward compatibility: use markdown format
  if (!data.tables) {
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
