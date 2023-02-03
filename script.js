// fetch current officer and complaint records from ccrb site
// save as csv and json

import * as d3 from 'd3'
import fetch from 'node-fetch'
import { promises as fs } from 'fs'

async function request({ query, headers }) {
  const response = await fetch(QUERY.url, {
    headers,
    body: JSON.stringify(query),
    method: 'POST'
  })
  const result = await response.json()

  return result
}

// see prior work https://git.io/JONBX
function processRows(response) {
  const DS0 = response.results[0].result.data.dsr.DS[0]

  const data = DS0.PH[0].DM0 // rows of results
  const columnCount = response.results[0].result.data.descriptor.Select.length // number of cols in response
  const lookupTable = DS0.ValueDicts // lookup for values
  const lookupKeys = DS0.PH[0].DM0[0].S // data type and key for lookup

  let prevRowCache = [] // lookup for previous row values

  const rows = data.map(entry => {
    let row = []

    const rowCacheMask = entry.R || 0
    const nullMask = entry['Ø'] || 0
    entry = entry.C.reverse()

    for (let i = 0; i < columnCount; i++) {
      let value

      const colMask = 1 << i
      const isRowCache = Boolean(rowCacheMask & colMask)
      const isNull = Boolean(nullMask & colMask)

      if (isRowCache) {
        value = prevRowCache[i]
      } else if (isNull) {
        value = null
        prevRowCache[i] = null
      } else {
        value = entry.pop()
        prevRowCache[i] = value
      }

      const key = lookupKeys[i]

      if (Number.isInteger(value)) {
        if (key.T === 7) { // date
          const date = new Date(value)
          row.push(`${date.getUTCMonth()+1}/${date.getUTCDate()}/${date.getUTCFullYear()}`)
        } else if (key.T === 1 ) { // text
          row.push(lookupTable[key.DN][value])
        } else {
          row.push(value)
        }
      } else {
        row.push(value)
      }

    }

    return row
  })

  return rows
}

function getRestartToken(response) {
  return response.results[0].result.data.dsr.DS[0].RT
}

function setRestartToken({ query, token }) {
  let response = JSON.parse(JSON.stringify(query))
  response.queries[0].Query.Commands[0].SemanticQueryDataShapeCommand.Binding.DataReduction.Primary.Window.RestartTokens = token
  return response
}

async function fetchRecords({ type, active }) {
  let query = QUERY[type]
  let headers = HEADERS
  let page = 1
  let rows = []
  let token

  if (active) {
    query.queries[0].ApplicationContext.DatasetId = '523ab509-8e2d-43ed-bfad-11fcd05180d7'
    query.queries[0].ApplicationContext.Sources.ReportId = 'f508555a-b39d-4c10-8d46-a14bc282e079'
    query.modelId = 404287
    headers = {
      ...HEADERS,
      'X-PowerBI-ResourceKey': 'b2c8d2f2-3ad1-48dc-883c-d4163a6e2d8f'
    }
  } else {
    query.queries[0].ApplicationContext.DatasetId = 'e9651248-cbdf-498c-8b19-c7bdfbe87cc3'
    query.queries[0].ApplicationContext.Sources.ReportId = '737c2470-a2eb-4f5b-a133-bb9e089c3a65'
    query.modelId = 404284
    headers = {
      ...HEADERS,
      'X-PowerBI-ResourceKey': '87914378-578f-4f43-b75e-8ddaeafbdda2'
    }
  }

  const arrayToObj = {
    officers: function(row) {
      return {
        id: row[0],
        command: row[1],
        last_name: row[2],
        first_name: row[3],
        rank: row[4],
        shield_no: row[5],
        active
      }
    },
    complaints: function(row) {
      return {
        officer_id: row[0],
        complaint_id: row[1],
        complaint_date: row[2],
        fado_type: row[3],
        allegation: row[4],
        board_disposition: row[5],
        nypd_disposition: row[6],
        penalty_desc: row[7],
        // ignore officer_allegation_id and active
      }
    }
  }

  do {
    process.stdout.write(` ${page}`)
    const response = await request({ query, headers })
    let processed = processRows(response)
    processed = processed.map(arrayToObj[type])
    rows.push(...processed)
    token = getRestartToken(response)
    if (token) {
      rows.pop() // if we restart, dont dupe the row
      query = setRestartToken({ token, query })
    }
    page++
  } while (token)

  return rows
}

async function save({ officers, complaints, closingReports, departureLetters }) {
  complaints = complaints.map(complaint => {
    const penalties = ['No penalty']
    penalties.forEach(penalty => {
      if (complaint.penalty_desc?.toLowerCase() === penalty.toLowerCase()) {
        complaint.penalty_desc = penalty
      }
    })
    const allegations = ['Gun pointed']
    allegations.forEach(allegation => {
      if (complaint.allegation?.toLowerCase() === allegation.toLowerCase()) {
        complaint.allegation = allegation
      }
    })
    return complaint
  })

  complaints.sort((a,b) => {
    const props = [
      'complaint_id', 'officer_id', 'fado_type', 'allegation',
      'board_disposition', 'nypd_disposition', 'penalty_desc'
    ]
    for (let i = 0; i < props.length; i++) {
      const prop = props[i]
      if (a[prop] < b[prop]) { return -1 }
      if (a[prop] > b[prop]) { return 1 }
    }
    return 0
  })

  officers = officers.map(officer => {
    officer.first_name = officer.first_name.toUpperCase()
    officer.last_name = officer.last_name.toUpperCase()
    return officer
  })

  officers.sort((a,b) => {
    if (a.id < b.id) { return -1 }
    if (a.id > b.id) { return 1 }
    return 0
  })

  await fs.writeFile('officers.json', JSON.stringify(officers, null, 2))
  await fs.writeFile('officers.csv', d3.csvFormat(officers))

  await fs.writeFile('complaints.json', JSON.stringify(complaints, null, 2))
  await fs.writeFile('complaints.csv', d3.csvFormat(complaints))

  // closing reports and departure letters, sort by ComplaintId
  closingReports.sort((a,b) => {
    if (a.ComplaintId < b.ComplaintId) { return -1 }
    if (a.ComplaintId > b.ComplaintId) { return 1 }
    return 0
  })
  departureLetters.sort((a,b) => {
    if (a.CaseNumber < b.CaseNumber) { return -1 }
    if (a.CaseNumber > b.CaseNumber) { return 1 }
    if (a.LastName < b.LastName) { return -1 }
    if (a.LastName > b.LastName) { return 1 }
    return 0
  })

  await fs.writeFile('closingreports.csv', d3.csvFormat(closingReports))
  await fs.writeFile('departureletters.csv', d3.csvFormat(departureLetters))

  closingReports = {
    totalPosted: closingReports[0].totalPosted,
    totalClosedCase: closingReports[0].totalClosedCase,
    lastPublishDate: closingReports[0].LastPublishDate,
    closingReports: closingReports.map(report => {
      delete report.totalPosted
      delete report.totalClosedCase
      delete report.LastPublishDate
      return report
    })
  }
  departureLetters = {
    totalPosted: departureLetters[0].totalPosted,
    totalClosedCase: departureLetters[0].totalClosedCase,
    lastPublishDate: departureLetters[0].LastPublishDate,
    departureLetters: departureLetters.map(letter => {
      delete letter.Id
      delete letter.LastPublishDate
      return letter
    })
  }
  await fs.writeFile('closingreports.json', JSON.stringify(closingReports, null, 2))
  await fs.writeFile('departureletters.json', JSON.stringify(departureLetters, null, 2))

  let officerById = {}
  officers.forEach(officer => { officerById[officer.id] = officer })
  let closingReportsById = {}
  closingReports.closingReports.forEach(report => { closingReportsById[report.ComplaintId] = report })
  let departureLettersById = {}
  departureLetters.departureLetters.forEach(letter => { departureLettersById[letter.CaseNumber] = letter })

  const combined = complaints.map(complaint => {
    const officer_id = complaint.officer_id
    delete complaint.officer_id

    let record = {
      officer_id,
      ...officerById[officer_id],
      ...complaint,
      closing_report_url: closingReportsById[complaint.complaint_id]?.WebsiteDocumentFileName || null,
      departure_letter_url: departureLettersById[complaint.complaint_id]?.FileLink || null
    }

    Object.keys(record).forEach(key => {
      if (record[key] === null) {
        delete record[key]
      }
    })

    delete record.id

    return record
  })

  await fs.writeFile('records.json', JSON.stringify(combined, null, 2))
  await fs.writeFile('records.csv', d3.csvFormat(combined))
}

async function fetchClosingReports() {
  return fetchCcrbCsv(
      'https://www.nyc.gov/assets/ccrb/csv/closing-reports/redacted-closing-reports.csv',
      'WebsiteDocumentFileName',
      'https://www1.nyc.gov/assets/ccrb/downloads/pdf/closing-reports/')
}

async function fetchDepartureLetters() {
  return fetchCcrbCsv(
      'https://www.nyc.gov/assets/ccrb/csv/departure-letter/RedactedDepartureLetters.csv',
      'FileLink',
      'https://www1.nyc.gov/assets/ccrb/downloads/pdf/complaints/complaint-outcomes/redacted-departure-letters/')
}

async function fetchCcrbCsv(url, docField, docPrefix) {
  const response = await fetch(url)
  const buffer = await response.text()
  let records = await d3.csvParse(buffer)

  records.forEach(record => {
    record[docField] = docPrefix + record[docField]
  })

  return records
}

async function start() {
  let results = {
    complaints: [],
    officers: [],
    closingReports: [],
    departureLetters: []
  }

  for (const type of ['officers', 'complaints']) {
    for (const active of [true, false]) {
      console.log(`fetching ${active ? '': 'in'}active ${type}`)
      const rows = await fetchRecords({ type, active })
      results[type].push(...rows)
      console.log(`\nfetched ${rows.length} ${active ? '': 'in'}active ${type}`)
    }
  }

  results.closingReports = await fetchClosingReports()
  results.departureLetters = await fetchDepartureLetters()

  save(results)
}

////////////////////////////////////////////////////////////////////////////////

const HEADERS = {
  'Connection': 'keep-alive',
  'sec-ch-ua': '.Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': 'macOS',
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
  'ActivityId': 'dc745861-c4a7-0ef7-33bb-68aebbd8c7a4',
  'Accept': 'application/json, text/plain, */*',
  'RequestId': '8e3188a8-6b6e-967c-8d3d-25ba6fe6140d',
  'Content-Type': 'application/json;charset=UTF-8',
  'Origin': 'https://app.powerbigov.us',
  'Sec-Fetch-Site': 'cross-site',
  'Sec-Fetch-Mode': 'cors',
  'Sec-Fetch-Dest': 'empty',
  'Referer': 'https://app.powerbigov.us/',
  'Accept-Language': 'en-US,en;q=0.9',
}

const QUERY = {
  url: 'https://wabi-us-gov-virginia-api.analysis.usgovcloudapi.net/public/reports/querydata?synchronous=true',

  officers: {
    "version": "1.0.0",
    "queries": [
      {
        "Query": {
          "Commands": [
            {
              "SemanticQueryDataShapeCommand": {
                "Query": {
                  "Version": 2,
                  "From": [
                    { "Name": "q1", "Entity": "CCRB Active - Oracle", "Type": 0 }
                  ],
                  "Select": [
                    {
                      "Column": {
                        "Expression": { "SourceRef": { "Source": "q1" } },
                        "Property": "Unique Id"
                      },
                      "Name": "Query1.Unique Id"
                    },
                    {
                      "Column": {
                        "Expression": { "SourceRef": { "Source": "q1" } },
                        "Property": "Command"
                      },
                      "Name": "Query1.Command1"
                    },
                    {
                      "Column": {
                        "Expression": { "SourceRef": { "Source": "q1" } },
                        "Property": "Last Name"
                      },
                      "Name": "Query1.Last Name1"
                    },
                    {
                      "Column": {
                        "Expression": { "SourceRef": { "Source": "q1" } },
                        "Property": "First Name"
                      },
                      "Name": "Query1.First Name1"
                    },
                    {
                      "Column": {
                        "Expression": { "SourceRef": { "Source": "q1" } },
                        "Property": "Rank"
                      },
                      "Name": "Query1.Rank1"
                    },
                    {
                      "Column": {
                        "Expression": { "SourceRef": { "Source": "q1" } },
                        "Property": "Shield No"
                      },
                      "Name": "Query1.ShieldNo"
                    }
                  ],
                  "OrderBy": [
                    {
                      "Direction": 1,
                      "Expression": {
                        "Column": {
                          "Expression": { "SourceRef": { "Source": "q1" } },
                          "Property": "Unique Id"
                        }
                      }
                    }
                  ]
                },
                "Binding": {
                  "Primary": {
                    "Groupings": [
                      { "Projections": [ 0, 1, 2, 3, 4, 5 ] }
                    ]
                  },
                  "DataReduction": {
                    "DataVolume": 3,
                    "Primary": { "Window": { "Count": 10000 } } },
                  "Version": 1
                }
              }
            }
          ]
        },
        "QueryId": "",
        "ApplicationContext": {
          "DatasetId": "523ab509-8e2d-43ed-bfad-11fcd05180d7",
          "Sources": [
            { "ReportId": "f508555a-b39d-4c10-8d46-a14bc282e079" }
          ]
        }
      }
    ],
    "cancelQueries": [],
    "modelId": 404287
  },

  complaints: {
    "version": "1.0.0",
    "queries": [
      {
        "Query": {
          "Commands": [
            {
              "SemanticQueryDataShapeCommand": {
                "Query": {
                  "Version": 2,
                  "From": [
                    { "Name": "q1", "Entity": "CCRB Active - Oracle", "Type": 0 }
                  ],
                  "Select": [
                    {
                      "Column": {
                        "Expression": { "SourceRef": { "Source": "q1" } },
                        "Property": "Unique Id"
                      },
                      "Name": "Query1.Unique Id"
                    },
                    {
                      "Column": {
                        "Expression": { "SourceRef": { "Source": "q1" } },
                        "Property": "Complaint ID"
                      },
                      "Name": "CountNonNull(Query1.Complaint Id)1"
                    },
                    {
                      "Column": {
                        "Expression": { "SourceRef": { "Source": "q1" } },
                        "Property": "Incident Date"
                      },
                      "Name": "Query1.Incident Date"
                    },
                    {
                      "Column": {
                        "Expression": { "SourceRef": { "Source": "q1" } },
                        "Property": "FADO Type"
                      },
                      "Name": "Query1.FADO Type1"
                    },
                    {
                      "Column": {
                        "Expression": { "SourceRef": { "Source": "q1" } },
                        "Property": "Allegation"
                      },
                      "Name": "Query1.Allegation1"
                    },
                    {
                      "Column": {
                        "Expression": { "SourceRef": { "Source": "q1" } },
                        "Property": "Board Disposition"
                      },
                      "Name": "Query1.Board Disposition1"
                    },
                    {
                      "Column": {
                        "Expression": { "SourceRef": { "Source": "q1" } },
                        "Property": "NYPD Disposition"
                      },
                      "Name": "Query1.NYPD Disposition"
                    },
                    {
                      "Column": {
                        "Expression": { "SourceRef": { "Source": "q1" } },
                        "Property": "Penalty"
                      },
                      "Name": "Query1.PenaltyDesc1"
                    },
                    {
                      "Column": {
                        "Expression": { "SourceRef": { "Source": "q1" } },
                        "Property": "Rn"
                      },
                      "Name": "Sum(Query1.Rn)"
                    }
                  ],
                  "Where": [
                    {
                      "Condition": {
                        "Not": {
                          "Expression": {
                            "Comparison": {
                              "ComparisonKind": 0,
                              "Left": {
                                "Column": {
                                  "Expression": { "SourceRef": { "Source": "q1" } },
                                  "Property": "Rn"
                                }
                              },
                              "Right": { "Literal": { "Value": "0L" } }
                            }
                          }
                        }
                      }
                    }
                  ],
                  "OrderBy": [
                    {
                      "Direction": 1,
                      "Expression": {
                        "Column": {
                          "Expression": { "SourceRef": { "Source": "q1" } },
                          "Property": "Complaint ID"
                        }
                      }
                    }
                  ]
                },
                "Binding": {
                  "Primary": {
                    "Groupings": [
                      { "Projections": [ 0, 1, 2, 3, 4, 5, 6, 7, 8 ] }
                    ]
                  },
                  "DataReduction": {
                    "DataVolume": 3,
                    "Primary": { "Window": { "Count": 10000 } } },
                  "Version": 1
                }
              }
            }
          ]
        },
        "QueryId": "",
        "ApplicationContext": {
          "DatasetId": "523ab509-8e2d-43ed-bfad-11fcd05180d7",
          "Sources": [
            { "ReportId": "f508555a-b39d-4c10-8d46-a14bc282e079" }
          ]
        }
      }
    ],
    "cancelQueries": [],
    "modelId": 404287
  }
}

start()
