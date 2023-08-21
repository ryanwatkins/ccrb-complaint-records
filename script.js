// fetch complaint records from nyc opendata
// and convert to previous formats

import fetch from 'node-fetch'
import { parse } from 'csv-parse/sync'
import { stringify } from 'csv-stringify/sync'
import { promises as fs } from 'fs'

async function save({ officers, allegations, closingReports, departureLetters }) {
  const csvOptions = {
    header: true,
    cast: {
      boolean: (value) => value ? 'true' : 'false'
    }
  }

  await fs.writeFile('officers.csv', stringify(officers, csvOptions))
  await fs.writeFile('officers.json', JSON.stringify(officers.map(stripRecord), null, 2))

  await fs.writeFile('complaints.csv', stringify(allegations, csvOptions))
  await fs.writeFile('complaints.json', JSON.stringify(allegations.map(stripRecord), null, 2))

  await fs.writeFile('closingreports.csv', stringify(closingReports, csvOptions))
  await fs.writeFile('departureletters.csv', stringify(departureLetters, csvOptions))

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

  const combined = allegations.map(allegation => {
    const officer_id = allegation.officer_id
    delete allegation.officer_id

    const officer = officerById[officer_id]
    const closingReport = closingReportsById[allegation.complaint_id]
    let departureLetter = departureLettersById[allegation.complaint_id]
    if ((departureLetter?.LastName.toUpperCase() !== officer?.last_name.replace('BRUCEWATSON', 'BRUCE').toUpperCase()) ||
        (departureLetter?.FirstName.substr(0, 10).toUpperCase() !== officer?.first_name.substr(0, 10).toUpperCase())) {
      departureLetter = null
    }

    let record = {
      officer_id,
      ...officer,
      ...allegation,
      closing_report_url: closingReport?.WebsiteDocumentFileName || null,
      departure_letter_url: departureLetter?.FileLink || null
    }

    delete record.id

    return record
  })

  await fs.writeFile('records.csv', stringify(combined, csvOptions))
  await fs.writeFile('records.json', JSON.stringify(combined.map(stripRecord), null, 2))
}

async function fetchComplaints() {
  const files = [
    { id: 'allegations', url: 'https://data.cityofnewyork.us/api/views/6xgr-kwjq/rows.csv?accessType=DOWNLOAD' },
    { id: 'complaints',  url: 'https://data.cityofnewyork.us/api/views/2mby-ccnw/rows.csv?accessType=DOWNLOAD' },
    { id: 'officers',    url: 'https://data.cityofnewyork.us/api/views/2fir-qns4/rows.csv?accessType=DOWNLOAD' },
    { id: 'penalties',   url: 'https://data.cityofnewyork.us/api/views/keep-pkmh/rows.csv?accessType=DOWNLOAD' },
  ]
  let results = {}

  for (const file of files) {
    console.info(file.url)
    const response = await fetch(file.url)
    const csv = await response.text()
    let records = parse(csv, { columns: true })

    records = records.map(record => {
      delete record['As Of Date'] // strip date to allow useful diffs
      delete record['Complaint Officer Number'] // strip officer number as they change
      return record
    })

    records.sort((a,b) => {
      const fields = [
        'Allegation Record Identity',
        'Complaint Id',
        'Tax ID',
        'Complaint Officer Number',
      ]
      for (const field of fields) {
        if (a[field] > b[field]) { return 1 }
        if (a[field] < b[field]) { return -1 }
      }
    })

    results[file.id] = records
    await fs.writeFile(`ccrb-complaints-database-${file.id}.csv`, stringify(records, { header: true }))
  }

  results = convertComplaints(results)

  return results
}

// convert to legacy formats
async function convertComplaints({ allegations, complaints, officers, penalties }) {
  allegations = allegations.filter(allegation => allegation['Tax ID'])

  let complaintsById = {}
  complaints.forEach(complaint => { complaintsById[complaint['Complaint Id']] = complaint })

  let penaltiesById = {}
  penalties.forEach(penalty => {
    if (!penalty['NYPD Officer Penalty']) return
    const id = `${penalty['Complaint Id']}:${penalty['Tax ID']}`
    penaltiesById[id] = penalty
  })

  allegations = allegations.map(record => {
    let allegation = {
      officer_id: record['Tax ID'],
      complaint_id: record['Complaint Id'],
      complaint_date: complaintsById[record['Complaint Id']]['Incident Date'],
      fado_type: record['FADO Type'],
      allegation: record['Allegation'],
      board_disposition: record['CCRB Allegation Disposition'],
      nypd_disposition: record['NYPD Allegation Disposition'],
      penalty_desc: '',
    }

    const penaltyId = `${record['Complaint Id']}:${record['Tax ID']}`
    if (penaltiesById[penaltyId] && allegation.board_disposition.startsWith('Substantiated')) {
      allegation.penalty_desc = penaltiesById[penaltyId]['NYPD Officer Penalty']
    }

    const fixcase = ['Gun pointed', 'No penalty']
    fixcase.forEach(entry => {
      if (allegation.allegation?.toLowerCase() === entry.toLowerCase()) {
        allegation.allegation = entry
      }
      if (allegation.penalty_desc?.toLowerCase() === entry.toLowerCase()) {
        allegation.penalty_desc = entry
      }
    })

    return allegation
  })

  officers = officers.map(record => {
    return {
      id: record['Tax ID'],
      command: record['Current Command'],
      last_name: record['Officer Last Name'].toUpperCase(),
      first_name: record['Officer First Name'].toUpperCase(),
      rank: record['Current Rank'],
      shield_no: record['Shield No'],
      active: (record['Active Per Last Reported Status'] === 'Yes') ? true : false,
    }
  })

  allegations.sort((a,b) => {
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

  officers.sort((a,b) => {
    if (a.id < b.id) { return -1 }
    if (a.id > b.id) { return 1 }
    return 0
  })

  return {
    allegations,
    officers,
  }
}

async function fetchClosingReports() {
  let closingReports = await fetchCcrbCsv(
    'https://www.nyc.gov/assets/ccrb/csv/closing-reports/redacted-closing-reports.csv',
    'WebsiteDocumentFileName',
    'https://www1.nyc.gov/assets/ccrb/downloads/pdf/closing-reports/')

  closingReports.sort((a,b) => {
    if (a.ComplaintId < b.ComplaintId) { return -1 }
    if (a.ComplaintId > b.ComplaintId) { return 1 }
    return 0
  })

  return closingReports
}

async function fetchDepartureLetters() {
  let departureLetters = await fetchCcrbCsv(
    'https://www.nyc.gov/assets/ccrb/csv/departure-letter/RedactedDepartureLetters.csv',
    'FileLink',
    'https://www1.nyc.gov/assets/ccrb/downloads/pdf/complaints/complaint-outcomes/redacted-departure-letters/')

  departureLetters.sort((a,b) => {
    if (a.CaseNumber < b.CaseNumber) { return -1 }
    if (a.CaseNumber > b.CaseNumber) { return 1 }
    if (a.LastName < b.LastName) { return -1 }
    if (a.LastName > b.LastName) { return 1 }
    return 0
  })

  return departureLetters
}

async function fetchCcrbCsv(url, docField, docPrefix) {
  console.info(url)

  const response = await fetch(url)
  const buffer = await response.text()
  let records = parse(buffer, { columns: true })

  records.forEach(record => {
    record[docField] = docPrefix + record[docField]
  })

  return records
}

function stripRecord(record) {
  let stripped = JSON.parse(JSON.stringify(record))
  Object.keys(stripped).forEach(key => {
    if ((stripped[key] === null) || (stripped[key] === '')) {
      delete stripped[key]
    }
  })

  return stripped
}

async function start() {
  let results = await fetchComplaints()
  results.closingReports = await fetchClosingReports()
  results.departureLetters = await fetchDepartureLetters()
  save(results)
}

start()
