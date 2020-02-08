// const req = {
//   // The spreadsheet to request.
//   spreadsheetId: '1dgk1inO4DWA89jQGYqVmWgKvQXGW2dsMrOnbGMsAo9M',

//   // The ranges to retrieve from the spreadsheet.
//   range: 'master!A1:Q',
//   auth,
// };
const Promise = require('bluebird');
const request = Promise.promisifyAll(require('request'));

const { google } = require('./node_modules/googleapis');
const sheets = google.sheets('v4');
const ss = Promise.promisifyAll(sheets.spreadsheets.values);

const mkdirp = require('mkdirp');
const fs = require('fs');
const path = require('path');
const parse = require('./node_modules/csv-parse/lib/sync');
const moment = require('moment');

require('./node_modules/dotenv').config();
const SPREADSHEET_ID = process.env.SPREADSHEET_ID

// TODO: should have a script to export the crentials from env vars to file?
const CREDENTIAL_JSON = process.env.CREDENTIAL_JSON
const GOOGLE_APPLICATION_CREDENTIALS = process.env.GOOGLE_APPLICATION_CREDENTIALS

mkdirp.sync(path.dirname(GOOGLE_APPLICATION_CREDENTIALS))
fs.writeFileSync(GOOGLE_APPLICATION_CREDENTIALS, CREDENTIAL_JSON)


async function readFromSpreadSheet(spreadsheetId, range) {
  // This method looks for the GCLOUD_PROJECT and GOOGLE_APPLICATION_CREDENTIALS
  // environment variables.
  const auth = new google.auth.GoogleAuth({
    // Scopes can be specified either as an array or as a single, space-delimited string.
    scopes: ['https://www.googleapis.com/auth/spreadsheets']
  });

  const req = {
    spreadsheetId,
    range,
    auth,
  };


  const data = await ss.getAsync(req);

  return data.data.values;
}

async function uploadToSpreadSheet(spreadsheetId, range, values) {
  const auth = new google.auth.GoogleAuth({
    // Scopes can be specified either as an array or as a single, space-delimited string.
    scopes: ['https://www.googleapis.com/auth/spreadsheets']
  });

  const data = [{
    range,
    values,
  }];
  // Additional ranges to update ...
  const resource = {
    data,
    valueInputOption: 'USER_ENTERED',
  };
  return ss.batchUpdateAsync({
    spreadsheetId,
    resource,
    auth,
  });
}

async function main() {

  const spreadsheetRecords = await readFromSpreadSheet(SPREADSHEET_ID, 'master!A1:Q')
  spreadsheetRecords.shift();
  spreadsheetRecords.shift();


  const records = await readCSV();
  // remove header
  records.shift();

  records.forEach(r => {
    const index = spreadsheetRecords.findIndex(sr => sr[0] === r[0])
    if (index < 0) {
      spreadsheetRecords.push(r)
    } else {
      spreadsheetRecords[index] = r
    }
  })


  const values = [['This spreadsheet is generated by bot.', `Last Updated at:${moment().format('YYYY-MM-DD hh:mm:ss')}`], [], ...spreadsheetRecords]
  const result = await uploadToSpreadSheet(SPREADSHEET_ID, 'master!A1:Q', values)
  console.log(`Total ${result.data.totalUpdatedRows} rows updated.`);
}

async function readCSV() {
  const data = await request.getAsync(process.env.REMOTE_CSV_PATH)
  const records = parse(data.body, {
    columns: false,
    skip_empty_lines: true
  })
  return records
}

main().catch(console.error);