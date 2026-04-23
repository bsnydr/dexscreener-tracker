// ---- MULTI-SHEET SYNC (two-way filtered views, header-based) ----

var SYNC_SOURCE = 'Sheet1';

// Update these to match your exact header text in Sheet1 row 1
var COL_STATUS    = 'Status';            // column D
var COL_CONTACTED = 'Last msg (us)';     // column L
var COL_LAST_MSG  = 'Last msg (them)';   // column M

var SYNC_SHEETS = [
  {
    target: 'Leads',
    matches: function(row) {
      // Status column = "Lead"
      return String(row[COL_STATUS] || '').trim().toUpperCase() === 'LEAD';
    }
  },
  {
    target: 'In Touch',
    matches: function(row) {
      // "Last message [them]" has a value (regardless of Contacted)
      return hasValue_(row[COL_LAST_MSG]);
    }
  },
  {
    target: 'Contacted',
    matches: function(row) {
      // "Contacted" has a value AND "Last message [them]" is empty
      return hasValue_(row[COL_CONTACTED]) && !hasValue_(row[COL_LAST_MSG]);
    }
  }
];

function hasValue_(v) {
  return v !== '' && v !== null && v !== undefined && String(v).trim() !== '';
}

function syncAllFilteredSheets() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var source = ss.getSheetByName(SYNC_SOURCE);
  if (!source) return;

  var data = source.getDataRange().getValues();
  if (data.length === 0) return;

  var headers = data[0];

  for (var s = 0; s < SYNC_SHEETS.length; s++) {
    var config = SYNC_SHEETS[s];
    var target = ss.getSheetByName(config.target);
    if (!target) continue;

    target.clearContents();

    var headerOut = ['_srcRow'].concat(headers);
    target.getRange(1, 1, 1, headerOut.length).setValues([headerOut]);

    var filteredRows = [];
    for (var i = 1; i < data.length; i++) {
      var rowObj = rowToObject_(headers, data[i]);
      if (config.matches(rowObj)) {
        filteredRows.push([i + 1].concat(data[i]));
      }
    }

    if (filteredRows.length > 0) {
      target.getRange(2, 1, filteredRows.length, filteredRows[0].length)
        .setValues(filteredRows);
    }

    target.hideColumns(1);
  }
}

function rowToObject_(headers, rowValues) {
  var obj = {};
  for (var i = 0; i < headers.length; i++) {
    obj[headers[i]] = rowValues[i];
  }
  return obj;
}

function handleFilteredSheetEdit_(e) {
  var sheet = e.source.getActiveSheet();
  var sheetName = sheet.getName();

  var isFilteredSheet = false;
  for (var s = 0; s < SYNC_SHEETS.length; s++) {
    if (SYNC_SHEETS[s].target === sheetName) {
      isFilteredSheet = true;
      break;
    }
  }
  if (!isFilteredSheet) return;

  var row = e.range.getRow();
  var col = e.range.getColumn();

  if (row <= 1 || col <= 1) return;

  var srcRow = sheet.getRange(row, 1).getValue();
  if (!srcRow) return;

  var source = e.source.getSheetByName(SYNC_SOURCE);
  source.getRange(srcRow, col - 1).setValue(e.value);
}

function setupFilteredSheetSync() {
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = 0; i < triggers.length; i++) {
    if (triggers[i].getHandlerFunction() === 'syncAllFilteredSheets') {
      ScriptApp.deleteTrigger(triggers[i]);
    }
  }
  ScriptApp.newTrigger('syncAllFilteredSheets').timeBased().everyMinutes(1).create();
}