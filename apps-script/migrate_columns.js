/**
 * One-shot column reorder for Sheet1 (2026-04-24).
 *
 * Transforms the 24-column layout:
 *   1 Project name, 2 Token CA, 3 Chain, 4 Status, 5 X, 6 TG, 7 Community,
 *   8 Mod, 9 Owner, 10 Channel contacted, 11 Date added, 12 Last msg (us),
 *   13 Last msg (them), 14 Outreach template, 15 Notes, 16 DS, 17 Market Cap,
 *   18 24h Volume, 19 24h Change %, 20 Age (days), 21 Last auto enrich,
 *   22 Message sent, 23 Message Replied, 24 Template
 *
 * into the 26-column layout (groups: identity, market, messaging, notes):
 *   1 Project name, 2 Token CA, 3 Chain, 4 Status, 5 X, 6 TG, 7 Community,
 *   8 Mod, 9 Owner, 10 DS, 11 Market Cap, 12 24h Volume, 13 24h Change %,
 *   14 Age (days), 15 VOL_AT_INGEST, 16 PRICE_MAX_SEEN, 17 Last auto enrich,
 *   18 Channel contacted, 19 Date added, 20 Outreach template, 21 Template,
 *   22 Last msg (us), 23 Last msg (them), 24 Message sent, 25 Message Replied,
 *   26 Notes
 *
 * WHAT IT PRESERVES: cell values.
 * WHAT IT DOES NOT PRESERVE: conditional formatting, number formats,
 *   background colors, data validation, column widths, formulas (formulas
 *   are copied as their evaluated values). Re-apply manually after verifying.
 *
 * SAFETY:
 *   - Refuses to run if headers don't match the expected 24-col layout
 *     (prevents double-run / running on an already-migrated sheet).
 *   - Duplicates Sheet1 as `Sheet1_backup_<timestamp>` before writing.
 *
 * USAGE: Run `migrateColumnsV1` once from the Apps Script editor.
 */

function migrateColumnsV1() {
  var EXPECTED_HEADERS = [
    'Project name', 'Token CA', 'Chain', 'Status', 'X', 'TG', 'Community',
    'Mod (most active/admin)', 'Owner', 'Channel contacted', 'Date added',
    'Last msg (us)', 'Last msg (them)', 'Outreach template', 'Notes', 'DS',
    'Market Cap', '24h Volume', '24h Change %', 'Age (days)',
    'Last auto enrich', 'Message sent', 'Message Replied', 'Template'
  ];
  var NEW_HEADERS = [
    'Project name', 'Token CA', 'Chain', 'Status', 'X', 'TG', 'Community',
    'Mod (most active/admin)', 'Owner', 'DS', 'Market Cap', '24h Volume',
    '24h Change %', 'Age (days)', 'VOL_AT_INGEST', 'PRICE_MAX_SEEN',
    'Last auto enrich', 'Channel contacted', 'Date added', 'Outreach template',
    'Template', 'Last msg (us)', 'Last msg (them)', 'Message sent',
    'Message Replied', 'Notes'
  ];
  // old 1-based col index -> new 1-based col index
  var MAP = {
    1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6, 7: 7, 8: 8, 9: 9,
    10: 18, 11: 19, 12: 22, 13: 23, 14: 20, 15: 26, 16: 10,
    17: 11, 18: 12, 19: 13, 20: 14, 21: 17, 22: 24, 23: 25, 24: 21
  };

  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName('Sheet1');
  if (!sheet) throw new Error('Sheet1 not found');

  var lastRow = sheet.getLastRow();
  var lastCol = sheet.getLastColumn();
  if (lastRow < 1) throw new Error('Sheet1 is empty');

  // Verify current headers match the pre-migration layout exactly.
  var currentHeaders = sheet.getRange(1, 1, 1, Math.max(lastCol, EXPECTED_HEADERS.length)).getValues()[0];
  for (var i = 0; i < EXPECTED_HEADERS.length; i++) {
    var got = String(currentHeaders[i] == null ? '' : currentHeaders[i]).trim();
    if (got !== EXPECTED_HEADERS[i]) {
      throw new Error(
        'Header mismatch at col ' + (i + 1) +
        ': expected "' + EXPECTED_HEADERS[i] + '", got "' + got + '". ' +
        'Migration aborted. If sheet is already migrated, do not run this again.'
      );
    }
  }

  // Back up Sheet1 before touching anything.
  var stamp = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyyMMdd_HHmmss');
  var backupName = 'Sheet1_backup_' + stamp;
  var backup = sheet.copyTo(ss);
  backup.setName(backupName);

  // Read every row including header; we'll rebuild header too.
  var allData = sheet.getRange(1, 1, lastRow, EXPECTED_HEADERS.length).getValues();
  var newData = [];
  for (var r = 0; r < allData.length; r++) {
    var oldRow = allData[r];
    var newRow = new Array(NEW_HEADERS.length);
    for (var c = 0; c < NEW_HEADERS.length; c++) newRow[c] = '';
    for (var oldCol = 1; oldCol <= EXPECTED_HEADERS.length; oldCol++) {
      var newCol = MAP[oldCol];
      newRow[newCol - 1] = oldRow[oldCol - 1];
    }
    newData.push(newRow);
  }
  // Override first row with the canonical new headers.
  newData[0] = NEW_HEADERS.slice();

  // Ensure sheet is wide enough for 26 cols.
  var maxCols = sheet.getMaxColumns();
  if (maxCols < NEW_HEADERS.length) {
    sheet.insertColumnsAfter(maxCols, NEW_HEADERS.length - maxCols);
  }

  // Clear old contents across the full written width, then write new values.
  var writeWidth = Math.max(EXPECTED_HEADERS.length, NEW_HEADERS.length);
  sheet.getRange(1, 1, lastRow, writeWidth).clearContent();
  sheet.getRange(1, 1, newData.length, NEW_HEADERS.length).setValues(newData);

  Logger.log(
    'Migrated ' + (newData.length - 1) + ' data rows to new 26-col layout. ' +
    'Backup: "' + backupName + '". ' +
    'Re-apply conditional formatting, number formats, and data validation manually.'
  );
}

/**
 * Test-ingest one synthetic CA via the real doPost code path.
 * No network — builds a fake event object and invokes doPost directly.
 * After running, visually verify the row landed with:
 *   col 2 = MIGRATION_TEST_<timestamp>, col 3 = solana, col 4 = Lead,
 *   col 10 = DS url, col 19 = today's date, col 26 = "auto: ..." note,
 *   and cols 22–25 (message tracking) are blank.
 * Then delete the test row manually.
 */
function testPostIngestAfterMigration() {
  var testCa = 'MIGRATION_TEST_' + Date.now();
  var fakeEvent = {
    postData: {
      contents: JSON.stringify({
        source: 'migration_test',
        rows: [{ chain: 'Solana', ca: testCa }]
      })
    }
  };
  var result = doPost(fakeEvent);
  Logger.log('doPost returned: ' + result.getContent());
  Logger.log('Look for row with Token CA = "' + testCa + '" at the top of Sheet1 data.');
}

