/**
 * CRM OUTREACH TRACKER - Google Apps Script for Sheet1
 * v8 - DS-first pipeline + pending enrichment
 *
 * Key safety rule:
 * - never overwrite a concrete chain (Base/BSC/Ethereum) with EVM
 * - never blank a DexScreener link that already exists
 *
 * Auto-enrich safety:
 * - processes only a small batch each run
 * - writes one row at a time
 * - re-finds each row by address before writing
 * - does not full-table overwrite while you are adding rows
 * - only uses BirdEye for rows that need core enrichment
 * - auto-enrich refreshes market data and retries missing socials
 * - each row can only be auto-enriched once every 24 hours
 *
 * v8 additions:
 * - DS-first pipeline: DS used before Birdeye, covers chain + market + socials + age
 * - Birdeye only called when DS can't identify EVM chain or has no market data
 * - GT pools endpoint retries up to 2x (fixes transient age misses)
 * - Pending enrichment: 1-min trigger, 2-min debounce, enriches fresh pastes
 * - Missing age now triggers social retry pass
 * - Birdeye call count logged per run
 */

var CONFIG = {
  SHEET_NAME: 'Sheet1',
  HEADER_ROW: 1,
  ENTRY_ROW: 2,
  DATA_START_ROW: 3,
  TIMESTAMP_FORMAT: 'dd-MMM',
  BIRDEYE_API_KEY_PROP: 'BIRDEYE_API_KEY',
  DIAG_SHEET_NAME: '_diagnostic',

  OWNER_BY_EMAIL: {
    'ben@forged.tech': 'Ben',
    'lfgnow@sonar.trade': 'LFG'
  },

  COLORS: {
    RED: '#FF0000',
    GREEN: '#B7F5C2',
    PURPLE: '#D8B4FE',
    ORANGE: '#FFA500'
  },

  STATUS: {
    LEAD: 'LEAD',
    COULD_NOT_CONTACT: 'COULD NOT CONTACT',
    IN_TOUCH: 'IN TOUCH'
  },

  API_SLEEP_MS: 250,
  CHAIN_SCAN_SLEEP_MS: 500,
  GT_SLEEP_MS: 2000,
  GT_POOL_SLEEP_MS: 500,
  BIRDEYE_RETRY_COUNT: 2,
  LOCK_WAIT_MS: 5000,
  AUTO_LOCK_WAIT_MS: 1000,

  AUTO_ENRICH_BATCH_SIZE: 25,
  AUTO_SKIP_SORT: true,
  AUTO_ENRICH_COOLDOWN_HOURS: 24,

  // v8 additions
  PENDING_BATCH_SIZE: 10,
  PENDING_DEBOUNCE_MS: 2 * 60 * 1000,
  LAST_EDIT_PROP: 'LAST_EDIT_TIMESTAMP_MS'
};

var COL = {
  PROJECT: 1,
  ADDRESS: 2,
  CHAIN: 3,
  STATUS: 4,
  X_PROFILE: 5,
  TG: 6,
  X_COMMUNITY: 7,
  USER: 9,
  TIMESTAMP: 11,
  OUTREACH_TEMPLATE: 14,
  NOTES: 15,
  DS: 16,
  MCAP: 17,
  VOLUME_24H: 18,
  CHANGE_24H: 19,
  AGE: 20,
  LAST_AUTO_ENRICH: 21
};

var DUPE_COLS = [COL.ADDRESS, COL.X_PROFILE, COL.TG, COL.X_COMMUNITY];

var GT_NETWORK = {
  solana: 'solana',
  ethereum: 'eth',
  base: 'base',
  bsc: 'bsc',
  arbitrum: 'arbitrum',
  polygon: 'polygon_pos',
  optimism: 'optimism',
  avalanche: 'avax'
};

var EVM_SCAN_CHAINS = ['base', 'bsc', 'ethereum'];

var PROCESS_MODE = {
  NORMAL: 'normal',
  AUTO: 'auto',
  REVALIDATE_EVM: 'revalidate_evm',
  FORCE_ROW: 'force_row',
  PENDING: 'pending'
};


// =============================================================================
// MENU
// =============================================================================

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('CRM')
    .addItem('Enrich All Rows', 'enrichAll')
    .addItem('Enrich Current Row', 'enrichCurrentRow')
    .addSeparator()
    .addItem('Enable Auto-Enrich (every 6 hours)', 'setupAutoEnrich')
    .addItem('Disable Auto-Enrich', 'removeAutoEnrich')
    .addItem('Enable Pending Enrich (every 1 minute)', 'setupPendingEnrich')
    .addItem('Disable Pending Enrich', 'removePendingEnrich')
    .addSeparator()
    .addItem('Run Diagnostic', 'runDiagnostic')
    .addItem('Force Refresh (sort + colors)', 'forceRefresh')
    .addItem('Debug Current Row', 'debugCurrentRow')
    .addToUi();
}


// =============================================================================
// SIMPLE TRIGGER
// =============================================================================

function onEdit(e) {
  if (!e || !e.range || !e.source) return;

  if (typeof handleFilteredSheetEdit_ === 'function') {
    handleFilteredSheetEdit_(e);
  }

  var sheet = e.source.getActiveSheet();
  if (!isTargetSheet_(sheet)) return;
  if (e.range.getRow() === CONFIG.HEADER_ROW) return;

  // Stamp last-edit time for debounced pending-enrichment.
  try {
    PropertiesService.getScriptProperties().setProperty(
      CONFIG.LAST_EDIT_PROP,
      String(Date.now())
    );
  } catch (_) {}

  var editRow = e.range.getRow();
  var editCol = e.range.getColumn();
  var numEditRows = e.range.getNumRows();

  if (editCol === COL.ADDRESS) {
    var needsRowInsert = false;

    for (var r = editRow; r < editRow + numEditRows; r++) {
      var isNew = handleAddressEdit_(sheet, r);
      if (isNew && r === CONFIG.ENTRY_ROW) needsRowInsert = true;
    }

    if (needsRowInsert) {
      sheet.insertRowBefore(CONFIG.ENTRY_ROW);
      sheet.getRange(CONFIG.ENTRY_ROW, 1, 1, sheet.getLastColumn()).setBackground(null);
    }
    return;
  }

  runMaintenance_(sheet, { doSort: false });
}

function handleAddressEdit_(sheet, rowNumber) {
  var address = normalize_(sheet.getRange(rowNumber, COL.ADDRESS).getValue());
  if (!address) return false;

  var chainCell = sheet.getRange(rowNumber, COL.CHAIN);
  if (chainCell.isBlank()) {
    chainCell.setValue(defaultChainFromAddress_(address));
  }

  var timestampCell = sheet.getRange(rowNumber, COL.TIMESTAMP);
  if (!timestampCell.isBlank()) return false;

  timestampCell.setValue(new Date());
  timestampCell.setNumberFormat(CONFIG.TIMESTAMP_FORMAT);

  var email = Session.getActiveUser().getEmail();
  var owner = CONFIG.OWNER_BY_EMAIL[email];
  if (owner) {
    sheet.getRange(rowNumber, COL.USER).setValue(owner);
  }

  var statusCell = sheet.getRange(rowNumber, COL.STATUS);
  if (statusCell.isBlank()) {
    statusCell.setValue('Lead');
  }

  return true;
}


// =============================================================================
// BATCH MAINTENANCE
// =============================================================================

function runMaintenance_(sheet, opts) {
  opts = opts || {};
  var doSort = opts.doSort !== false;

  var lastRow = sheet.getLastRow();
  if (lastRow < CONFIG.DATA_START_ROW) return;

  var numRows = lastRow - CONFIG.DATA_START_ROW + 1;
  var numCols = sheet.getLastColumn();
  if (numRows <= 0 || numCols <= 0) return;

  var range = sheet.getRange(CONFIG.DATA_START_ROW, 1, numRows, numCols);
  var values = range.getValues();
  var backgrounds = range.getBackgrounds();

  fillChainPlaceholdersFromAddress_(values);
  fillDsUrls_(values, numCols);

  if (doSort) {
    sortRows_(values, backgrounds);
  }

  var dupeSet = findDuplicateIndices_(values);
  applyHighlights_(values, backgrounds, dupeSet, numCols);

  range.setValues(values);
  range.setBackgrounds(backgrounds);
}

function fillChainPlaceholdersFromAddress_(values) {
  var addrIdx = idx_(COL.ADDRESS);
  var chainIdx = idx_(COL.CHAIN);

  for (var r = 0; r < values.length; r++) {
    var address = normalize_(values[r][addrIdx]);
    var chain = normalize_(values[r][chainIdx]);
    if (!address || chain) continue;
    values[r][chainIdx] = defaultChainFromAddress_(address);
  }
}

function fillDsUrls_(values, numCols) {
  var addrIdx = idx_(COL.ADDRESS);
  var chainIdx = idx_(COL.CHAIN);
  var dsIdx = idx_(COL.DS);
  if (dsIdx >= numCols) return;

  for (var r = 0; r < values.length; r++) {
    var address = normalize_(values[r][addrIdx]);
    var chain = chainKey_(values[r][chainIdx]);
    if (!address || !chain || chain === 'evm') continue;

    var correctUrl = buildDexScreenerUrl_(chain, address);
    if (normalize_(values[r][dsIdx]) !== correctUrl) {
      values[r][dsIdx] = correctUrl;
    }
  }
}

function sortRows_(values, backgrounds) {
  var tsIdx = idx_(COL.TIMESTAMP);
  var indices = values.map(function(_, i) { return i; });

  indices.sort(function(a, b) {
    var aTs = values[a][tsIdx];
    var bTs = values[b][tsIdx];
    var aValid = isValidDate_(aTs);
    var bValid = isValidDate_(bTs);
    if (!aValid && !bValid) return 0;
    if (!aValid) return 1;
    if (!bValid) return -1;
    return bTs - aTs;
  });

  var sortedV = indices.map(function(i) { return values[i]; });
  var sortedB = indices.map(function(i) { return backgrounds[i]; });

  for (var r = 0; r < values.length; r++) {
    values[r] = sortedV[r];
    backgrounds[r] = sortedB[r];
  }
}

function findDuplicateIndices_(values) {
  var dupeSet = {};

  for (var d = 0; d < DUPE_COLS.length; d++) {
    var colIdx = idx_(DUPE_COLS[d]);
    var buckets = {};

    for (var r = 0; r < values.length; r++) {
      var val = normalize_(values[r][colIdx]);
      if (!val || isNA_(val)) continue;
      var key = val.toLowerCase();
      if (!buckets[key]) buckets[key] = [];
      buckets[key].push(r);
    }

    var keys = Object.keys(buckets);
    for (var k = 0; k < keys.length; k++) {
      if (buckets[keys[k]].length > 1) {
        var rows = buckets[keys[k]];
        for (var i = 0; i < rows.length; i++) dupeSet[rows[i]] = true;
      }
    }
  }

  return dupeSet;
}

function applyHighlights_(values, backgrounds, dupeSet, numCols) {
  var statusIdx = idx_(COL.STATUS);
  var knownColors = [
    normalizeColor_(CONFIG.COLORS.RED),
    normalizeColor_(CONFIG.COLORS.GREEN),
    normalizeColor_(CONFIG.COLORS.PURPLE),
    normalizeColor_(CONFIG.COLORS.ORANGE)
  ];

  for (var r = 0; r < values.length; r++) {
    var isDupe = dupeSet[r] === true;
    var status = normalize_(values[r][statusIdx]).toUpperCase();
    var targetColor = null;

    if (isDupe) {
      targetColor = CONFIG.COLORS.RED;
    } else if (status === CONFIG.STATUS.IN_TOUCH) {
      targetColor = CONFIG.COLORS.GREEN;
    } else if (status === CONFIG.STATUS.COULD_NOT_CONTACT) {
      targetColor = CONFIG.COLORS.PURPLE;
    } else if (status === CONFIG.STATUS.LEAD) {
      targetColor = CONFIG.COLORS.ORANGE;
    }

    for (var c = 0; c < numCols; c++) {
      var bg = normalizeColor_(backgrounds[r][c]);
      var isKnownColor = knownColors.indexOf(bg) !== -1;
      if (targetColor) {
        backgrounds[r][c] = targetColor;
      } else if (isKnownColor) {
        backgrounds[r][c] = null;
      }
    }
  }
}


// =============================================================================
// ENRICHMENT - ENTRY POINTS
// =============================================================================

function enrichAll() {
  withScriptLock_(CONFIG.LOCK_WAIT_MS, 'Enrichment already running', function() {
    var sheet = getTargetSheet_();
    if (sheet) {
      enrichRows_(
        sheet,
        CONFIG.DATA_START_ROW,
        null,
        false,
        PROCESS_MODE.NORMAL,
        { batchSize: null, skipSort: false }
      );
    }
  });
}

function enrichCurrentRow() {
  var sheet = getTargetSheet_();
  if (!sheet) return;

  var rowNumber = SpreadsheetApp.getActiveRange().getRow();
  if (rowNumber < CONFIG.DATA_START_ROW) {
    toast_('Select a data row (row 3+)', 3);
    return;
  }

  withScriptLock_(CONFIG.LOCK_WAIT_MS, 'Enrichment already running', function() {
    enrichRows_(
      sheet,
      rowNumber,
      rowNumber,
      false,
      PROCESS_MODE.FORCE_ROW,
      { batchSize: 1, skipSort: false }
    );
  });
}

function autoEnrich() {
  var sheet = getTargetSheet_();
  if (!sheet) return;

  var lock = LockService.getScriptLock();
  if (!lock.tryLock(CONFIG.AUTO_LOCK_WAIT_MS)) return;

  try {
    enrichRows_(
      sheet,
      CONFIG.DATA_START_ROW,
      null,
      true,
      PROCESS_MODE.AUTO,
      {
        batchSize: CONFIG.AUTO_ENRICH_BATCH_SIZE,
        skipSort: CONFIG.AUTO_SKIP_SORT
      }
    );
  } finally {
    lock.releaseLock();
  }
}


// =============================================================================
// ENRICHMENT - CORE LOOP
// =============================================================================

function enrichRows_(sheet, startRow, endRow, silent, mode, opts) {
  opts = opts || {};

  var lastRow = endRow || sheet.getLastRow();
  if (lastRow < startRow) return;

  var numRows = lastRow - startRow + 1;
  var numCols = Math.max(sheet.getLastColumn(), COL.LAST_AUTO_ENRICH);
  var values = sheet.getRange(startRow, 1, numRows, numCols).getValues();

  ensureRowsWideEnough_(values, COL.LAST_AUTO_ENRICH);

  var apiKey = getBirdeyeApiKey_();
  var targets = buildTargetList_(values, startRow, mode || PROCESS_MODE.NORMAL);

  if (opts.batchSize && opts.batchSize > 0) {
    targets = targets.slice(0, opts.batchSize);
  }

  if (targets.length === 0) {
    if (!silent) toast_('All rows already enriched', 3);
    return;
  }

  if (!silent) toast_('Enriching ' + targets.length + ' rows...', 30);

  var log = [];
  log.push('ENRICHMENT LOG (v8 DS-first + pending)');
  log.push('Started: ' + new Date().toISOString());
  log.push('Range: rows ' + startRow + '-' + lastRow + ' (' + targets.length + ' rows to process)');
  log.push('Birdeye: ' + (apiKey ? 'SET' : 'MISSING'));
  log.push('Mode: ' + mode);
  log.push('');

  var enriched = 0;
  var errors = 0;
  var birdeyeCallCount = 0;

  for (var i = 0; i < targets.length; i++) {
    var target = targets[i];

    var liveRowNumber = findRowByAddress_(sheet, target.address);
    if (liveRowNumber < CONFIG.DATA_START_ROW) {
      log.push('--- Row ??: ' + abbreviate_(target.address, 16) + ' ---');
      log.push('  [ROW] skipped - address moved or deleted');
      log.push('');
      continue;
    }

    var liveRow = sheet.getRange(liveRowNumber, 1, 1, numCols).getValues()[0];
    ensureRowsWideEnough_([liveRow], COL.LAST_AUTO_ENRICH);

    var ctx = buildRowContext_(liveRow);

    log.push('--- Row ' + liveRowNumber + ': ' + abbreviate_(ctx.address, 16) + ' ---');

    try {
      enrichSingleRow_(ctx, apiKey, log, target);
      birdeyeCallCount += ctx.birdeyeCallsMade || 0;
    } catch (err) {
      log.push('  [ROW] ERROR: ' + err.message);
      log.push('');
      errors++;
      continue;
    }

    writeContextToRow_(ctx, liveRow);

    if (mode === PROCESS_MODE.AUTO || mode === PROCESS_MODE.PENDING) {
      liveRow[idx_(COL.LAST_AUTO_ENRICH)] = new Date();
    }

    sheet.getRange(liveRowNumber, 1, 1, COL.LAST_AUTO_ENRICH)
      .setValues([liveRow.slice(0, COL.LAST_AUTO_ENRICH)]);

    logRowSummary_(ctx, log);

    enriched++;
    if (!silent && (enriched % 3 === 0 || enriched === targets.length)) {
      toast_('Enriched ' + enriched + '/' + targets.length, 5);
    }
  }

  runMaintenance_(sheet, { doSort: !opts.skipSort });

  log.push('ENRICHMENT COMPLETE');
  log.push('Finished: ' + new Date().toISOString());
  log.push('Enriched: ' + enriched + ' | Errors: ' + errors + ' | Birdeye calls: ' + birdeyeCallCount);

  var report = log.join('\n');
  Logger.log(report);
  writeDiagnostic_(report);
  if (!silent) toast_('Done - ' + enriched + ' rows (' + errors + ' errors). See _diagnostic tab.', 5);
}

function rowShouldProcess_(row, mode) {
  var address = normalize_(row[idx_(COL.ADDRESS)]);
  if (!address) return false;

  if (mode === PROCESS_MODE.FORCE_ROW) return true;
  if (mode === PROCESS_MODE.AUTO) return canAutoEnrichRow_(row);

  if (mode === PROCESS_MODE.REVALIDATE_EVM) {
    return isEvmAddress_(address) || rowNeedsEnrichment_(row);
  }

  return rowNeedsEnrichment_(row);
}


// =============================================================================
// ENRICHMENT - TARGETING HELPERS
// =============================================================================

function hasCellValue_(value) {
  return !(value === '' || value === null || value === undefined);
}

function rowNeedsCoreEnrichment_(row) {
  var address = normalize_(row[idx_(COL.ADDRESS)]);
  if (!address) return false;

  var chain = chainKey_(row[idx_(COL.CHAIN)]);
  var project = normalize_(row[idx_(COL.PROJECT)]);

  if (!chain || chain === 'evm') return true;
  if (!project) return true;

  return false;
}

function rowNeedsSocialPass_(row) {
  var address = normalize_(row[idx_(COL.ADDRESS)]);
  if (!address) return false;

  if (!normalize_(row[idx_(COL.X_PROFILE)])) return true;
  if (!normalize_(row[idx_(COL.TG)])) return true;
  if (!normalize_(row[idx_(COL.X_COMMUNITY)])) return true;
  if (!normalize_(row[idx_(COL.AGE)])) return true;  // v8: missing age = needs retry

  var chain = chainKey_(row[idx_(COL.CHAIN)]);
  if (isConcreteChain_(chain) && !normalize_(row[idx_(COL.DS)])) return true;

  return false;
}

function rowNeedsMarketRefresh_(row) {
  var address = normalize_(row[idx_(COL.ADDRESS)]);
  if (!address) return false;

  var chain = chainKey_(row[idx_(COL.CHAIN)]);
  if (!isConcreteChain_(chain)) return false;

  if (!hasCellValue_(row[idx_(COL.MCAP)])) return true;
  if (!hasCellValue_(row[idx_(COL.VOLUME_24H)])) return true;
  if (!hasCellValue_(row[idx_(COL.CHANGE_24H)])) return true;

  return false;
}

function rowNeedsEnrichment_(row) {
  return (
    rowNeedsCoreEnrichment_(row) ||
    rowNeedsSocialPass_(row) ||
    rowNeedsMarketRefresh_(row)
  );
}

// v8: a row is "pending" if it was never auto-enriched and still needs something
function rowIsPending_(row) {
  var address = normalize_(row[idx_(COL.ADDRESS)]);
  if (!address) return false;

  var last = parseSheetDate_(row[idx_(COL.LAST_AUTO_ENRICH)]);
  if (last) return false;

  return rowNeedsEnrichment_(row);
}

function buildTargetList_(values, startRow, mode) {
  var core = [];
  var social = [];
  var market = [];

  for (var r = 0; r < values.length; r++) {
    var row = values[r];
    var address = normalize_(row[idx_(COL.ADDRESS)]);
    if (!address) continue;

    var needsCore = rowNeedsCoreEnrichment_(row);
    var needsSocial = rowNeedsSocialPass_(row);
    var needsMarket = rowNeedsMarketRefresh_(row);
    var chain = chainKey_(row[idx_(COL.CHAIN)]);
    var hasConcreteChain = isConcreteChain_(chain);

    if (mode === PROCESS_MODE.FORCE_ROW) {
      return [{
        rowNumber: startRow + r,
        address: address,
        needsCore: true,
        needsSocial: true,
        needsMarket: true,
        force: true
      }];
    }

    if (mode === PROCESS_MODE.PENDING) {
      if (!rowIsPending_(row)) continue;
      core.push({
        rowNumber: startRow + r,
        address: address,
        needsCore: true,
        needsSocial: true,
        needsMarket: true
      });
      continue;
    }

    if (mode === PROCESS_MODE.AUTO) {
      if (!canAutoEnrichRow_(row)) continue;

      var autoSortValue = getAutoSortValue_(row);

      if (needsCore) {
        core.push({
          rowNumber: startRow + r,
          address: address,
          needsCore: true,
          needsSocial: true,
          needsMarket: hasConcreteChain,
          autoSortValue: autoSortValue
        });
      } else if (hasConcreteChain) {
        market.push({
          rowNumber: startRow + r,
          address: address,
          needsCore: false,
          needsSocial: true,
          needsMarket: true,
          autoSortValue: autoSortValue
        });
      } else if (needsSocial) {
        social.push({
          rowNumber: startRow + r,
          address: address,
          needsCore: false,
          needsSocial: true,
          needsMarket: false,
          autoSortValue: autoSortValue
        });
      }

      continue;
    }

    if (mode === PROCESS_MODE.REVALIDATE_EVM) {
      if (!chain || chain === 'evm') {
        core.push({
          rowNumber: startRow + r,
          address: address,
          needsCore: true,
          needsSocial: needsSocial,
          needsMarket: needsMarket
        });
      } else if (needsCore) {
        core.push({
          rowNumber: startRow + r,
          address: address,
          needsCore: true,
          needsSocial: needsSocial,
          needsMarket: needsMarket
        });
      } else if (needsSocial) {
        social.push({
          rowNumber: startRow + r,
          address: address,
          needsCore: false,
          needsSocial: true,
          needsMarket: false
        });
      } else if (needsMarket) {
        market.push({
          rowNumber: startRow + r,
          address: address,
          needsCore: false,
          needsSocial: false,
          needsMarket: true
        });
      }
      continue;
    }

    if (needsCore) {
      core.push({
        rowNumber: startRow + r,
        address: address,
        needsCore: true,
        needsSocial: needsSocial,
        needsMarket: needsMarket
      });
    } else if (needsSocial) {
      social.push({
        rowNumber: startRow + r,
        address: address,
        needsCore: false,
        needsSocial: true,
        needsMarket: false
      });
    } else if (needsMarket) {
      market.push({
        rowNumber: startRow + r,
        address: address,
        needsCore: false,
        needsSocial: false,
        needsMarket: true
      });
    }
  }

  if (mode === PROCESS_MODE.AUTO) {
    core.sort(compareAutoTargets_);
    social.sort(compareAutoTargets_);
    market.sort(compareAutoTargets_);

    return core.concat(social).concat(market).map(function(t) {
      delete t.autoSortValue;
      return t;
    });
  }

  return core.concat(social).concat(market);
}

function findRowByAddress_(sheet, address) {
  var lastRow = sheet.getLastRow();
  if (lastRow < CONFIG.DATA_START_ROW) return -1;

  var values = sheet.getRange(
    CONFIG.DATA_START_ROW,
    COL.ADDRESS,
    lastRow - CONFIG.DATA_START_ROW + 1,
    1
  ).getValues();

  var needle = normalize_(address).toLowerCase();

  for (var i = 0; i < values.length; i++) {
    if (normalize_(values[i][0]).toLowerCase() === needle) {
      return CONFIG.DATA_START_ROW + i;
    }
  }

  return -1;
}


// =============================================================================
// ENRICHMENT - SINGLE ROW PIPELINE (v8 DS-first)
// =============================================================================

function enrichSingleRow_(ctx, apiKey, log, target) {
  if (!ctx.address) return;

  ctx.birdeyeCallsMade = 0;

  var force = !!(target && target.force);
  var doCore = !!(target && target.needsCore);
  var doSocial = !!(target && target.needsSocial);
  var doMarket = !!(target && target.needsMarket);

  if (force) {
    doCore = true;
    doSocial = true;
    doMarket = true;
  }

  var isEvm = isEvmAddress_(ctx.address);

  // STEP 1: DS is always first. Free, covers ~90% of what we need.
  stepDexScreenerChainDiscovery_(ctx, log);

  var needsChainDiscovery = !ctx.chain || ctx.chain === 'evm';
  var dsGaveMarketData = ctx.dsResult && (
    hasValue_(ctx.dsResult.marketCap) ||
    hasValue_(ctx.dsResult.volume24h)
  );

  // STEP 2: Apply DS market data immediately if we got any.
  if ((doCore || doMarket || force) && dsGaveMarketData) {
    applyDexScreenerMarketData_(ctx, log);
  }

  // STEP 3: Birdeye chain scan - ONLY if DS failed and it's EVM.
  if (doCore && needsChainDiscovery && isEvm && apiKey) {
    stepBirdeyeChainScan_(ctx, apiKey, log);
    ctx.birdeyeCallsMade += EVM_SCAN_CHAINS.length;
  }

  var hasConcreteChain = ctx.chain && ctx.chain !== 'evm';

  // STEP 4: Birdeye enrichment - ONLY if we still need market data after DS.
  if ((doCore || doMarket || force) && hasConcreteChain && !dsGaveMarketData && apiKey) {
    var beApplied = stepBirdeyeEnrichment_(ctx, ctx.chain, apiKey, log);
    ctx.birdeyeCallsMade += 1;

    if (!beApplied && isEvm && needsChainDiscovery) {
      stepBirdeyeRescan_(ctx, apiKey, log);
      ctx.birdeyeCallsMade += (EVM_SCAN_CHAINS.length - 1);
    }
  }

  // STEP 5: GT fallback for socials + age.
  if ((doSocial || doCore || force) && hasConcreteChain) {
    if (!ctx.xProfile || !ctx.tg || !ctx.xCommunity || !normalize_(ctx.age)) {
      stepGeckoTerminalFallback_(ctx, log);
    }
  }

  // STEP 6: DS social fallback. Second chance for social fields + age.
  if (doSocial || doCore || force) {
    if (!ctx.xProfile || !ctx.tg || !ctx.xCommunity || !normalize_(ctx.age)) {
      stepDexScreenerSocialFallback_(ctx, log);
    }
  }

  restoreSafeFallbacks_(ctx);
}

function buildRowContext_(row) {
  var existingChain = chainKey_(row[idx_(COL.CHAIN)]);
  var existingDs = normalize_(row[idx_(COL.DS)]);

  return {
    address: normalize_(row[idx_(COL.ADDRESS)]),
    chain: existingChain,
    originalChain: existingChain,

    project: normalize_(row[idx_(COL.PROJECT)]),
    xProfile: normalize_(row[idx_(COL.X_PROFILE)]),
    tg: normalize_(row[idx_(COL.TG)]),
    xCommunity: normalize_(row[idx_(COL.X_COMMUNITY)]),

    ds: existingDs,
    originalDs: existingDs,

    mcap: row[idx_(COL.MCAP)],
    volume24h: row[idx_(COL.VOLUME_24H)],
    change24h: row[idx_(COL.CHANGE_24H)],
    age: row[idx_(COL.AGE)],

    beforeXProfile: normalize_(row[idx_(COL.X_PROFILE)]),
    beforeTg: normalize_(row[idx_(COL.TG)]),
    beforeXCommunity: normalize_(row[idx_(COL.X_COMMUNITY)]),

    dsResult: null,
    beCandidateChain: '',
    beCandidateData: null,
    birdeyeCallsMade: 0
  };
}

function writeContextToRow_(ctx, row) {
  var finalChain = ctx.chain || ctx.originalChain;
  var finalDs = ctx.ds || ctx.originalDs;

  if (finalChain) {
    row[idx_(COL.CHAIN)] = formatChainDisplay_(finalChain);
  }

  row[idx_(COL.PROJECT)] = ctx.project;
  row[idx_(COL.X_PROFILE)] = ctx.xProfile;
  row[idx_(COL.TG)] = ctx.tg;
  row[idx_(COL.X_COMMUNITY)] = ctx.xCommunity;

  if (isConcreteChain_(finalChain)) {
    row[idx_(COL.DS)] = buildDexScreenerUrl_(finalChain, ctx.address);
  } else if (finalDs) {
    row[idx_(COL.DS)] = finalDs;
  }

  row[idx_(COL.MCAP)] = ctx.mcap;
  row[idx_(COL.VOLUME_24H)] = ctx.volume24h;
  row[idx_(COL.CHANGE_24H)] = ctx.change24h;
  row[idx_(COL.AGE)] = ctx.age;
}


// =============================================================================
// PIPELINE STEPS
// =============================================================================

function stepDexScreenerChainDiscovery_(ctx, log) {
  try {
    ctx.dsResult = fetchDexScreenerToken_(ctx.address);
    if (ctx.dsResult && ctx.dsResult.chain) {
      ctx.chain = chainKey_(ctx.dsResult.chain);
      log.push('  [DS] chain: ' + ctx.chain);

      if (!ctx.project && (ctx.dsResult.name || ctx.dsResult.symbol)) {
        ctx.project = formatProjectName_(ctx.dsResult.symbol, ctx.dsResult.name);
      }

      // v8: capture age from DS pairCreatedAt
      if (!normalize_(ctx.age) && ctx.dsResult.pairCreatedAt) {
        var dsDate = parseAnyDate_(ctx.dsResult.pairCreatedAt);
        if (dsDate) {
          ctx.age = calculateAgeDays_(dsDate);
          log.push('  [DS] age: ' + ctx.age + 'd');
        }
      }
    } else {
      log.push('  [DS] no data');
    }
  } catch (err) {
    log.push('  [DS] ERROR: ' + err.message);
  }

  Utilities.sleep(CONFIG.API_SLEEP_MS);
}

function stepBirdeyeChainScan_(ctx, apiKey, log) {
  log.push('  [BE chain scan] trying ' + EVM_SCAN_CHAINS.join(', '));

  var bestChain = null;
  var bestScore = null;
  var bestData = null;

  for (var i = 0; i < EVM_SCAN_CHAINS.length; i++) {
    var chain = EVM_SCAN_CHAINS[i];
    var probe = fetchBirdeyeOverviewWithRetry_(ctx.address, chain, apiKey, log, '  [BE chain scan] ' + chain);

    if (probe && probe.success && probe.data && probe.data.address) {
      if (isGarbageBirdeyeRecord_(probe.data)) {
        log.push('  [BE chain scan] ' + chain + ': garbage record');
      } else {
        var score = getBirdeyeScore_(probe.data);
        log.push(
          '  [BE chain scan] ' + chain +
          ': liq=' + Math.round(score.liquidity) +
          ' vol=' + Math.round(score.volume) +
          ' mcap=' + Math.round(score.mcap)
        );

        if (isBetterBirdeyeScore_(score, bestScore)) {
          bestChain = chain;
          bestScore = score;
          bestData = probe.data;
        }
      }
    }

    Utilities.sleep(CONFIG.CHAIN_SCAN_SLEEP_MS);
  }

  if (bestChain) {
    ctx.chain = bestChain;
    ctx.beCandidateChain = bestChain;
    ctx.beCandidateData = bestData;

    log.push(
      '  [BE chain scan] winner: ' + bestChain +
      ' (liq=' + Math.round(bestScore.liquidity) +
      ', vol=' + Math.round(bestScore.volume) +
      ', mcap=' + Math.round(bestScore.mcap) + ')'
    );
  } else {
    ctx.beCandidateChain = '';
    ctx.beCandidateData = null;
    log.push('  [BE chain scan] not found on any chain - keeping existing chain');
  }
}

function stepBirdeyeEnrichment_(ctx, chain, apiKey, log) {
  try {
    var data = null;

    if (ctx.beCandidateData && ctx.beCandidateChain === chain) {
      data = ctx.beCandidateData;
      log.push('  [BE] using cached chain-scan data for ' + chain);
    } else {
      var beResult = fetchBirdeyeOverviewWithRetry_(ctx.address, chain, apiKey, log, '  [BE] ' + chain);
      if (!(beResult && beResult.success && beResult.data)) {
        log.push('  [BE] no valid record');
        Utilities.sleep(CONFIG.API_SLEEP_MS);
        return false;
      }
      data = beResult.data;
    }

    if (isGarbageBirdeyeRecord_(data)) {
      log.push('  [BE] garbage record - treating as invalid');
      Utilities.sleep(CONFIG.API_SLEEP_MS);
      return false;
    }

    applyBirdeyeDataToContext_(ctx, chain, data, log);
    ctx.beCandidateChain = '';
    ctx.beCandidateData = null;

    Utilities.sleep(CONFIG.API_SLEEP_MS);
    return true;
  } catch (err) {
    log.push('  [BE] ERROR: ' + err.message);
    Utilities.sleep(CONFIG.API_SLEEP_MS);
    return false;
  }
}

function stepBirdeyeRescan_(ctx, apiKey, log) {
  var currentChain = chainKey_(ctx.chain);
  var altChains = EVM_SCAN_CHAINS.filter(function(c) { return c !== currentChain; });

  log.push('  [BE re-scan] ' + currentChain + ' returned nothing/junk, trying ' + altChains.join(', '));

  var bestChain = null;
  var bestScore = null;
  var bestData = null;

  for (var i = 0; i < altChains.length; i++) {
    var chain = altChains[i];
    var probe = fetchBirdeyeOverviewWithRetry_(ctx.address, chain, apiKey, log, '  [BE re-scan] ' + chain);

    if (probe && probe.success && probe.data && probe.data.address) {
      if (isGarbageBirdeyeRecord_(probe.data)) {
        log.push('  [BE re-scan] ' + chain + ': garbage record');
      } else {
        var score = getBirdeyeScore_(probe.data);
        log.push(
          '  [BE re-scan] ' + chain +
          ': liq=' + Math.round(score.liquidity) +
          ' vol=' + Math.round(score.volume) +
          ' mcap=' + Math.round(score.mcap)
        );

        if (isBetterBirdeyeScore_(score, bestScore)) {
          bestChain = chain;
          bestScore = score;
          bestData = probe.data;
        }
      }
    }

    Utilities.sleep(CONFIG.CHAIN_SCAN_SLEEP_MS);
  }

  if (bestData) {
    ctx.chain = bestChain;
    log.push(
      '  [BE re-scan] winner: ' + bestChain +
      ' (liq=' + Math.round(bestScore.liquidity) +
      ', vol=' + Math.round(bestScore.volume) +
      ', mcap=' + Math.round(bestScore.mcap) + ')'
    );
    applyBirdeyeDataToContext_(ctx, bestChain, bestData, log);
    return true;
  }

  log.push('  [BE re-scan] no valid data on any chain - keeping existing chain');
  return false;
}

function stepGeckoTerminalFallback_(ctx, log) {
  var gtNetwork = GT_NETWORK[ctx.chain] || ctx.chain;

  try {
    var gtResult = fetchGeckoTerminalInfo_(gtNetwork, ctx.address);
    if (gtResult) {
      log.push('  [GT] twitter: ' + (gtResult.twitter || 'n/a') + ' | tg: ' + (gtResult.telegram || 'n/a'));

      if (gtResult.twitter && !ctx.xProfile) {
        if (gtResult.twitter.indexOf('/status/') === -1) {
          ctx.xProfile = 'https://x.com/' + gtResult.twitter;
          log.push('    -> X profile (GT): ' + ctx.xProfile);
        } else {
          log.push('    -> X handle is a tweet link, skipping');
        }
      }

      if (gtResult.telegram && !ctx.tg) {
        ctx.tg = 'https://t.me/' + gtResult.telegram;
        log.push('    -> TG (GT): ' + ctx.tg);
      }

      // v8: write community if GT has one
      if (gtResult.discord && !ctx.xCommunity) {
        // GT returns discord, not x community - skip for xCommunity but log it
        log.push('  [GT] discord: ' + gtResult.discord + ' (not mapped to x_community)');
      }

      if (gtResult.poolCreatedAt) {
        var gtDate = parseAnyDate_(gtResult.poolCreatedAt);
        if (gtDate) {
          ctx.age = calculateAgeDays_(gtDate);
          log.push('  [GT] age: ' + ctx.age + 'd');
        }
      }
    } else {
      log.push('  [GT] no data');
    }
  } catch (err) {
    log.push('  [GT] ERROR: ' + err.message);
  }

  Utilities.sleep(CONFIG.GT_SLEEP_MS);
}

function stepDexScreenerSocialFallback_(ctx, log) {
  if (!ctx.dsResult) {
    try {
      ctx.dsResult = fetchDexScreenerToken_(ctx.address);
    } catch (err) {
      log.push('  [DS fallback] ERROR: ' + err.message);
    }
    Utilities.sleep(CONFIG.API_SLEEP_MS);
  }

  if (ctx.dsResult && ctx.dsResult.socials && ctx.dsResult.socials.length > 0) {
    for (var s = 0; s < ctx.dsResult.socials.length; s++) {
      var social = ctx.dsResult.socials[s];
      if (!social || !social.url) continue;
      var socialType = normalize_(social.type).toLowerCase();

      if (socialType === 'twitter' && !ctx.xProfile) {
        var xUrl = normalizeXUrl_(social.url, false);
        if (xUrl) {
          ctx.xProfile = xUrl;
          log.push('    -> X profile (DS): ' + xUrl);
        }
      } else if (socialType === 'telegram' && !ctx.tg) {
        var tgUrl = normalizeTelegramUrl_(social.url);
        if (tgUrl) {
          ctx.tg = tgUrl;
          log.push('    -> TG (DS): ' + tgUrl);
        }
      }
    }
  }

  if (ctx.dsResult && ctx.dsResult.chain && (!ctx.chain || ctx.chain === 'evm')) {
    ctx.chain = chainKey_(ctx.dsResult.chain);
  }

  // v8: last-chance age pickup
  if (!normalize_(ctx.age) && ctx.dsResult && ctx.dsResult.pairCreatedAt) {
    var dsDate2 = parseAnyDate_(ctx.dsResult.pairCreatedAt);
    if (dsDate2) {
      ctx.age = calculateAgeDays_(dsDate2);
      log.push('  [DS] age (fallback): ' + ctx.age + 'd');
    }
  }
}


// =============================================================================
// BIRDEYE DATA APPLICATION
// =============================================================================

function applyBirdeyeDataToContext_(ctx, chain, data, log) {
  ctx.chain = chain;
  log.push('  [BE] ' + (data.symbol || '?') + ' / ' + (data.name || '?'));

  if (!ctx.project && normalize_(data.symbol) && normalize_(data.symbol) !== '?') {
    ctx.project = formatProjectName_(data.symbol, data.name);
  }

  var mcap = findField_(data, ['mc', 'marketcap', 'market_cap', 'fdv']);
  var vol = findField_(data, ['v24husd', 'v24hUSD', 'volume24h', 'volume_24h_usd']);
  var change = findField_(data, ['pricechange24hpercent', 'priceChange24hPercent']);

  if (hasValue_(mcap)) ctx.mcap = Math.round(Number(mcap));
  if (hasValue_(vol)) ctx.volume24h = Math.round(Number(vol));
  if (hasValue_(change)) ctx.change24h = Math.round(Number(change) * 100) / 100;

  log.push(
    '  [BE] mcap: ' + (hasValue_(mcap) ? Math.round(Number(mcap)) : 'n/a') +
    ' | vol24h: ' + (hasValue_(vol) ? Math.round(Number(vol)) : 'n/a') +
    ' | change24h: ' + (hasValue_(change) ? (Math.round(Number(change) * 100) / 100) + '%' : 'n/a')
  );

  var created = findField_(data, ['createdat', 'tokencreatedat', 'createtime', 'created_at']);
  if (hasValue_(created)) {
    var createdDate = parseAnyDate_(created);
    if (createdDate) {
      ctx.age = calculateAgeDays_(createdDate);
      log.push('  [BE] age: ' + ctx.age + 'd');
    }
  }

  var candidates = extractSocialCandidates_(data);
  for (var c = 0; c < candidates.length; c++) {
    var classified = classifySocialCandidate_(candidates[c]);
    if (!classified || !classified.url) continue;

    if (classified.type === 'profile' && !ctx.xProfile) {
      ctx.xProfile = classified.url;
      log.push('    -> X profile (BE): ' + classified.url);
    } else if (classified.type === 'telegram' && !ctx.tg) {
      ctx.tg = classified.url;
      log.push('    -> TG (BE): ' + classified.url);
    } else if (classified.type === 'community' && !ctx.xCommunity) {
      ctx.xCommunity = classified.url;
      log.push('    -> X community (BE): ' + classified.url);
    }
  }
}

// v8: write DS market data to ctx (same shape as applyBirdeyeDataToContext_)
function applyDexScreenerMarketData_(ctx, log) {
  if (!ctx.dsResult) return;

  var mcap = ctx.dsResult.marketCap;
  var vol = ctx.dsResult.volume24h;
  var change = ctx.dsResult.priceChange24h;

  if (hasValue_(mcap)) ctx.mcap = Math.round(Number(mcap));
  if (hasValue_(vol)) ctx.volume24h = Math.round(Number(vol));
  if (hasValue_(change)) ctx.change24h = Math.round(Number(change) * 100) / 100;

  log.push(
    '  [DS market] mcap: ' + (hasValue_(mcap) ? Math.round(Number(mcap)) : 'n/a') +
    ' | vol24h: ' + (hasValue_(vol) ? Math.round(Number(vol)) : 'n/a') +
    ' | change24h: ' + (hasValue_(change) ? (Math.round(Number(change) * 100) / 100) + '%' : 'n/a')
  );
}

function isGarbageBirdeyeRecord_(data) {
  var symbol = normalize_(data.symbol);
  var name = normalize_(data.name);

  var mcap = getNumericField_(data, ['mc', 'marketcap', 'market_cap', 'fdv']);
  var vol = getNumericField_(data, ['v24husd', 'v24hUSD', 'volume24h', 'volume_24h_usd']);

  var symbolUseless = !symbol || symbol === '?';
  var nameUseless = !name || name === '?';

  return symbolUseless && nameUseless && mcap <= 0 && vol <= 0;
}


// =============================================================================
// ROW HELPERS
// =============================================================================

function logRowSummary_(ctx, log) {
  var filled = [];
  var gaps = [];

  if (ctx.xProfile && !ctx.beforeXProfile) filled.push('x_profile');
  if (ctx.tg && !ctx.beforeTg) filled.push('tg');
  if (ctx.xCommunity && !ctx.beforeXCommunity) filled.push('x_community');
  if (normalize_(ctx.mcap)) filled.push('mcap');
  if (normalize_(ctx.volume24h)) filled.push('vol');
  if (normalize_(ctx.change24h)) filled.push('change');

  if (!ctx.xProfile) gaps.push('x_profile');
  if (!ctx.tg) gaps.push('tg');
  if (!ctx.xCommunity) gaps.push('x_community');
  if (!normalize_(ctx.ds) && isConcreteChain_(ctx.chain)) gaps.push('ds');

  log.push('  FILLED: ' + (filled.length > 0 ? filled.join(', ') : 'none'));
  if (gaps.length > 0) log.push('  GAPS: ' + gaps.join(', '));
  log.push('');
}

function ensureRowsWideEnough_(values, width) {
  for (var r = 0; r < values.length; r++) {
    while (values[r].length < width) values[r].push('');
  }
}


// =============================================================================
// SAFE FALLBACK HELPERS
// =============================================================================

function isConcreteChain_(chain) {
  var c = chainKey_(chain);
  return !!c && c !== 'evm';
}

function restoreSafeFallbacks_(ctx) {
  if (!isConcreteChain_(ctx.chain) && isConcreteChain_(ctx.originalChain)) {
    ctx.chain = ctx.originalChain;
  }

  if (isConcreteChain_(ctx.chain)) {
    ctx.ds = buildDexScreenerUrl_(ctx.chain, ctx.address);
  } else if (ctx.originalDs) {
    ctx.ds = ctx.originalDs;
  }

  if (!ctx.chain) {
    ctx.chain = chainKey_(defaultChainFromAddress_(ctx.address));
  }
}


// =============================================================================
// AUTO-ENRICH TRIGGER
// =============================================================================

function setupAutoEnrich() {
  deleteTriggersByHandler_('autoEnrich');
  ScriptApp.newTrigger('autoEnrich').timeBased().everyHours(6).create();
  toast_('Auto-enrich enabled (every 6 hours)', 5);
}

function removeAutoEnrich() {
  var removed = deleteTriggersByHandler_('autoEnrich');
  toast_(removed > 0 ? 'Auto-enrich disabled' : 'No auto-enrich trigger was active', 5);
}

function deleteTriggersByHandler_(handlerName) {
  var triggers = ScriptApp.getProjectTriggers();
  var removed = 0;

  for (var i = 0; i < triggers.length; i++) {
    if (triggers[i].getHandlerFunction() === handlerName) {
      ScriptApp.deleteTrigger(triggers[i]);
      removed++;
    }
  }

  return removed;
}


// =============================================================================
// PENDING-ENRICH TRIGGER (v8)
// =============================================================================

/**
 * Debounced pending-enrichment trigger. Runs every 1 minute.
 * Exits immediately if user edited sheet within last PENDING_DEBOUNCE_MS.
 * Only enriches rows that have never been auto-enriched.
 */
function runPendingEnrichment() {
  var lastEditStr = PropertiesService.getScriptProperties().getProperty(CONFIG.LAST_EDIT_PROP);
  if (lastEditStr) {
    var lastEdit = Number(lastEditStr);
    if (!isNaN(lastEdit)) {
      var elapsed = Date.now() - lastEdit;
      if (elapsed < CONFIG.PENDING_DEBOUNCE_MS) {
        return;
      }
    }
  }

  var sheet = getTargetSheet_();
  if (!sheet) return;

  var lock = LockService.getScriptLock();
  if (!lock.tryLock(CONFIG.AUTO_LOCK_WAIT_MS)) return;

  try {
    enrichRows_(
      sheet,
      CONFIG.DATA_START_ROW,
      null,
      true,
      PROCESS_MODE.PENDING,
      {
        batchSize: CONFIG.PENDING_BATCH_SIZE,
        skipSort: false
      }
    );
  } finally {
    lock.releaseLock();
  }
}

function setupPendingEnrich() {
  deleteTriggersByHandler_('runPendingEnrichment');
  ScriptApp.newTrigger('runPendingEnrichment').timeBased().everyMinutes(1).create();
  toast_('Pending-enrich enabled (every 1 min, 2-min debounce)', 5);
}

function removePendingEnrich() {
  var removed = deleteTriggersByHandler_('runPendingEnrichment');
  toast_(removed > 0 ? 'Pending-enrich disabled' : 'No pending-enrich trigger was active', 5);
}


// =============================================================================
// DIAGNOSTIC
// =============================================================================

function runDiagnostic() {
  var log = [];
  log.push('CRM DIAGNOSTIC REPORT (v8 DS-first + pending)');
  log.push('Timestamp: ' + new Date().toISOString());
  log.push('User: ' + Session.getActiveUser().getEmail());
  log.push('');

  var apiKey = getBirdeyeApiKey_();
  log.push('--- CONFIG ---');
  log.push('BIRDEYE_API_KEY: ' + (apiKey ? 'SET' : 'MISSING'));

  var triggers = ScriptApp.getProjectTriggers();
  log.push('Triggers: ' + triggers.length);
  for (var t = 0; t < triggers.length; t++) {
    log.push('  ' + triggers[t].getHandlerFunction() + ' (' + triggers[t].getEventType() + ')');
  }
  log.push('');

  var sheet = getTargetSheet_();
  if (!sheet) {
    log.push('ERROR: Sheet not found');
    writeDiagnostic_(log.join('\n'));
    return;
  }

  var lastRow = sheet.getLastRow();
  var dataRows = lastRow >= CONFIG.DATA_START_ROW ? lastRow - CONFIG.DATA_START_ROW + 1 : 0;
  log.push('--- SHEET STATE ---');
  log.push('Data rows: ' + dataRows);

  if (dataRows === 0) {
    log.push('No data rows');
    log.push('DIAGNOSTIC COMPLETE');
    writeDiagnostic_(log.join('\n'));
    toast_('Diagnostic done - check _diagnostic tab', 5);
    return;
  }

  var numCols = Math.max(sheet.getLastColumn(), COL.LAST_AUTO_ENRICH);
  var values = sheet.getRange(CONFIG.DATA_START_ROW, 1, dataRows, numCols).getValues();

  var counts = {
    addr: 0,
    chain: 0,
    project: 0,
    xProf: 0,
    tg: 0,
    xComm: 0,
    ds: 0,
    mcap: 0,
    vol: 0,
    change: 0,
    age: 0,
    evm: 0,
    enrich: 0,
    pending: 0
  };

  for (var r = 0; r < dataRows; r++) {
    if (normalize_(values[r][idx_(COL.ADDRESS)])) counts.addr++;
    var ch = chainKey_(values[r][idx_(COL.CHAIN)]);
    if (ch) {
      counts.chain++;
      if (ch === 'evm') counts.evm++;
    }
    if (normalize_(values[r][idx_(COL.PROJECT)])) counts.project++;
    if (normalize_(values[r][idx_(COL.X_PROFILE)])) counts.xProf++;
    if (normalize_(values[r][idx_(COL.TG)])) counts.tg++;
    if (normalize_(values[r][idx_(COL.X_COMMUNITY)])) counts.xComm++;
    if (normalize_(values[r][idx_(COL.DS)])) counts.ds++;
    if (normalize_(values[r][idx_(COL.MCAP)])) counts.mcap++;
    if (normalize_(values[r][idx_(COL.VOLUME_24H)])) counts.vol++;
    if (normalize_(values[r][idx_(COL.CHANGE_24H)])) counts.change++;
    if (normalize_(values[r][idx_(COL.AGE)])) counts.age++;
    if (rowNeedsEnrichment_(values[r])) counts.enrich++;
    if (rowIsPending_(values[r])) counts.pending++;
  }

  log.push('');
  log.push('--- COVERAGE ---');
  log.push('Address:     ' + counts.addr + '/' + dataRows);
  log.push('Chain:       ' + counts.chain + '/' + counts.addr + ' (EVM placeholder: ' + counts.evm + ')');
  log.push('Project:     ' + counts.project + '/' + counts.addr);
  log.push('X profile:   ' + counts.xProf + '/' + counts.addr);
  log.push('Telegram:    ' + counts.tg + '/' + counts.addr);
  log.push('X community: ' + counts.xComm + '/' + counts.addr);
  log.push('DS URL:      ' + counts.ds + '/' + counts.addr);
  log.push('Market Cap:  ' + counts.mcap + '/' + counts.addr);
  log.push('24h Volume:  ' + counts.vol + '/' + counts.addr);
  log.push('24h Change:  ' + counts.change + '/' + counts.addr);
  log.push('Age:         ' + counts.age + '/' + counts.addr);
  log.push('Need enrich: ' + counts.enrich);
  log.push('Pending:     ' + counts.pending);

  var dupeSet = findDuplicateIndices_(values);
  log.push('Duplicates:  ' + Object.keys(dupeSet).length + ' rows');
  log.push('');

  var testAddr = '';
  var testChain = '';
  var testRow = -1;

  for (var i = 0; i < dataRows; i++) {
    var a = normalize_(values[i][idx_(COL.ADDRESS)]);
    if (a) {
      testAddr = a;
      testChain = chainKey_(values[i][idx_(COL.CHAIN)]);
      testRow = CONFIG.DATA_START_ROW + i;
      break;
    }
  }

  if (testAddr) {
    log.push('--- TEST: ' + abbreviate_(testAddr, 20) + ' (row ' + testRow + ') ---');

    try {
      var dsResult = fetchDexScreenerToken_(testAddr);
      log.push('[DS] ' + (dsResult ? 'OK chain=' + dsResult.chain + ' socials=' + dsResult.socials.length + ' pairCreatedAt=' + (dsResult.pairCreatedAt || 'n/a') + ' mcap=' + (dsResult.marketCap || 'n/a') : 'NO DATA'));
    } catch (err) {
      log.push('[DS] ERROR: ' + err.message);
    }

    if (apiKey) {
      var beChains = isEvmAddress_(testAddr) ? EVM_SCAN_CHAINS : [testChain];
      for (var c = 0; c < beChains.length; c++) {
        if (!beChains[c]) continue;

        try {
          var beResult = fetchBirdeyeOverviewWithRetry_(testAddr, beChains[c], apiKey, log, '[BE diag] ' + beChains[c]);
          if (beResult && beResult.success && beResult.data) {
            var bd = beResult.data;
            var score = getBirdeyeScore_(bd);
            log.push(
              '[BE ' + beChains[c] + '] token=' + (bd.symbol || '?') + '/' + (bd.name || '?') +
              ' liq=' + Math.round(score.liquidity) +
              ' vol=' + Math.round(score.volume) +
              ' mcap=' + Math.round(score.mcap) +
              ' garbage=' + (isGarbageBirdeyeRecord_(bd) ? 'yes' : 'no')
            );
          } else {
            log.push('[BE ' + beChains[c] + '] NO DATA');
          }
        } catch (err2) {
          log.push('[BE ' + beChains[c] + '] ERROR: ' + err2.message);
        }
      }
    }

    var gtNet = GT_NETWORK[testChain] || testChain;
    if (gtNet) {
      try {
        var gtResult = fetchGeckoTerminalInfo_(gtNet, testAddr);
        log.push(
          gtResult
            ? '[GT] twitter=' + (gtResult.twitter || 'n/a') + ' tg=' + (gtResult.telegram || 'n/a') + ' poolAge=' + (gtResult.poolCreatedAt || 'n/a')
            : '[GT] NO DATA'
        );
      } catch (err3) {
        log.push('[GT] ERROR: ' + err3.message);
      }
    }
  }

  log.push('');
  var t0 = Date.now();
  runMaintenance_(sheet);
  log.push('Maintenance: ' + (Date.now() - t0) + 'ms');
  log.push('');
  log.push('DIAGNOSTIC COMPLETE');

  writeDiagnostic_(log.join('\n'));
  toast_('Diagnostic done - check _diagnostic tab', 5);
}


// =============================================================================
// MANUAL UTILITIES
// =============================================================================

function forceRefresh() {
  var sheet = getTargetSheet_();
  if (!sheet) return;
  runMaintenance_(sheet);
  toast_('Refreshed', 3);
}

function debugCurrentRow() {
  var sheet = getTargetSheet_();
  if (!sheet) return;

  var rowNumber = SpreadsheetApp.getActiveRange().getRow();
  if (rowNumber < CONFIG.DATA_START_ROW) {
    toast_('Select row 3+', 3);
    return;
  }

  var address = normalize_(sheet.getRange(rowNumber, COL.ADDRESS).getValue());
  var chain = chainKey_(sheet.getRange(rowNumber, COL.CHAIN).getValue());

  var log = [];
  log.push('DEBUG ROW ' + rowNumber);
  log.push('Address: ' + address + ' | Chain: ' + (chain || '[blank]'));

  if (!address) {
    writeDiagnostic_(log.join('\n'));
    return;
  }

  var dsResult = fetchDexScreenerToken_(address);
  log.push('--- DS ---');
  log.push(dsResult ? JSON.stringify(dsResult, null, 2) : 'No data');

  var apiKey = getBirdeyeApiKey_();
  if (apiKey) {
    var chainsToTest = isEvmAddress_(address) ? EVM_SCAN_CHAINS : [chain];
    for (var i = 0; i < chainsToTest.length; i++) {
      if (!chainsToTest[i]) continue;
      log.push('--- BE [' + chainsToTest[i] + '] ---');

      var beResult = fetchBirdeyeOverviewWithRetry_(address, chainsToTest[i], apiKey, log, '[BE debug] ' + chainsToTest[i]);
      if (beResult && beResult.data) {
        var score = getBirdeyeScore_(beResult.data);
        log.push('token: ' + (beResult.data.symbol || '?') + ' / ' + (beResult.data.name || '?'));
        log.push('liq: ' + Math.round(score.liquidity) + ' | mcap: ' + Math.round(score.mcap) + ' | vol: ' + Math.round(score.volume));
        log.push('garbage: ' + (isGarbageBirdeyeRecord_(beResult.data) ? 'yes' : 'no'));
      } else {
        log.push('No data');
      }
    }
  }

  var gtNet = GT_NETWORK[chain] || chain;
  if (gtNet) {
    log.push('--- GT [' + gtNet + '] ---');
    var gtResult = fetchGeckoTerminalInfo_(gtNet, address);
    log.push(gtResult ? JSON.stringify(gtResult, null, 2) : 'No data');
  }

  writeDiagnostic_(log.join('\n'));
  toast_('Debug done - check _diagnostic tab', 5);
}


// =============================================================================
// DEXSCREENER API (v8: captures pairCreatedAt, marketCap, volume, priceChange)
// =============================================================================

function fetchDexScreenerToken_(address) {
  // DexScreener deprecated /tokens/v1/{address} (returns 404).
  // Use /latest/dex/tokens/{address} instead - response shape is different.
  var url = 'https://api.dexscreener.com/latest/dex/tokens/' + encodeURIComponent(address);

  try {
    var response = UrlFetchApp.fetch(url, {
      muteHttpExceptions: true,
      headers: {
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
      }
    });
    if (response.getResponseCode() !== 200) return null;

    var body = JSON.parse(response.getContentText());
    // response shape: { schemaVersion, pairs: [...] }
    var pairs = body && body.pairs;
    if (!Array.isArray(pairs) || pairs.length === 0) return null;

    pairs.sort(function(a, b) {
      return ((b.liquidity && b.liquidity.usd) || 0) - ((a.liquidity && a.liquidity.usd) || 0);
    });

    var best = pairs[0];

    var marketCap = best.marketCap || (best.fdv || null);
    var volume24h = (best.volume && best.volume.h24) || null;
    var priceChange24h = (best.priceChange && best.priceChange.h24) || null;

    return {
      chain: best.chainId || '',
      name: (best.baseToken && best.baseToken.name) || '',
      symbol: (best.baseToken && best.baseToken.symbol) || '',
      socials: (best.info && best.info.socials) || [],
      websites: (best.info && best.info.websites) || [],
      pairCreatedAt: best.pairCreatedAt || null,
      marketCap: marketCap,
      volume24h: volume24h,
      priceChange24h: priceChange24h
    };
  } catch (err) {
    Logger.log('DS error: ' + err.message);
    return null;
  }
}


// =============================================================================
// BIRDEYE API
// =============================================================================

function fetchBirdeyeOverview_(address, chain, apiKey) {
  var url = 'https://public-api.birdeye.so/defi/token_overview?address=' + encodeURIComponent(address);

  try {
    var response = UrlFetchApp.fetch(url, {
      muteHttpExceptions: true,
      headers: {
        accept: 'application/json',
        'x-chain': chain,
        'X-API-KEY': apiKey
      }
    });

    if (response.getResponseCode() !== 200) return null;
    return JSON.parse(response.getContentText());
  } catch (err) {
    Logger.log('BE error [' + chain + ']: ' + err.message);
    return null;
  }
}

function fetchBirdeyeOverviewWithRetry_(address, chain, apiKey, log, label) {
  var attempts = CONFIG.BIRDEYE_RETRY_COUNT;

  for (var i = 1; i <= attempts; i++) {
    var result = fetchBirdeyeOverview_(address, chain, apiKey);
    if (result) return result;

    if (log) {
      log.push((label || '[BE]') + ': no response' + (i < attempts ? ' - retry ' + (i + 1) + '/' + attempts : ''));
    }

    if (i < attempts) {
      Utilities.sleep(CONFIG.CHAIN_SCAN_SLEEP_MS);
    }
  }

  return null;
}


// =============================================================================
// GECKOTERMINAL API (v8: /pools retries up to 2x)
// =============================================================================

function fetchGeckoTerminalInfo_(network, address) {
  var infoUrl = 'https://api.geckoterminal.com/api/v2/networks/' + encodeURIComponent(network) +
    '/tokens/' + encodeURIComponent(address) + '/info';

  var result = {
    twitter: null,
    telegram: null,
    discord: null,
    websites: [],
    poolCreatedAt: null
  };

  try {
    var infoResp = UrlFetchApp.fetch(infoUrl, { muteHttpExceptions: true });
    if (infoResp.getResponseCode() === 200) {
      var infoData = JSON.parse(infoResp.getContentText());
      var attrs = infoData && infoData.data && infoData.data.attributes;
      if (attrs) {
        result.twitter = attrs.twitter_handle || null;
        result.telegram = attrs.telegram_handle || null;
        result.discord = attrs.discord_url || null;
        result.websites = attrs.websites || [];
      }
    }
  } catch (err) {
    Logger.log('GT info error: ' + err.message);
  }

  Utilities.sleep(CONFIG.GT_POOL_SLEEP_MS);

  var poolUrl = 'https://api.geckoterminal.com/api/v2/networks/' + encodeURIComponent(network) +
    '/tokens/' + encodeURIComponent(address) + '/pools';

  var poolAttempts = 2;
  for (var i = 1; i <= poolAttempts; i++) {
    try {
      var poolResp = UrlFetchApp.fetch(poolUrl, { muteHttpExceptions: true });
      if (poolResp.getResponseCode() === 200) {
        var poolData = JSON.parse(poolResp.getContentText());
        if (poolData && poolData.data && poolData.data.length > 0) {
          var topPool = poolData.data[0];
          if (topPool.attributes && topPool.attributes.pool_created_at) {
            result.poolCreatedAt = topPool.attributes.pool_created_at;
          }
        }
        break;
      }
      if (i < poolAttempts) Utilities.sleep(CONFIG.GT_POOL_SLEEP_MS);
    } catch (err2) {
      Logger.log('GT pool error (attempt ' + i + '): ' + err2.message);
      if (i < poolAttempts) Utilities.sleep(CONFIG.GT_POOL_SLEEP_MS);
    }
  }

  if (!result.twitter && !result.telegram && !result.poolCreatedAt) return null;
  return result;
}


// =============================================================================
// SOCIAL EXTRACTION
// =============================================================================

function extractSocialCandidates_(data) {
  var out = [];
  var seen = {};

  function push(path, value) {
    var v = normalize_(value);
    if (!v) return;

    var key = (String(path || '') + '|' + v).toLowerCase();
    if (seen[key]) return;

    seen[key] = true;
    out.push({ path: String(path || '').toLowerCase(), value: v });
  }

  function walk(node, path) {
    if (node == null) return;

    if (typeof node === 'string' || typeof node === 'number') {
      push(path, String(node));
      return;
    }

    if (Array.isArray(node)) {
      for (var i = 0; i < node.length; i++) walk(node[i], path);
      return;
    }

    if (typeof node === 'object') {
      var keys = Object.keys(node);
      for (var k = 0; k < keys.length; k++) {
        walk(node[keys[k]], path ? path + '.' + keys[k] : keys[k]);
      }
    }
  }

  var likelyKeys = ['twitter', 'telegram', 'x', 'community', 'extensions', 'socials', 'links', 'websites'];
  for (var i = 0; i < likelyKeys.length; i++) {
    if (data[likelyKeys[i]] != null) walk(data[likelyKeys[i]], likelyKeys[i]);
  }

  walk(data, 'data');
  return out;
}

function classifySocialCandidate_(candidate) {
  var path = normalize_(candidate.path).toLowerCase();
  var value = normalize_(candidate.value);

  if (looksLikeXCommunityUrl_(value)) {
    var communityUrl = normalizeXUrl_(value, true);
    if (communityUrl) return { type: 'community', url: communityUrl };
  }

  var pathTg = /(^|\.)(telegram|tg)(\.|$)/i.test(path);
  if (pathTg || looksLikeTelegramUrl_(value)) {
    var tgUrl = normalizeTelegramUrl_(value);
    if (tgUrl) return { type: 'telegram', url: tgUrl };
  }

  var pathX = /(^|\.)(twitter|x|community)(\.|$)/i.test(path);
  if (pathX || looksLikeXUrl_(value) || looksLikeHandle_(value)) {
    var xUrl = normalizeXUrl_(value, pathX);
    if (xUrl) {
      return {
        type: xUrl.toLowerCase().indexOf('/i/communities/') !== -1 ? 'community' : 'profile',
        url: xUrl
      };
    }
  }

  return null;
}


// =============================================================================
// DIAGNOSTIC OUTPUT
// =============================================================================

function writeDiagnostic_(report) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var diagSheet = ss.getSheetByName(CONFIG.DIAG_SHEET_NAME);

  if (!diagSheet) {
    diagSheet = ss.insertSheet(CONFIG.DIAG_SHEET_NAME);
    diagSheet.setColumnWidth(1, 1000);
  }

  var cell = diagSheet.getRange(1, 1);
  cell.setNumberFormat('@');

  var existing = '';
  try {
    if (diagSheet.getLastRow() > 0) existing = String(cell.getValue() || '');
  } catch (_) {}

  var separator = '\n\n' + Array(61).join('-') + '\n\n';
  var combined = report + (existing ? separator + existing : '');
  if (combined.length > 30000) {
    combined = combined.substring(0, 30000) + '\n\n[older logs truncated]';
  }

  cell.setValue(combined).setFontFamily('Courier New').setFontSize(9).setWrap(true);
}


// =============================================================================
// HELPERS
// =============================================================================

function findField_(obj, names) {
  if (!obj || typeof obj !== 'object') return '';

  var objKeys = Object.keys(obj);
  for (var n = 0; n < names.length; n++) {
    var target = names[n].toLowerCase();
    for (var k = 0; k < objKeys.length; k++) {
      if (objKeys[k].toLowerCase() === target) {
        var val = obj[objKeys[k]];
        if (val !== null && val !== undefined && val !== '') return val;
      }
    }
  }

  return '';
}

function getNumericField_(obj, names) {
  var raw = findField_(obj, names);
  var num = Number(raw);
  return isNaN(num) ? 0 : num;
}

function getBirdeyeScore_(data) {
  return {
    liquidity: getNumericField_(data, ['liquidity']),
    volume: getNumericField_(data, ['v24husd', 'v24hUSD', 'volume24h', 'volume_24h_usd']),
    mcap: getNumericField_(data, ['mc', 'marketcap', 'market_cap', 'fdv'])
  };
}

function isBetterBirdeyeScore_(a, b) {
  if (!b) return true;
  if (a.liquidity !== b.liquidity) return a.liquidity > b.liquidity;
  if (a.volume !== b.volume) return a.volume > b.volume;
  if (a.mcap !== b.mcap) return a.mcap > b.mcap;
  return false;
}

function getTargetSheet_() {
  return SpreadsheetApp.getActiveSpreadsheet().getSheetByName(CONFIG.SHEET_NAME);
}

function getBirdeyeApiKey_() {
  return PropertiesService.getScriptProperties().getProperty(CONFIG.BIRDEYE_API_KEY_PROP);
}

function isTargetSheet_(sheet) {
  return !!sheet && sheet.getName() === CONFIG.SHEET_NAME;
}

function withScriptLock_(waitMs, failureMessage, fn) {
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(waitMs)) {
    toast_(failureMessage || 'Already running', 5);
    return;
  }

  try {
    fn();
  } finally {
    lock.releaseLock();
  }
}

function toast_(message, seconds) {
  SpreadsheetApp.getActiveSpreadsheet().toast(message, 'CRM', seconds || 3);
}

function idx_(colNumber) { return colNumber - 1; }
function normalize_(value) { return String(value == null ? '' : value).trim(); }
function normalizeColor_(value) { return normalize_(value).toLowerCase(); }
function isNA_(value) { return normalize_(value).toUpperCase() === 'N/A'; }
function hasValue_(value) { return value !== null && value !== undefined && value !== ''; }
function isEvmAddress_(value) { return /^0x[a-fA-F0-9]{40}$/.test(normalize_(value)); }
function isSolanaAddress_(value) { return /^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(normalize_(value)); }
function isValidDate_(value) { return value instanceof Date && !isNaN(value.getTime()); }
function chainKey_(value) { return normalize_(value).toLowerCase(); }

function defaultChainFromAddress_(address) {
  if (isSolanaAddress_(address)) return 'Solana';
  if (isEvmAddress_(address)) return 'EVM';
  return '';
}

function formatChainDisplay_(chain) {
  var key = chainKey_(chain);
  var map = {
    solana: 'Solana',
    ethereum: 'Ethereum',
    base: 'Base',
    bsc: 'BSC',
    arbitrum: 'Arbitrum',
    polygon: 'Polygon',
    optimism: 'Optimism',
    avalanche: 'Avalanche',
    evm: 'EVM'
  };
  return map[key] || chain;
}

function abbreviate_(value, maxLen) {
  var s = normalize_(value);
  return !s ? '[blank]' : s.length > maxLen ? s.substring(0, maxLen) + '...' : s;
}

function formatProjectName_(symbol, name) {
  var s = normalize_(symbol);
  var n = normalize_(name);
  if (s && n) return s + ' (' + n + ')';
  return s || n;
}

function parseAnyDate_(value) {
  if (!hasValue_(value)) return null;
  var date = typeof value === 'number' ? new Date(value * 1000) : new Date(value);
  return isNaN(date.getTime()) ? null : date;
}

function parseSheetDate_(value) {
  if (value instanceof Date && !isNaN(value.getTime())) return value;
  var s = normalize_(value);
  if (!s) return null;
  var d = new Date(s);
  return isNaN(d.getTime()) ? null : d;
}

function calculateAgeDays_(dateObj) {
  return Math.floor((Date.now() - dateObj.getTime()) / 86400000);
}

function buildDexScreenerUrl_(chain, address) {
  return 'https://dexscreener.com/' + chainKey_(chain) + '/' + normalize_(address).toLowerCase();
}

function looksLikeHandle_(value) {
  return /^@?[A-Za-z0-9_]{1,32}$/.test(normalize_(value));
}

function looksLikeXUrl_(value) {
  return /(?:^|\/\/|www\.)((x\.com)|(twitter\.com)|(mobile\.twitter\.com))\//i.test(normalize_(value));
}

function looksLikeXCommunityUrl_(value) {
  return /(x\.com|twitter\.com|mobile\.twitter\.com)\/i\/communities\//i.test(normalize_(value));
}

function looksLikeTelegramUrl_(value) {
  return /(?:^|\/\/|www\.)(t\.me|telegram\.me)\//i.test(normalize_(value));
}

function normalizeXUrl_(value, allowBareHandle) {
  var s = normalize_(value);
  if (!s) return '';

  if (looksLikeXUrl_(s)) {
    s = s.replace(/^https?:\/\//i, '').replace(/^www\./i, '');
    s = s.replace(/^mobile\.twitter\.com\//i, 'x.com/');
    s = s.replace(/^twitter\.com\//i, 'x.com/');

    var parts = s.split('/');
    parts.shift();

    var path = parts.join('/').replace(/[?#].*$/, '').replace(/\/+$/, '');
    if (!path) return '';

    var lower = path.toLowerCase();
    if (lower.indexOf('/status/') !== -1 || lower.indexOf('/statuses/') !== -1) return '';
    if (lower.indexOf('i/communities/') === 0) return 'https://x.com/' + path;

    var first = path.split('/')[0];
    if (!first || ['home', 'intent', 'share', 'search', 'explore', 'hashtag'].indexOf(first.toLowerCase()) !== -1) return '';

    return 'https://x.com/' + path;
  }

  if (allowBareHandle && looksLikeHandle_(s)) {
    s = s.replace(/^@/, '');
    return s ? 'https://x.com/' + s : '';
  }

  return '';
}

function normalizeTelegramUrl_(value) {
  var s = normalize_(value);
  if (!s) return '';

  if (looksLikeTelegramUrl_(s)) {
    s = s.replace(/^https?:\/\//i, '').replace(/^www\./i, '');
    s = s.replace(/^telegram\.me\//i, 't.me/');
    s = s.replace(/^t\.me\//i, 't.me/');

    var parts = s.split('/');
    parts.shift();

    var path = parts.join('/').replace(/[?#].*$/, '').replace(/\/+$/, '');
    if (!path) return '';

    return 'https://t.me/' + path;
  }

  if (looksLikeHandle_(s)) {
    s = s.replace(/^@/, '');
    return s ? 'https://t.me/' + s : '';
  }

  return '';
}

function canAutoEnrichRow_(row) {
  var last = parseSheetDate_(row[idx_(COL.LAST_AUTO_ENRICH)]);
  if (!last) return true;

  var cooldownMs = CONFIG.AUTO_ENRICH_COOLDOWN_HOURS * 60 * 60 * 1000;
  return (Date.now() - last.getTime()) >= cooldownMs;
}

function getAutoSortValue_(row) {
  var last = parseSheetDate_(row[idx_(COL.LAST_AUTO_ENRICH)]);
  return last ? last.getTime() : 0;
}

function compareAutoTargets_(a, b) {
  return a.autoSortValue - b.autoSortValue;
}

// =============================================================================
// DEXSCREENER INGEST (v2) - paste at the bottom of existing CRM Apps Script
// =============================================================================
//
// Accepts POSTs from dexscreener_tracker.py and inserts new CAs at the TOP
// of the data section (row 3), pushing existing rows down. Matches the manual
// entry convention where new CAs go at the top.
//
// Payload shape:
//   {
//     "source": "dexscreener_tracker",
//     "rows": [{"chain": "Solana", "ca": "..."}, ...]
//   }
//
// Dedup: checks col B (ADDRESS) against existing values before inserting.
// Only writes: ADDRESS, CHAIN, STATUS, TIMESTAMP, NOTES, DS.
// Everything else gets filled by the existing enrichment pipeline.

function doPost(e) {
  try {
    var payload = JSON.parse(e.postData.contents);
    var rows = payload.rows || [];

    if (!Array.isArray(rows) || rows.length === 0) {
      return _jsonResponse_({ ok: true, inserted: 0, skipped: 0, msg: 'no rows' });
    }

    var sheet = getTargetSheet_();
    if (!sheet) {
      return _jsonResponse_({ ok: false, error: 'Sheet1 not found' });
    }

    var existingCAs = _getExistingAddresses_(sheet);

    var toInsert = [];
    var seenInBatch = {};
    var skipped = 0;

    for (var i = 0; i < rows.length; i++) {
      var row = rows[i];
      var ca = normalize_(row.ca);
      var chain = normalize_(row.chain);
      if (!ca || !chain) { skipped++; continue; }

      var caKey = ca.toLowerCase();
      if (existingCAs[caKey] || seenInBatch[caKey]) {
        skipped++;
        continue;
      }
      seenInBatch[caKey] = true;

      toInsert.push(_buildSheetRow_(ca, chain));
    }

    if (toInsert.length === 0) {
      return _jsonResponse_({ ok: true, inserted: 0, skipped: skipped });
    }

    // Insert at the TOP of the data section (row 3), pushing existing rows down.
    // insertRowsBefore creates blank rows without inheriting formatting/validation
    // from the row below, which avoids triggering data-validation errors.
    var insertAtRow = CONFIG.DATA_START_ROW;
    sheet.insertRowsBefore(insertAtRow, toInsert.length);

    var numCols = Math.max(sheet.getLastColumn(), COL.LAST_AUTO_ENRICH);
    var padded = toInsert.map(function(r) {
      while (r.length < numCols) r.push('');
      return r;
    });

    // Copy data validation (dropdowns etc.) from an existing reference row.
    // After inserting N rows at row 3, the old row-3 data is now at row 3+N.
    // That row serves as our validation template.
    var referenceRowAfterInsert = insertAtRow + toInsert.length;
    var refLastRow = sheet.getLastRow();
    if (referenceRowAfterInsert <= refLastRow) {
      var referenceValidations = sheet.getRange(referenceRowAfterInsert, 1, 1, numCols)
        .getDataValidations();
      if (referenceValidations && referenceValidations[0]) {
        // tile the single reference row across all inserted rows
        var tiled = [];
        for (var v = 0; v < padded.length; v++) {
          tiled.push(referenceValidations[0]);
        }
        sheet.getRange(insertAtRow, 1, padded.length, numCols).setDataValidations(tiled);
      }
    }

    // Write values
    sheet.getRange(insertAtRow, 1, padded.length, numCols).setValues(padded);

    // Format timestamp column
    sheet.getRange(insertAtRow, COL.TIMESTAMP, padded.length, 1)
      .setNumberFormat(CONFIG.TIMESTAMP_FORMAT);

    // Clear any background that may have been inherited from the inserted rows
    sheet.getRange(insertAtRow, 1, padded.length, numCols).setBackground(null);

    return _jsonResponse_({
      ok: true,
      inserted: toInsert.length,
      skipped: skipped,
      insertedAtRow: insertAtRow
    });

  } catch (err) {
    return _jsonResponse_({ ok: false, error: String(err) });
  }
}

function doGet() {
  return _jsonResponse_({
    ok: true,
    msg: 'dexscreener ingest endpoint - POST {source, rows: [{chain, ca}]}'
  });
}

function _getExistingAddresses_(sheet) {
  var lastRow = sheet.getLastRow();
  var set = {};
  if (lastRow < CONFIG.ENTRY_ROW) return set;

  // scan from ENTRY_ROW down (covers row 2 and all data rows)
  var values = sheet.getRange(CONFIG.ENTRY_ROW, COL.ADDRESS, lastRow - CONFIG.ENTRY_ROW + 1, 1).getValues();
  for (var i = 0; i < values.length; i++) {
    var v = normalize_(values[i][0]);
    if (v) set[v.toLowerCase()] = true;
  }
  return set;
}

function _buildSheetRow_(ca, chain) {
  var row = [];
  for (var c = 0; c < COL.LAST_AUTO_ENRICH; c++) row.push('');

  row[idx_(COL.ADDRESS)] = ca;
  row[idx_(COL.CHAIN)] = chain;
  row[idx_(COL.STATUS)] = 'Lead';
  row[idx_(COL.TIMESTAMP)] = new Date();
  row[idx_(COL.NOTES)] = 'auto: dexscreener tracker ' +
    Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd');

  var chainLower = chain.toLowerCase();
  if (chainLower && chainLower !== 'evm') {
    row[idx_(COL.DS)] = buildDexScreenerUrl_(chainLower, ca);
  }

  return row;
}

function _jsonResponse_(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}
// ============================================================================
// DEXSCREENER CONNECTIVITY TEST
// Paste anywhere in your Apps Script. Run it. Check the execution log.
// ============================================================================

function testDexScreenerFromAppsScript() {
  var testAddress = 'BUVSTjCb9F17HF5mmBBjHbLau92r9LGNRGPKDxj6brrr'; // a solana row from your debug

  var url = 'https://api.dexscreener.com/tokens/v1/' + testAddress;

  try {
    var response = UrlFetchApp.fetch(url, {
      muteHttpExceptions: true,
      headers: {
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
      }
    });

    var code = response.getResponseCode();
    var body = response.getContentText();

    Logger.log('HTTP status: ' + code);
    Logger.log('Response length: ' + body.length);
    Logger.log('First 500 chars of response: ' + body.substring(0, 500));

    // also try /latest/dex/tokens (older endpoint)
    var url2 = 'https://api.dexscreener.com/latest/dex/tokens/' + testAddress;
    var response2 = UrlFetchApp.fetch(url2, {
      muteHttpExceptions: true,
      headers: {
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
      }
    });
    Logger.log('--- older endpoint ---');
    Logger.log('HTTP status: ' + response2.getResponseCode());
    Logger.log('First 500 chars: ' + response2.getContentText().substring(0, 500));

  } catch (err) {
    Logger.log('Exception: ' + err.message);
  }
}
function quickBirdeyeCheck() {
  var key = PropertiesService.getScriptProperties().getProperty('BIRDEYE_API_KEY');
  if (!key) {
    Logger.log('NO KEY SET');
    return;
  }
  Logger.log('Key prefix: ' + key.substring(0, 8) + '... (length ' + key.length + ')');

  // Hit a cheap Solana endpoint with a known token (USDC on Solana)
  var url = 'https://public-api.birdeye.so/defi/price?address=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
  var resp = UrlFetchApp.fetch(url, {
    method: 'get',
    headers: {
      'X-API-KEY': key,
      'x-chain': 'solana',
      'accept': 'application/json'
    },
    muteHttpExceptions: true
  });

  Logger.log('HTTP status: ' + resp.getResponseCode());
  Logger.log('Headers: ' + JSON.stringify(resp.getAllHeaders()));
  Logger.log('Body (first 500 chars): ' + resp.getContentText().substring(0, 500));
}
// =============================================================================
// FULL SYSTEM DIAGNOSTIC
// Run via: CRM menu (after adding line below) OR Apps Script editor → runFullDiagnostic
// =============================================================================

function runFullDiagnostic() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var diagSheet = ss.getSheetByName(CONFIG.DIAG_SHEET_NAME);
  if (!diagSheet) diagSheet = ss.insertSheet(CONFIG.DIAG_SHEET_NAME);

  var output = [];
  var now = new Date();
  output.push('================================================================');
  output.push('FULL SYSTEM DIAGNOSTIC — ' + Utilities.formatDate(now, Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss'));
  output.push('================================================================');
  output.push('');

  // ---------- 1. BIRDEYE HEALTH ----------
  output.push('## 1. BIRDEYE HEALTH');
  output.push('');
  try {
    var key = PropertiesService.getScriptProperties().getProperty(CONFIG.BIRDEYE_API_KEY_PROP);
    if (!key) {
      output.push('  STATUS: NO KEY SET in Script Property "' + CONFIG.BIRDEYE_API_KEY_PROP + '"');
    } else {
      output.push('  Key prefix: ' + key.substring(0, 8) + '... (length ' + key.length + ')');

      // Test 1: Solana known token (USDC)
      var sol = _diagBirdeyeCall(key, 'solana', 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v');
      output.push('  Solana (USDC test): HTTP ' + sol.status + ' | remaining: ' + sol.remaining + '/' + sol.limit);
      if (sol.status !== 200) output.push('    body: ' + sol.body.substring(0, 200));

      // Test 2: Ethereum known token (USDC)
      var eth = _diagBirdeyeCall(key, 'ethereum', '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48');
      output.push('  Ethereum (USDC test): HTTP ' + eth.status + ' | remaining: ' + eth.remaining + '/' + eth.limit);
      if (eth.status !== 200) output.push('    body: ' + eth.body.substring(0, 200));

      // Test 3: Recent Solana token from sheet
      var recentSol = _diagFindRecentByChain('solana');
      if (recentSol) {
        var solRow = _diagBirdeyeCall(key, 'solana', recentSol.address);
        output.push('  Solana (recent sheet token ' + recentSol.address.substring(0, 8) + '...): HTTP ' + solRow.status);
        if (solRow.status !== 200) output.push('    body: ' + solRow.body.substring(0, 200));
      } else {
        output.push('  Solana (recent sheet token): no Solana rows found in sheet');
      }
    }
  } catch (err) {
    output.push('  ERROR: ' + err.message);
  }
  output.push('');

  // ---------- 2. DEXSCREENER VERIFICATION ----------
  output.push('## 2. DEXSCREENER /latest/dex/tokens/ VERIFICATION');
  output.push('');
  try {
    var dsToken = _diagFindRecentByChain('solana');
    if (!dsToken) dsToken = _diagFindRecentByChain('any');
    if (dsToken) {
      var dsUrl = 'https://api.dexscreener.com/latest/dex/tokens/' + dsToken.address;
      var dsResp = UrlFetchApp.fetch(dsUrl, { muteHttpExceptions: true });
      var dsStatus = dsResp.getResponseCode();
      output.push('  URL: ' + dsUrl);
      output.push('  HTTP: ' + dsStatus);
      if (dsStatus === 200) {
        var dsBody = JSON.parse(dsResp.getContentText());
        var pairs = (dsBody && dsBody.pairs) ? dsBody.pairs.length : 0;
        output.push('  Pairs returned: ' + pairs);
        if (pairs > 0) {
          var p = dsBody.pairs[0];
          output.push('  First pair: chain=' + p.chainId + ' | symbol=' + (p.baseToken && p.baseToken.symbol) + ' | mcap=' + p.marketCap + ' | vol24h=' + (p.volume && p.volume.h24));
          output.push('  --> Endpoint fix is LIVE and returning data');
        } else {
          output.push('  --> Endpoint fix is LIVE but returned 0 pairs for this token');
        }
      } else {
        output.push('  body: ' + dsResp.getContentText().substring(0, 200));
      }
    } else {
      output.push('  Could not find a token in the sheet to test against');
    }
  } catch (err) {
    output.push('  ERROR: ' + err.message);
  }
  output.push('');

  // ---------- 3. GITHUB ACTIONS INGEST RATE ----------
  output.push('## 3. INGEST RATE (rows added per 6h window, last 48h)');
  output.push('');
  try {
    var sheet = ss.getSheetByName(CONFIG.SHEET_NAME);
    var lastRow = sheet.getLastRow();
    if (lastRow < CONFIG.DATA_START_ROW) {
      output.push('  No data rows.');
    } else {
      var tsRange = sheet.getRange(CONFIG.DATA_START_ROW, COL.TIMESTAMP, lastRow - CONFIG.DATA_START_ROW + 1, 1).getValues();
      var nowMs = now.getTime();
      var buckets = {
        '0-6h ago':   { start: nowMs - 6*3600*1000,  end: nowMs,                 count: 0 },
        '6-12h ago':  { start: nowMs - 12*3600*1000, end: nowMs - 6*3600*1000,   count: 0 },
        '12-18h ago': { start: nowMs - 18*3600*1000, end: nowMs - 12*3600*1000,  count: 0 },
        '18-24h ago': { start: nowMs - 24*3600*1000, end: nowMs - 18*3600*1000,  count: 0 },
        '24-30h ago': { start: nowMs - 30*3600*1000, end: nowMs - 24*3600*1000,  count: 0 },
        '30-36h ago': { start: nowMs - 36*3600*1000, end: nowMs - 30*3600*1000,  count: 0 },
        '36-42h ago': { start: nowMs - 42*3600*1000, end: nowMs - 36*3600*1000,  count: 0 },
        '42-48h ago': { start: nowMs - 48*3600*1000, end: nowMs - 42*3600*1000,  count: 0 }
      };
      var totalLast48h = 0;
      var older = 0;
      for (var i = 0; i < tsRange.length; i++) {
        var v = tsRange[i][0];
        if (!(v instanceof Date)) continue;
        var t = v.getTime();
        if (t < nowMs - 48*3600*1000) { older++; continue; }
        for (var k in buckets) {
          if (t >= buckets[k].start && t < buckets[k].end) {
            buckets[k].count++;
            totalLast48h++;
            break;
          }
        }
      }
      var ordered = ['0-6h ago','6-12h ago','12-18h ago','18-24h ago','24-30h ago','30-36h ago','36-42h ago','42-48h ago'];
      for (var j = 0; j < ordered.length; j++) {
        var b = buckets[ordered[j]];
        output.push('  ' + ordered[j].padEnd(12) + ': ' + b.count + ' rows');
      }
      output.push('  ----------------------------');
      output.push('  Total last 48h: ' + totalLast48h);
      output.push('  Older than 48h: ' + older);
    }
  } catch (err) {
    output.push('  ERROR: ' + err.message);
  }
  output.push('');

  // ---------- 4. ENRICHMENT SUCCESS BY CHAIN (last 50 rows) ----------
  output.push('## 4. ENRICHMENT SUCCESS BY CHAIN (last 50 rows by timestamp)');
  output.push('');
  try {
    var sh = ss.getSheetByName(CONFIG.SHEET_NAME);
    var lr = sh.getLastRow();
    if (lr < CONFIG.DATA_START_ROW) {
      output.push('  No data.');
    } else {
      var n = Math.min(50, lr - CONFIG.DATA_START_ROW + 1);
      var startRow = lr - n + 1;
      var lastCol = Math.max(COL.CHAIN, COL.MCAP, COL.VOLUME_24H, COL.AGE, COL.X_PROFILE);
      var data = sh.getRange(startRow, 1, n, lastCol).getValues();
      var byChain = {};
      for (var r = 0; r < data.length; r++) {
        var chain = (data[r][COL.CHAIN - 1] || 'unknown').toString().toLowerCase();
        if (!byChain[chain]) byChain[chain] = { total: 0, mcap: 0, vol: 0, age: 0, xProfile: 0 };
        byChain[chain].total++;
        if (data[r][COL.MCAP - 1] !== '' && data[r][COL.MCAP - 1] != null) byChain[chain].mcap++;
        if (data[r][COL.VOLUME_24H - 1] !== '' && data[r][COL.VOLUME_24H - 1] != null) byChain[chain].vol++;
        if (data[r][COL.AGE - 1] !== '' && data[r][COL.AGE - 1] != null) byChain[chain].age++;
        if (data[r][COL.X_PROFILE - 1] !== '' && data[r][COL.X_PROFILE - 1] != null) byChain[chain].xProfile++;
      }
      output.push('  chain        | rows | mcap | vol  | age  | xProfile');
      output.push('  -------------|------|------|------|------|---------');
      for (var c in byChain) {
        var s = byChain[c];
        output.push('  ' + c.padEnd(12) + ' | ' + String(s.total).padEnd(4) + ' | ' + String(s.mcap).padEnd(4) + ' | ' + String(s.vol).padEnd(4) + ' | ' + String(s.age).padEnd(4) + ' | ' + s.xProfile);
      }
    }
  } catch (err) {
    output.push('  ERROR: ' + err.message);
  }
  output.push('');
  output.push('================================================================');
  output.push('END');

  diagSheet.getRange('A1').setValue(output.join('\n'));
  diagSheet.setColumnWidth(1, 800);
  SpreadsheetApp.getUi().alert('Diagnostic complete. Check the _diagnostic tab.');
}


function _diagBirdeyeCall(key, chain, address) {
  var url = 'https://public-api.birdeye.so/defi/price?address=' + address;
  var resp = UrlFetchApp.fetch(url, {
    method: 'get',
    headers: { 'X-API-KEY': key, 'x-chain': chain, 'accept': 'application/json' },
    muteHttpExceptions: true
  });
  var headers = resp.getAllHeaders();
  return {
    status: resp.getResponseCode(),
    remaining: headers['x-ratelimit-remaining'] || 'n/a',
    limit: headers['x-ratelimit-limit'] || 'n/a',
    body: resp.getContentText()
  };
}


function _diagFindRecentByChain(chainFilter) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(CONFIG.SHEET_NAME);
  var lastRow = sheet.getLastRow();
  if (lastRow < CONFIG.DATA_START_ROW) return null;
  var n = Math.min(20, lastRow - CONFIG.DATA_START_ROW + 1);
  var startRow = lastRow - n + 1;
  var data = sheet.getRange(startRow, 1, n, Math.max(COL.ADDRESS, COL.CHAIN)).getValues();
  for (var i = data.length - 1; i >= 0; i--) {
    var addr = data[i][COL.ADDRESS - 1];
    var chain = (data[i][COL.CHAIN - 1] || '').toString().toLowerCase();
    if (!addr) continue;
    if (chainFilter === 'any') return { address: addr, chain: chain };
    if (chain === chainFilter) return { address: addr, chain: chain };
  }
  return null;
}