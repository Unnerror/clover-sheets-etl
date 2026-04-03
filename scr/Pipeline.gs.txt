// Pipeline.gs

/**
 * MASTER FUNCTIONS
 */

// main nightly sequence
function nightlyCloverFetchingPipeline() {
  const ss = SpreadsheetApp.getActive();
  const tz = ss.getSpreadsheetTimeZone();

  // "yesterday"
  const now = new Date();
  const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000);
  const dateStr = Utilities.formatDate(yesterday, tz, 'yyyy-MM-dd')
  //Logger.log(dateStr); '2025-11-30';
  
  // 0) Sync All Mappings with Clover
  syncAllhMappings();

  // 1) Fetch Payments JSON → RawPayments
  pullRawPaymentsForDate(dateStr);

  // 2) Reformat RawPayments → FormattedPayments
  processRawPayments();

  // 3) Link PaymentId to FormattedPayments by OrderId
  linkPaymentIdsToFormattedOrders();

  // 4) Tag PrimarySuccess Orders
  markPrimarySuccessInFormattedPayments();

  // 5) Fetch Raw JSON Orders (lineItems/modifiers) → RawSuccessedOrders
  updateRawOrdersFromFormattedForDateFull(dateStr);

  // -> Call once 6) and 7)
  formattingItemsAndModifiers();
  
}

// sub nightly sequence (need to repeat it more than once/night)
function formattingItemsAndModifiers() {
  const lock = LockService.getScriptLock();
  let gotLock = false;   // Did we get lock
  let flagSet = false;   // Did we set FORMAT_ITEMS_FLAG ourselves

  try {
    gotLock = lock.tryLock(5000); // ждём до 5 секунд
    if (!gotLock) {
      Logger.log('❌ formattingItemsAndModifiers: Another execution is holding the LOCK. Skipping this run.');
      return;
    }

    // At this stage we DEFINITELY have a lock
    const alreadyRunning = SCRIPT_PROPS.getProperty(FORMAT_ITEMS_FLAG);
    if (alreadyRunning === '1') {
      Logger.log('❌ formattingItemsAndModifiers: FLAG says another run is in progress. Skipping this run (keeping existing flag).');
      return;  // важно: флаг НЕ трогаем
    }

    // put up a flag and mark that it was we who put it up
    SCRIPT_PROPS.setProperty(FORMAT_ITEMS_FLAG, '1');
    flagSet = true;
    Logger.log('🔐 Lock + FLAG acquired. Starting formattingItemsAndModifiers()');
    
    // 6) Reformat Order's Items
    buildFormattedOrderItemsModifiersFromRaw();

    // 7) Add Margin formulas to Items
    populateFormattedOrderItemsModifiersProfit();

    Logger.log('✅ formattingItemsAndModifiers finished successfully');

  } catch (err) {
    Logger.log('⚠️ Error in formattingItemsAndModifiers: ' + err);

  } finally {
    // remove the flag ONLY if we put it up
    if (flagSet) {
      SCRIPT_PROPS.deleteProperty(FORMAT_ITEMS_FLAG);
    }

    // releaseLock only if we got it
    if (gotLock) {
      try {
        lock.releaseLock();
      } catch (e) {
        Logger.log('⚠️ Error releasing lock (maybe already released): ' + e);
      }
    }

    Logger.log('🔓 Lock released ' + (flagSet ? '& FLAG cleared.' : '(FLAG kept as-is).'));
  }
}
