// Main.gs

/**
 * PROPERTIES
 */

// for processRawPayments (Row Number of sheet RawPayments)
const LAST_FORMATED_PAYMENT_ROW = 'LAST_FORMATED_PAYMENT_ROW';

// for buildFormattedOrderItemsModifiersFromRaw
const LAST_PROCESSED_RAW_ORDERS_ROW_KEY = 'LAST_PROCESSED_RAW_ORDERS_ROW_FOR_ITEMS'

// for formattingItemsAndModifiers
const FORMAT_ITEMS_FLAG = 'FORMAT_ITEMS_RUNNING';
var RAW_ORDERS_CACHE = null;      // orderId -> json text from RawOrders
var FMT_ORDER_IDS_CACHE = null;   // Set of orderIds already in FormattedOrderItemsModifiers


// for syncModifiersMap
const TAG_GROUP_ID = 'E37E73Z82SBNP'; // special group → Type = TAG

// for all functions
const SCRIPT_PROPS = PropertiesService.getScriptProperties();
const CLOVER_MERCHANT_ID = SCRIPT_PROPS.getProperty('CLOVER_MERCHANT_ID');
const CLOVER_TOKEN = SCRIPT_PROPS.getProperty('CLOVER_ACCESS_TOKEN');
const CLOVER_BASE_URL = 'https://api.clover.com/v3/merchants/' + CLOVER_MERCHANT_ID;

// data sheets
const RAWPAYMENTS_SHEET = 'RawPayments';
const FORMATTEDPAYMENTS_SHEET     = 'FormattedPayments';
const RAWORDERS_SHEET     = 'RawSuccessedOrders';
const FORMATTEDORDERITEMS_SHEET    = 'FormattedOrderItemsModifiers';

// mapping sheets
const EMPLOYEESMAP_SHEET = 'MappingEmployees';
const ITEMSMAP_SHEET = 'MappingItems';
const MODIFIERSMAP_SHEET = 'MappingModifiers';

//temp for discounts pull
const DISCOUNT_LAST_ROW_KEY = 'LAST_PROCESSED_DISCOUNT_ROW';



/**
 * MAIN PIPELINE
 */

// 1) Pull payments from Clover API into RawPayments for a given date
function pullRawPaymentsForDate(dateStr) {
  // dateStr в формате 'yyyy-MM-dd', например '2024-11-20'
  if (!dateStr) {
    throw new Error('pullRawPaymentsForDate: dateStr is required, e.g. "2024-11-20"');
  }

  const props     = PropertiesService.getScriptProperties();
  const merchantId = props.getProperty('CLOVER_MERCHANT_ID');
  const token      = props.getProperty('CLOVER_ACCESS_TOKEN');

  if (!merchantId || !token) {
    throw new Error('Need CLOVER_MERCHANT_ID and CLOVER_ACCESS_TOKEN in Script Properties');
  }

  const ss    = SpreadsheetApp.getActive();
  let sheet   = ss.getSheetByName(RAWPAYMENTS_SHEET);
  if (!sheet) {
    sheet = ss.insertSheet(RAWPAYMENTS_SHEET);
  }

  // Заголовок, если лист пустой
  if (sheet.getLastRow() === 0) {
    sheet.getRange(1, 1, 1, 3).setValues([[
      'CreatedTime', 'PaymentId', 'JSON'
    ]]);
  }

  // ---------- 0. читаем уже существующие PaymentId, чтобы не дублировать ----------
  const lastRow = sheet.getLastRow();
  const existingIds = {};
  if (lastRow >= 2) {
    const idRange = sheet.getRange(2, 2, lastRow - 1, 1); // col B
    idRange.getValues().forEach(r => {
      const pid = String(r[0] || '').trim();
      if (pid) existingIds[pid] = true;
    });
  }

  // ---------- 1. считаем start/end по дате ----------
  // dateStr -> Date at local midnight
  const tz = Session.getScriptTimeZone();
  const parts = dateStr.split('-'); // [yyyy, MM, dd]
  const year  = Number(parts[0]);
  const month = Number(parts[1]) - 1; // JS months 0–11
  const day   = Number(parts[2]);

  const start = new Date(year, month, day);
  const end   = new Date(year, month, day + 1);

  const startMs = start.getTime();
  const endMs   = end.getTime();

  // ---------- 2. тянем платежи из Clover, с пагинацией по cursor ----------
  const baseUrl =
    'https://api.clover.com/v3/merchants/' + merchantId + '/payments';

  const headers = { Authorization: 'Bearer ' + token };
  const options = {
    method: 'get',
    headers,
    muteHttpExceptions: true
  };

  const rowsToAppend = [];
  let cursor = null;
  let page   = 0;

  while (true) {
    page++;

    // фильтры кодируем через encodeURIComponent
    const f1 = encodeURIComponent('createdTime>=' + startMs);
    const f2 = encodeURIComponent('createdTime<'  + endMs);

    let url =
      baseUrl +
      '?filter=' + f1 +
      '&filter=' + f2 +
      '&limit=1000' +
      '&expand=order,employee,tender';

    // если Clover вернул cursor – дальше используем только его
    if (cursor) {
      url = baseUrl + '?cursor=' + encodeURIComponent(cursor);
    }

    const res  = UrlFetchApp.fetch(url, options); // <-- теперь ok
    const code = res.getResponseCode();
    const text = res.getContentText();

    if (code !== 200) {
      Logger.log('[pullRawPaymentsForDate] page ' + page +
                 ' failed: ' + code + ' ' + text.slice(0, 200));
      break;
    }

    let json;
    try {
      json = JSON.parse(text);
    } catch (e) {
      Logger.log('[pullRawPaymentsForDate] JSON parse error page ' + page + ': ' + e);
      break;
    }

    const elements = Array.isArray(json.elements) ? json.elements : [];
    Logger.log('[pullRawPaymentsForDate] page ' + page +
               ', elements: ' + elements.length);

    if (!elements.length) break;

    elements.forEach(p => {
      const paymentId = String(p.id || '').trim();
      if (!paymentId) return;
      if (existingIds[paymentId]) return; // уже есть в листе

      const createdTime = p.createdTime ? new Date(p.createdTime) : new Date();
      const jsonStr = JSON.stringify(p);

      rowsToAppend.push([
        createdTime,
        paymentId,
        jsonStr
      ]);

      existingIds[paymentId] = true;
    });

    // Clover обычно даёт cursor для следующей страницы
    cursor = json.cursor || null;
    if (!cursor) break; // нет следующей страницы
  }

  // ---------- 3. пишем в RawPayments ----------
  if (rowsToAppend.length) {
    const startRow = sheet.getLastRow() + 1;
    sheet.getRange(startRow, 1, rowsToAppend.length, 3)
         .setValues(rowsToAppend);
  }

  Logger.log(
    '[pullRawPaymentsForDate] ' + dateStr +
    ' → appended rows: ' + rowsToAppend.length
  );
}

// 2) Reformating Raw JSONs from sheet RawPayments to FormattedPayments for new rows
function processRawPayments() {
  const ss = SpreadsheetApp.getActive();
  const rawSheet = ss.getSheetByName(RAWPAYMENTS_SHEET);
  const outSheet = ss.getSheetByName(FORMATTEDPAYMENTS_SHEET);

  if (!rawSheet || !outSheet) {
    throw new Error('Need sheets "' + RAWPAYMENTS_SHEET + '" and "' + FORMATTEDPAYMENTS_SHEET + '"');
  }

  const lastRow = rawSheet.getLastRow();
  if (lastRow < 2) return; // только шапка

  const props = PropertiesService.getScriptProperties();

  // Читаем, до какой строки мы уже доходили ранее.
  // По умолчанию 1 (только шапка).
  const lastProcessedStr = props.getProperty(LAST_FORMATED_PAYMENT_ROW);  
  let lastProcessed = lastProcessedStr ? parseInt(lastProcessedStr, 10) : 1;

  // Если новых строк нет — выходим.
  if (lastRow <= lastProcessed) {
    Logger.log('Нет новых строк в "' + RAWPAYMENTS_SHEET + '"');
    return;
  }

  // Новые строки: с (lastProcessed + 1) до lastRow
  const startRow = lastProcessed + 1;
  const numRows = lastRow - lastProcessed;

  Logger.log(
    'Обрабатываем "' + RAWPAYMENTS_SHEET + '" строки ' + startRow + '–' + lastRow
  );

  // Берём диапазон A:C для новых строк
  const rawValues = rawSheet.getRange(startRow, 1, numRows, 3).getValues();

  // Мэппинг сотрудников
  const employeeMap = loadEmployeeMapFromSheet();


  const tz = Session.getScriptTimeZone();
  const output = [];

  for (let i = 0; i < rawValues.length; i++) {
    const [rawTimestamp, , jsonStr] = rawValues[i];

    if (!jsonStr) continue;

    const p = safeParseJsonFromCell(jsonStr);
    if (!p) continue;

    // --- дата/время ---
    let createdDate = null;
    if (p.createdTime) {
      createdDate = new Date(p.createdTime);
    } else if (rawTimestamp) {
      createdDate = new Date(rawTimestamp);
    }

    const dateStr = createdDate
      ? Utilities.formatDate(createdDate, tz, 'yyyy-MM-dd')
      : '';
    const timeStr = createdDate
      ? Utilities.formatDate(createdDate, tz, 'HH:mm:ss')
      : '';

    // --- суммы ---
    const subtotal = (p.amount || 0) / 100;
    const tax = (p.taxAmount || 0) / 100;
    const tip = (p.tipAmount || 0) / 100;
    const total = (p.total || 0) / 100;

    // --- сотрудник ---
    const employeeId = p.employee && p.employee.id ? p.employee.id : '';
    const employeeName = employeeMap[employeeId] || '';

    // --- orderId ---
    const orderHref = p.order && p.order.href ? p.order.href : '';
    const orderId = orderHref ? orderHref.split('/').pop() : '';

    // --- тип оплаты ---
    const tenderLabel =
      (p.tender && (p.tender.label || p.tender.labelKey)) || '';
    let paymentType = 'Other';
    const tl = tenderLabel.toLowerCase();
    if (tl.includes('cash')) paymentType = 'Cash';
    else if (tl.includes('debit')) paymentType = 'Debit';
    else if (tl.includes('credit')) paymentType = 'Credit';

    // --- результат / оффлайн ---
    const result =
      p.result ||
      (p.cardTransaction && p.cardTransaction.state) ||
      '';

    const isOffline = !!p.offline;

    // Items пока пусто — заполним отдельным скриптом
    const items = '';

    output.push([
      dateStr,       // 1 Date
      timeStr,       // 2 Time
      orderId,       // 3 OrderId
      employeeName,  // 4 EmployeeName
      subtotal,      // 5 Subtotal
      tax,           // 6 Tax
      tip,           // 7 Tip
      total,         // 8 Total
      paymentType,   // 9 PaymentType
      result,        // 10 Result
      isOffline,     // 11 IsOffline
      items          // 12 Items
    ]);
  }

  if (output.length === 0) {
    // всё равно обновим lastProcessed, чтобы не зацикливаться на пустых json
    props.setProperty(LAST_FORMATED_PAYMENT_ROW, String(lastRow));
    return;
  }

  // Пишем в конец FormattedPayments
  const outStartRow = outSheet.getLastRow() + 1;
  outSheet
    .getRange(outStartRow, 1, output.length, output[0].length)
    .setValues(output);

  // Сохраняем, до какой строки дошли в RawPayments
  props.setProperty(LAST_FORMATED_PAYMENT_ROW, String(lastRow));
}

// 3) Link PaymentId to Orders in FormattedPayments for new rows
function linkPaymentIdsToFormattedOrders() {
  const USED_LABEL = 'USED';

  const ss = SpreadsheetApp.getActive();
  const rawSheet = ss.getSheetByName(RAWPAYMENTS_SHEET);
  const formattedSheet = ss.getSheetByName(FORMATTEDPAYMENTS_SHEET);

  if (!rawSheet) throw new Error('Sheet "' + RAWPAYMENTS_SHEET + '" not found');
  if (!formattedSheet) throw new Error('Sheet "' + FORMATTEDPAYMENTS_SHEET + '" not found');

  // ---------- 1. Читаем RawPayments ----------
  const lastRawRow = rawSheet.getLastRow();
  if (lastRawRow < 2) return; // нет данных

  // Берём B:E (PaymentId, JSON, .., Marker)
  const rawRange = rawSheet.getRange(2, 2, lastRawRow - 1, 4); // cols B–E
  const rawValues = rawRange.getValues();  // [ [B,C,D,E], ... ]

  // Карта: orderId -> массив { paymentId, rawRowIndex, used }
  const orderMap = {};

  for (let i = 0; i < rawValues.length; i++) {
    const paymentId = String(rawValues[i][0] || '').trim(); // col B
    const jsonText  = rawValues[i][1];                      // col C
    const marker    = String(rawValues[i][3] || '').trim(); // col E

    if (!paymentId || !jsonText) continue;

    let orderId = null;
    try {
      const obj = JSON.parse(String(jsonText));
      if (obj && obj.order && obj.order.id) {
        orderId = String(obj.order.id);
      }
    } catch (e) {
      // битый JSON – пропускаем
      continue;
    }

    if (!orderId) continue;

    if (!orderMap[orderId]) orderMap[orderId] = [];
    orderMap[orderId].push({
      paymentId,
      rawRowIndex: i,              // индекс в rawValues (начинается с 0)
      used: marker === USED_LABEL  // уже был помечен
    });
  }

  // ---------- 2. Читаем FormattedPayments ----------
  const lastFmtRow = formattedSheet.getLastRow();
  if (lastFmtRow < 2) return;

  // Берём C–M (OrderId и колонка M внутри этого диапазона)
  const fmtRange = formattedSheet.getRange(2, 3, lastFmtRow - 1, 11); // C–M
  const fmtValues = fmtRange.getValues(); // [ [C, D, ..., M], ... ]

  // Индексы внутри fmtValues:
  const IDX_ORDER_ID = 0;       // C
  const IDX_PAYMENT_ID_COL_M = 10; // M (C=0, D=1, ..., M=10)

  // ---------- 3. Матчим OrderId → PaymentId ----------
  for (let i = 0; i < fmtValues.length; i++) {
    const orderId = String(fmtValues[i][IDX_ORDER_ID] || '').trim();
    if (!orderId) continue;

    // Если PaymentId в M уже стоит – пропускаем (чтобы не перезаписывать вручную поставленное)
    const existingPaymentId = String(fmtValues[i][IDX_PAYMENT_ID_COL_M] || '').trim();
    if (existingPaymentId) continue;

    const candidates = orderMap[orderId];
    if (!candidates || candidates.length === 0) continue;

    // Находим первый неиспользованный PaymentId для этого OrderId
    const candidate = candidates.find(c => !c.used);
    if (!candidate) continue;

    // Записываем PaymentId в M на FormattedPayments
    fmtValues[i][IDX_PAYMENT_ID_COL_M] = candidate.paymentId;
    candidate.used = true; // помечаем как использованный
  }

  // ---------- 4. Записываем изменения обратно ----------

  // Обновляем FormattedPayments (C–M)
  fmtRange.setValues(fmtValues);

  // Обновляем метки USED в колонке E на RawPayments
  // Сначала вытаскиваем текущие значения E, чтобы не терять другие пометки
  const markerRange = rawSheet.getRange(2, 5, lastRawRow - 1, 1); // col E
  const markerValues = markerRange.getValues(); // [ [E], [E], ... ]

  // Проставляем USED там, где candidate.used = true
  Object.keys(orderMap).forEach(orderId => {
    orderMap[orderId].forEach(entry => {
      if (entry.used) {
        const idx = entry.rawRowIndex;
        markerValues[idx][0] = USED_LABEL;
      }
    });
  });

  markerRange.setValues(markerValues);
}

// 4) Tag Primary SUCCESS In Formated Payments for new rows
function markPrimarySuccessInFormattedPayments() {
  const ss = SpreadsheetApp.getActive();
  const rawSh = ss.getSheetByName(RAWPAYMENTS_SHEET);
  const fmtSh = ss.getSheetByName(FORMATTEDPAYMENTS_SHEET);

  if (!rawSh) throw new Error('Sheet "' + RAWPAYMENTS_SHEET + '" not found');
  if (!fmtSh) throw new Error('Sheet "' + FORMATTEDPAYMENTS_SHEET + '" not found');

  // ----- 1. Собираем PRIMARY_SUCCESS по RawPayments -----
  const rawLastRow = rawSh.getLastRow();
  if (rawLastRow < 2) {
    Logger.log('[markPrimary] "' + RAWPAYMENTS_SHEET + '" is empty');
    return;
  }

  // B: PaymentId, C: JSON
  const rawRange  = rawSh.getRange(2, 2, rawLastRow - 1, 2);
  const rawValues = rawRange.getValues();

  /** orderId -> bestCandidate */
  const bestByOrderId = {};

  rawValues.forEach(row => {
    const paymentId = String(row[0] || '').trim();
    const jsonText  = row[1];
    if (!paymentId || !jsonText) return;

    let data;
    try {
      data = JSON.parse(jsonText);
    } catch (e) {
      Logger.log('[markPrimary] JSON parse error for payment ' + paymentId + ': ' + e);
      return;
    }

    const order = data.order;
    if (!order || !order.id) return;
    const orderId    = order.id;
    const result     = data.result;
    const amount     = Number(data.amount || 0);
    const orderTotal = Number(order.total || 0);
    const created    = Number(data.createdTime || 0);

    // интересуют только SUCCESS
    if (result !== 'SUCCESS') return;

    const isExact = (amount === orderTotal);
    const candidate = { paymentId, amount, orderTotal, created, isExact };

    const current = bestByOrderId[orderId];
    if (!current) {
      bestByOrderId[orderId] = candidate;
      return;
    }

    // Приоритет: exact > notExact, затем больший amount, затем более поздний created
    if (isExact && !current.isExact) {
      bestByOrderId[orderId] = candidate;
      return;
    }
    if (isExact === current.isExact) {
      if (amount > current.amount ||
         (amount === current.amount && created > current.created)) {
        bestByOrderId[orderId] = candidate;
      }
    }
  });

  // Оставляем только те orders, у которых есть точное совпадение суммы
  Object.keys(bestByOrderId).forEach(orderId => {
    if (!bestByOrderId[orderId].isExact) {
      delete bestByOrderId[orderId];
    }
  });

  Logger.log('[markPrimary] primary map size: ' + Object.keys(bestByOrderId).length);

  // ----- 2. Проставляем IsPrimarySuccess ТОЛЬКО для новых строк -----
  const lastRow = fmtSh.getLastRow();
  if (lastRow < 2) {
    Logger.log('[markPrimary] "' + FORMATTEDPAYMENTS_SHEET + '" is empty');
    return;
  }

  const ORDER_ID_COL   = 3;   // C
  const STATUS_COL     = 10;  // J (SUCCESS / FAIL)
  const PAYMENT_ID_COL = 13;  // M
  const PRIMARY_COL    = PAYMENT_ID_COL + 1; // N

  // Заголовок
  const headerCell = fmtSh.getRange(1, PRIMARY_COL);
  if (headerCell.getValue() !== 'IsPrimarySuccess') {
    headerCell.setValue('IsPrimarySuccess');
  }

  const numRows = lastRow - 1;

  // Читаем нужные колонки отдельно
  const orderIdsRange   = fmtSh.getRange(2, ORDER_ID_COL,   numRows, 1);
  const statusesRange   = fmtSh.getRange(2, STATUS_COL,     numRows, 1);
  const paymentIdsRange = fmtSh.getRange(2, PAYMENT_ID_COL, numRows, 1);
  const primaryRange    = fmtSh.getRange(2, PRIMARY_COL,    numRows, 1);

  const orderIds   = orderIdsRange.getValues();
  const statuses   = statusesRange.getValues();
  const paymentIds = paymentIdsRange.getValues();
  const primaries  = primaryRange.getValues(); // то, что уже стоит в N

  for (let i = 0; i < numRows; i++) {
    const orderId   = String(orderIds[i][0]   || '').trim();
    const status    = String(statuses[i][0]   || '').trim();
    const paymentId = String(paymentIds[i][0] || '').trim();
    const currentPrimary = String(primaries[i][0] || '').trim();

    // Если уже есть TRUE/FALSE/что-то — не трогаем эту строку
    if (currentPrimary) continue;

    // Для не-успешных и без paymentId — оставляем пусто
    if (!orderId || status !== 'SUCCESS' || !paymentId) {
      primaries[i][0] = '';
      continue;
    }

    const best = bestByOrderId[orderId];
    if (!best) {
      primaries[i][0] = '';
      continue;
    }

    primaries[i][0] = (paymentId === best.paymentId);
  }

  primaryRange.setValues(primaries);
  Logger.log('[markPrimary] done writing IsPrimarySuccess (only for new/empty rows)');
}

// 5) Pull raw JSONs of Primary SUCCESSED Orders with lineItems using Primary-orderId for a given date
function updateRawOrdersFromFormattedForDateFull(targetDateStr) {
  const MAX_LOOPS = 50;        // защита от бесконечных циклов
  const BATCH_SIZE = 15;       // тот же размер батча
  let totalFetched = 0;

  for (let i = 0; i < MAX_LOOPS; i++) {
    const fetched = updateRawOrdersFromFormattedForDateBatch(targetDateStr, BATCH_SIZE);

    // если в этом батче ничего нового не подтянули — значит, всё уже есть
    if (fetched === 0) {
      Logger.log('[updateRawOrdersFromFormattedForDateFull] no more orders to fetch for ' + targetDateStr);
      break;
    }

    totalFetched += fetched;

    // небольшая пауза между батчами — чуть бережнее к API
    Utilities.sleep(500); // 0.5 сек между батчами
  }

  Logger.log('[updateRawOrdersFromFormattedForDateFull] total fetched for ' +
             targetDateStr + ': ' + totalFetched);
}
function updateRawOrdersFromFormattedForDateBatch(targetDateStr, maxPerRun) {
  const MAX_ORDERS_PER_RUN  = maxPerRun || 15;
  const ss = SpreadsheetApp.getActive();

  const paymentsSh = ss.getSheetByName(FORMATTEDPAYMENTS_SHEET);
  if (!paymentsSh) throw new Error('Sheet "' + FORMATTEDPAYMENTS_SHEET + '" not found');

  let rawOrdersSh = ss.getSheetByName(RAWORDERS_SHEET);
  if (!rawOrdersSh) {
    rawOrdersSh = ss.insertSheet(RAWORDERS_SHEET);
  }

  // Обновляем/ставим заголовки A:D
  rawOrdersSh.getRange(1, 1, 1, 4).setValues([['OrderId', 'JSON', 'Date', 'Time']]);

  const lastRow = paymentsSh.getLastRow();
  if (lastRow < 2) {
    Logger.log('[updateRawOrdersFromFormattedForDateBatch] no data rows');
    return 0;
  }

  const numRows = lastRow - 1;
  // Читаем A..J: Date, Time, OrderId, ..., Status(J)
  const dataRange = paymentsSh.getRange(2, 1, numRows, 10);
  const data = dataRange.getValues();

  // Set уже существующих OrderId в RawOrders
  const existingSet = {};
  const rawLastRow = rawOrdersSh.getLastRow();
  if (rawLastRow >= 2) {
    const existingRange = rawOrdersSh.getRange(2, 1, rawLastRow - 1, 1); // только кол. A
    existingRange.getValues().forEach(r => {
      const id = String(r[0] || '').trim();
      if (id) existingSet[id] = true;
    });
  }

  const tz = ss.getSpreadsheetTimeZone();
  const rowsToAppend = [];
  let fetchedCount = 0;

  for (let i = 0; i < data.length; i++) {
    if (fetchedCount >= MAX_ORDERS_PER_RUN) break;

    const row      = data[i];
    const dateVal  = row[0];                    // A (Date)
    const timeVal  = row[1];                    // B (Time)
    const orderId  = String(row[2] || '').trim(); // C
    const status   = String(row[9] || '').trim(); // J

    if (!orderId) continue;
    if (status !== 'SUCCESS') continue;

    // сравниваем дату строки с targetDateStr
    const rowDateStr = (dateVal instanceof Date)
      ? Utilities.formatDate(dateVal, tz, 'yyyy-MM-dd')
      : String(dateVal).trim();

    if (rowDateStr !== targetDateStr) continue;

    // уже есть в RawOrders?
    if (existingSet[orderId]) continue;

    const jsonText = fetchOrderJsonWithModifiers(orderId);
    if (!jsonText) {
      Logger.log('[updateRawOrdersFromFormattedForDateBatch] failed to fetch order ' + orderId);
      continue;
    }

    // теперь сохраняем и дату, и время
    rowsToAppend.push([orderId, jsonText, dateVal, timeVal]);
    existingSet[orderId] = true;
    fetchedCount++;

    // небольшая пауза после УСПЕШНОГО запроса
    Utilities.sleep(150); // 0.15 сек
  }

  if (rowsToAppend.length) {
    const startRow = rawOrdersSh.getLastRow() + 1;
    rawOrdersSh
      .getRange(startRow, 1, rowsToAppend.length, 4) // теперь 4 колонки: A..D
      .setValues(rowsToAppend);
  }

  Logger.log('[updateRawOrdersFromFormattedForDateBatch] fetched ' + fetchedCount +
             ' orders for ' + targetDateStr + ' in this batch');

  return fetchedCount;
}

// 6) Process RawSuccessedOrders to FormattedOrderItemsModifiers with Items and Modifiers by batches (process 15 orders takes approx. 1 min.)
function buildFormattedOrderItemsModifiersFromRaw() {
  const MAX_ORDERS_ITEMS_PER_RUN = 1000; // сколько заказов обрабатываем за один запуск
  const itemMap     = loadItemMapFromSheet();
  const modifierMap = loadModifierMapFromSheet();
  const ss = SpreadsheetApp.getActive();
  const sh = ss.getSheetByName(RAWORDERS_SHEET); // 'RawSuccessedOrders'
  if (!sh) throw new Error('Sheet "' + RAWORDERS_SHEET + '" not found');

  const lastRow = sh.getLastRow();
  if (lastRow < 2) {
    Logger.log('[buildFormattedOrderItemsModifiersFromRaw] no data rows');
    return;
  }

  const props = PropertiesService.getScriptProperties();

  // с какой строки RawSuccessedOrders начинаем (1-based)
  let lastProcessedRow = parseInt(
    props.getProperty(LAST_PROCESSED_RAW_ORDERS_ROW_KEY) || '1',
    10
  );
  if (isNaN(lastProcessedRow) || lastProcessedRow < 1) {
    lastProcessedRow = 1;
  }

  // если уже всё обработали
  if (lastProcessedRow >= lastRow) {
    Logger.log(
      '[buildFormattedOrderItemsModifiersFromRaw] all rows processed. lastRow=' +
      lastRow +
      ', lastProcessedRow=' +
      lastProcessedRow
    );
    return;
  }

  // стартуем со следующей строки после lastProcessedRow
  const startRow = lastProcessedRow + 1;
  const endRow   = Math.min(startRow + MAX_ORDERS_ITEMS_PER_RUN - 1, lastRow);
  const numRows  = endRow - startRow + 1;

  Logger.log(
    '[buildFormattedOrderItemsModifiersFromRaw] processing RawSuccessedOrders rows ' +
    startRow + '–' + endRow + ' (count=' + numRows + ')'
  );

  // A: OrderId
  const range  = sh.getRange(startRow, 1, numRows, 1);
  const values = range.getValues();

  let processed = 0;

  values.forEach(row => {
    const orderId = String(row[0] || '').trim();
    if (!orderId) return;

    appendOrderDetailsForOrder(orderId, itemMap, modifierMap);
    processed++;
  });

  // обновляем указатель на последнюю фактически просмотренную строку
  props.setProperty(LAST_PROCESSED_RAW_ORDERS_ROW_KEY, String(endRow));

  Logger.log(
    '[buildFormattedOrderItemsModifiersFromRaw] done this run. ' +
    'processed=' + processed +
    ', new LAST_PROCESSED_RAW_ORDERS_ROW=' + endRow
  );
}

// 7) Populate "Margin" on FormattedOrderItemsModifiers
function populateFormattedOrderItemsModifiersProfit() {
  const ss = SpreadsheetApp.getActive();
  const sh = ss.getSheetByName('FormattedOrderItemsModifiers');
  if (!sh) throw new Error('Sheet "FormattedOrderItemsModifiers" not found');

  const lastRow = sh.getLastRow();
  if (lastRow < 2) {
    Logger.log('[populateProfit] no data rows');
    return;
  }

  // Колонка K = 11
  const PROFIT_COL = 11;

  // Читаем весь K столбец
  const profitRange = sh.getRange(2, PROFIT_COL, lastRow - 1, 1);
  const profitValues = profitRange.getValues();

  // Заполняем только пустые клетки формулой
  for (let i = 0; i < profitValues.length; i++) {
    const current = String(profitValues[i][0] || '').trim();
    if (current) continue; // уже заполнено

    const rowNum = i + 2; // реальный номер строки
    const formula = `=G${rowNum}-H${rowNum}`;

    profitValues[i][0] = formula;
  }

  // Записываем обратно
  profitRange.setValues(profitValues);

  Logger.log('[populateProfit] done filling empty K cells');
}


/** 
* HELPERS
*/

// parse raw JSONs from RawPayments (for processRawPayments)
function safeParseJsonFromCell(str) {
  if (!str) return null;

  // Transforming to string - trimming backspaces form the sides
  str = String(str).trim();

  // Clear BOM, if it has it
  str = str.replace(/^\uFEFF/, '');

  // If row wrapped in single/double quotes - removing them
  if (
    (str[0] === "'" && str[str.length - 1] === "'") ||
    (str[0] === '"' && str[str.length - 1] === '"')
  ) {
    str = str.slice(1, -1);
  }

  // Removing hidden symbols
  str = str.replace(/[\u0000-\u001F]+/g, '');

  try {
    return JSON.parse(str);
  } catch (e) {
    Logger.log('JSON parse error: ' + e);
    Logger.log('Raw string: ' + str);
    return null;
  }
}

// fetch single order JSON with items + modifiers (for updateRawOrdersFromFormattedForDate)
function fetchOrderJsonWithModifiers(orderId) {
  if (!orderId) return '';

  const props     = PropertiesService.getScriptProperties();
  const merchantId = props.getProperty('CLOVER_MERCHANT_ID');
  const token      = props.getProperty('CLOVER_ACCESS_TOKEN');

  if (!merchantId || !token) {
    throw new Error('Need CLOVER_MERCHANT_ID and CLOVER_ACCESS_TOKEN in Script Properties');
  }

  const url =
    'https://api.clover.com/v3/merchants/' +
    merchantId +
    '/orders/' +
    orderId +
    '?expand=lineItems,lineItems.modifications,lineItems.item';

  const res = UrlFetchApp.fetch(url, {
    method: 'get',
    headers: { Authorization: 'Bearer ' + token },
    muteHttpExceptions: true
  });

  const status = res.getResponseCode();
  const text   = res.getContentText();

  if (status !== 200) {
    Logger.log('[fetchOrderJsonWithModifiers] ' + orderId +
               ' failed: ' + status + ' ' + text.slice(0, 200));
    return '';
  }

  return text;
}

// Process One Row Order JSON to Formated Orders with Items and Modifiers using passed Maps (for buildFormattedOrderItemsModifiersFromRaw)
function appendOrderDetailsForOrder(orderId, itemMap, modifierMap) {
  if (!orderId) {
    Logger.log('[appendOrderDetailsForOrder] empty orderId');
    return;
  }

  const ss = SpreadsheetApp.getActive();

  // ---------- RAW ORDERS: используем кэш ----------
  const rawSh = ss.getSheetByName(RAWORDERS_SHEET);
  if (!rawSh) throw new Error('Sheet "' + RAWORDERS_SHEET + '" not found');

  if (!RAW_ORDERS_CACHE) {
    RAW_ORDERS_CACHE = {};
    const rawLastRow = rawSh.getLastRow();
    if (rawLastRow >= 2) {
      const rawRange  = rawSh.getRange(2, 1, rawLastRow - 1, 2); // A:B
      const rawValues = rawRange.getValues();

      rawValues.forEach(row => {
        const id  = String(row[0] || '').trim();
        const txt = String(row[1] || '');
        if (id) RAW_ORDERS_CACHE[id] = txt;
      });
      Logger.log('[appendOrderDetailsForOrder] RAW_ORDERS_CACHE built: ' +
                 Object.keys(RAW_ORDERS_CACHE).length + ' orders');
    }
  }

  const jsonText = RAW_ORDERS_CACHE[orderId] || null;
  if (!jsonText) {
    Logger.log('[appendOrderDetailsForOrder] orderId ' + orderId + ' not found in RawOrders');
    return;
  }

  let order;
  try {
    order = JSON.parse(jsonText);
  } catch (e) {
    Logger.log('[appendOrderDetailsForOrder] JSON parse error for ' + orderId + ': ' + e);
    return;
  }

  // ---------- FormattedOrderItemsModifiers: одна инициализация кэша ----------
  let fmtSh = ss.getSheetByName(FORMATTEDORDERITEMS_SHEET);
  if (!fmtSh) {
    fmtSh = ss.insertSheet(FORMATTEDORDERITEMS_SHEET);
  }

  if (fmtSh.getLastRow() === 0) {
    fmtSh.getRange(1, 1, 1, 10).setValues([[
      'Date', 'Time', 'OrderId', 'Type',
      'Item/Modifier Id', 'Item/Modifier Name',
      'Price', 'Cost',
      'BaseItemId', 'BaseItemName'
    ]]);
  }

  if (!FMT_ORDER_IDS_CACHE) {
    FMT_ORDER_IDS_CACHE = new Set();
    const fmtLastRow = fmtSh.getLastRow();
    if (fmtLastRow >= 2) {
      const existingRange = fmtSh.getRange(2, 3, fmtLastRow - 1, 1); // col C
      const existingVals  = existingRange.getValues();
      existingVals.forEach(r => {
        const v = String(r[0] || '').trim();
        if (v) FMT_ORDER_IDS_CACHE.add(v);
      });
      Logger.log('[appendOrderDetailsForOrder] FMT_ORDER_IDS_CACHE built: ' +
                 FMT_ORDER_IDS_CACHE.size + ' orderIds');
    }
  }

  if (FMT_ORDER_IDS_CACHE.has(orderId)) {
    Logger.log('[appendOrderDetailsForOrder] orderId ' + orderId +
               ' already present in FormattedOrderItemsModifiers, skip');
    return;
  }

  // ---------- Подготовка дат / времени ----------
  const tsMs    = order.clientCreatedTime || order.createdTime;
  const dt      = tsMs ? new Date(tsMs) : new Date();
  const dateVal = new Date(dt.getFullYear(), dt.getMonth(), dt.getDate());
  const timeVal = new Date(1899, 11, 30,
                           dt.getHours(), dt.getMinutes(), dt.getSeconds());

  // ---------- Линии заказа ----------
  let lineItems = [];
  if (order.lineItems) {
    if (Array.isArray(order.lineItems)) lineItems = order.lineItems;
    else if (order.lineItems.elements && Array.isArray(order.lineItems.elements))
      lineItems = order.lineItems.elements;
  }

  const rowsToAppend = [];

  lineItems.forEach(li => {
    if (!li) return;

    const itemObj = li.item || null;
    const itemId  = itemObj && itemObj.id ? String(itemObj.id) : '';
    const mapItem = itemId ? itemMap[itemId] : null;

    let itemName  = mapItem ? mapItem.name :
                    (li.name || (itemObj && itemObj.name) || '');
    let itemPrice = (typeof li.price === 'number')
                      ? li.price / 100
                      : (mapItem && typeof mapItem.price === 'number'
                          ? mapItem.price : '');
    let itemCost  = mapItem && typeof mapItem.cost === 'number'
                      ? mapItem.cost
                      : (itemObj && typeof itemObj.cost === 'number'
                          ? itemObj.cost / 100 : '');

    if (!itemName) return;

    // строка для ITEM
    rowsToAppend.push([
      dateVal,
      timeVal,
      orderId,
      'ITEM',
      itemId,
      itemName,
      itemPrice,
      itemCost,
      itemId,    // BaseItemId
      itemName   // BaseItemName
    ]);

    // модификаторы / теги
    let mods = [];
    if (li.modifications) {
      if (Array.isArray(li.modifications)) mods = li.modifications;
      else if (li.modifications.elements && Array.isArray(li.modifications.elements))
        mods = li.modifications.elements;
    }

    mods.forEach(m => {
      if (!m) return;

      const modObj     = m.modifier || null;
      const modifierId = modObj && modObj.id ? String(modObj.id) : '';
      if (!modifierId) return;

      const mapMod = modifierMap[modifierId];
      if (!mapMod) return; // уже отфильтрованные "Nothing" и т.п.

      const modType  = mapMod.type || 'MODIFIER';
      const modName  = mapMod.name || m.name || '';
      const modPrice = (typeof m.amount === 'number')
                        ? m.amount / 100
                        : (typeof mapMod.price === 'number'
                            ? mapMod.price : '');
      const modCost  = (typeof mapMod.cost === 'number') ? mapMod.cost : '';

      rowsToAppend.push([
        dateVal,
        timeVal,
        orderId,
        modType,
        modifierId,
        modName,
        modPrice,
        modCost,
        itemId,    // BaseItemId
        itemName   // BaseItemName
      ]);
    });
  });

  if (!rowsToAppend.length) {
    Logger.log('[appendOrderDetailsForOrder] no lineItems for orderId ' + orderId);
    return;
  }

  const startRow = fmtSh.getLastRow() + 1;
  fmtSh.getRange(startRow, 1, rowsToAppend.length, 10).setValues(rowsToAppend);

  // добавляем orderId в кэш, чтобы в этом же запуске больше не писать дубль
  FMT_ORDER_IDS_CACHE.add(orderId);

  Logger.log('[appendOrderDetailsForOrder] written rows: ' +
             rowsToAppend.length + ' for orderId ' + orderId);
}