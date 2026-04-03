// Mappings.gs

/**
 * CREATING MAPPINGS FROM SHEETS
 */

// create MappingItems Object
function loadItemMapFromSheet() {
  const ss = SpreadsheetApp.getActive();
  const sh = ss.getSheetByName('MappingItems');
  if (!sh) throw new Error('Sheet "MappingItems" not found');

  const lastRow = sh.getLastRow();
  if (lastRow < 2) {
    Logger.log('[loadItemMapFromSheet] no data rows');
    return {};
  }

  // A:E  (ID, Name, Price, Cost, Type)
  const range  = sh.getRange(2, 1, lastRow - 1, 5);
  const values = range.getValues();

  const map = {};

  values.forEach(row => {
    const id    = String(row[0] || '').trim();   // A
    const name  = String(row[1] || '').trim();   // B
    const price = row[2];                        // C
    const cost  = row[3];                        // D
    const type  = String(row[4] || '').trim() || 'ITEM'; // E

    if (!id) return; // empty row

    map[id] = {
      name,
      price: (typeof price === 'number') ? price : '',
      cost:  (typeof cost  === 'number') ? cost  : '',
      type   // like "ITEM"
    };
  });

  Logger.log('[loadItemMapFromSheet] items loaded: ' + Object.keys(map).length);
  return map;
}

// create MappingModifiers Object
function loadModifierMapFromSheet() {
  const ss = SpreadsheetApp.getActive();
  const sh = ss.getSheetByName('MappingModifiers');
  if (!sh) throw new Error('Sheet "MappingModifiers" not found');

  const lastRow = sh.getLastRow();
  if (lastRow < 2) {
    Logger.log('[loadModifierMapFromSheet] no data rows');
    return {};
  }

  // A:H
  const range  = sh.getRange(2, 1, lastRow - 1, 8);
  const values = range.getValues();

  const mapByModifierId = {};

  values.forEach(row => {
    const groupName     = String(row[0] || '').trim();  // A
    const groupId       = String(row[1] || '').trim();  // B
    const modifierName  = String(row[2] || '').trim();  // C
    const modifierId    = String(row[3] || '').trim();  // D
    const modifierPrice = row[4];                       // E
    const modifierCost  = row[5];                       // F
    // G = Sample Order Id (row[6]) — don't use in 
    let   type          = String(row[7] || '').trim();  // H (Type)

    // skipping empty rows and "Rien / Nothing"
    if (!modifierId) return;
    if (modifierName === 'Rien / Nothing') return;

    // if in sheet Type is empty — using default logic
    if (!type) {
      type = (groupName === 'In House / ToGo') ? 'TAG' : 'MODIFIER';
    }

    mapByModifierId[modifierId] = {
      groupName,
      groupId,
      name: modifierName,
      price: (typeof modifierPrice === 'number') ? modifierPrice : '',
      cost:  (typeof modifierCost  === 'number') ? modifierCost  : '',
      type,           // "MODIFIER" or "TAG"
      kind: type      // for older version of code where expecting "kind"
    };
  });

  Logger.log(
    '[loadModifierMapFromSheet] modifiers loaded: ' +
    Object.keys(mapByModifierId).length
  );
  return mapByModifierId;
}

// create MappingEmployee Object
function loadEmployeeMapFromSheet() {
  const SHEET_NAME = 'MappingEmployees';

  const ss = SpreadsheetApp.getActive();
  const sh = ss.getSheetByName(SHEET_NAME);
  if (!sh) throw new Error('Sheet "' + SHEET_NAME + '" not found');

  const lastRow = sh.getLastRow();
  if (lastRow < 2) {
    Logger.log('[loadEmployeeMapFromSheet] no data rows');
    return {};
  }

  // A:B  (Employee Id, Employee Name)
  const range  = sh.getRange(2, 1, lastRow - 1, 2);
  const values = range.getValues();

  const map = {};

  values.forEach(row => {
    const id   = String(row[0] || '').trim(); // col A
    const name = String(row[1] || '').trim(); // col B
    if (!id) return;
    map[id] = name;
  });

  Logger.log('[loadEmployeeMapFromSheet] employees loaded: ' +
             Object.keys(map).length);
  return map;
}


/**
 * SYNCING MAPPINGS WITH CLOVER
 */

// syncing Items with Categories
function syncItemsMap() {
  const ss = SpreadsheetApp.getActive();
  const sh = ss.getSheetByName(ITEMSMAP_SHEET);
  if (!sh) throw new Error('Sheet "' + ITEMSMAP_SHEET + '" not found');

  // 0) Header, if sheet is empty
  const lastRow = sh.getLastRow();
  if (lastRow === 0) {
    const header = [['id', 'name', 'price', 'cost', 'type', 'category', 'categoryIds']];
    sh.getRange(1, 1, 1, header[0].length).setValues(header);
  }

  const lastRowAfterHeader = sh.getLastRow();
  const hasDataRows = lastRowAfterHeader > 1;

  // 1) Existing mapping (for "historical" items removed from Clover)
  const existingValues = hasDataRows
    ? sh.getRange(2, 1, lastRowAfterHeader - 1, 7).getValues() // A:G
    : [];

  const oldById = new Map(); // id -> row [id,name,price,cost,type,category,categoryIds]
  existingValues.forEach(row => {
    const id = String(row[0] || '').trim();
    if (id) oldById.set(id, row);
  });

  // 2) Fetch all items with categories from Clover
  const items = cloverFetchAllItemsWithCategories();

  // helper: category names
  function getCategoryNamesForItem(it) {
    const names = [];

    if (it.categories && Array.isArray(it.categories.elements)) {
      it.categories.elements.forEach(c => {
        if (c && c.name) {
          names.push(String(c.name));
        }
      });
    }

    if (it.category && it.category.name) {
      names.push(String(it.category.name));
    }

    const unique = Array.from(new Set(names));
    return unique.join(', ');
  }

  // helper: category IDs
  function getCategoryIdsForItem(it) {
    const ids = [];

    if (it.categories && Array.isArray(it.categories.elements)) {
      it.categories.elements.forEach(c => {
        if (c && c.id) {
          ids.push(String(c.id));
        }
      });
    }

    if (it.category && it.category.id) {
      ids.push(String(it.category.id));
    }

    const unique = Array.from(new Set(ids));
    return unique.join(', ');
  }

  // 3) Build fresh rows from Clover items
  const rowsFromClover = [];
  const liveIds = new Set();

  items.forEach(it => {
    const id = it.id ? String(it.id).trim() : '';
    if (!id) return;

    const rawPrice = it.price;
    const rawCost  = (it.cost != null ? it.cost : it.defaultCost);

    const price = (rawPrice != null ? rawPrice / 100 : 0);
    const cost  = (rawCost  != null ? rawCost  / 100 : 0);

    const categoryNames = getCategoryNamesForItem(it);
    const categoryIds   = getCategoryIdsForItem(it);

    const row = [
      id,                 // A id
      it.name || '',      // B name
      price,              // C price
      cost,               // D cost
      'ITEM',             // E type
      categoryNames,      // F category (names)
      categoryIds         // G categoryIds (ids)
    ];

    rowsFromClover.push(row);
    liveIds.add(id);
  });

  // 4) Append old items that no longer exist in Clover (historical)
  existingValues.forEach(oldRow => {
    const id = String(oldRow[0] || '').trim();
    if (id && !liveIds.has(id)) {
      // ensure row has 7 columns
      const normOldRow = oldRow.slice();
      while (normOldRow.length < 7) normOldRow.push('');
      rowsFromClover.push(normOldRow);
    }
  });

  // 5) Clear old data and write new list
  if (hasDataRows) {
    sh.getRange(2, 1, lastRowAfterHeader - 1, 7).clearContent();
  }

  if (rowsFromClover.length) {
    sh
      .getRange(2, 1, rowsFromClover.length, 7)
      .setValues(rowsFromClover);
  }

  Logger.log(
    '[syncItemsMap] Clover items: ' + items.length +
    ', total rows written (incl. old-only): ' + rowsFromClover.length
  );
}

// syncing MappingModifiers
function syncModifiersMap() {
  const ss = SpreadsheetApp.getActive();
  const sh = ss.getSheetByName(MODIFIERSMAP_SHEET);
  if (!sh) throw new Error('Sheet "' + MODIFIERSMAP_SHEET + '" not found');

  // --- 0. Сохраняем старые Sample Order Id по Modifier Id ---
  const oldLastRow = sh.getLastRow();
  const sampleByModifierId = new Map(); // modifierId -> sampleOrderId

  if (oldLastRow > 1) {
    // читаем существующие данные (A:H)
    const oldValues = sh.getRange(2, 1, oldLastRow - 1, 8).getValues();
    oldValues.forEach(row => {
      const modifierId = String(row[3] || '').trim(); // col D
      const sampleOrderId = row[6] || '';            // col G
      if (modifierId) {
        sampleByModifierId.set(modifierId, sampleOrderId);
      }
    });
  }

  // --- 1. Тянем все modifier groups из Clover ---
  const groups = cloverFetchAll('/modifier_groups');

  const rows = [];

  groups.forEach(group => {
    const groupId = group.id;
    const groupName = group.name || '';

    let modifiers = [];
    try {
      modifiers = cloverFetchAll('/modifier_groups/' + groupId + '/modifiers');
      Utilities.sleep(200); // чтобы не спамить Clover
    } catch (e) {
      Logger.log('Error fetching modifiers for group ' + groupId + ': ' + e);
      return;
    }

    modifiers.forEach(m => {
      const modifierId = m.id ? String(m.id).trim() : '';
      if (!modifierId) return;

      const name = m.name || '';
      const rawPrice = m.price;
      const rawCost  = (m.cost != null ? m.cost : m.defaultCost);

      const price = (rawPrice != null ? rawPrice / 100 : 0);
      const cost  = (rawCost  != null ? rawCost  / 100 : 0);

      const type = (groupId === TAG_GROUP_ID ? 'TAG' : 'MODIFIER');

      // если раньше руками заносился Sample Order Id — сохраняем
      const sampleOrderId = sampleByModifierId.get(modifierId) || '';

      rows.push([
        groupName,      // A Modifier Group Name
        groupId,        // B Modifier Group Id
        name,           // C Modifier Name
        modifierId,     // D Modifier Id
        price,          // E Modifier Price
        cost,           // F Modifier Cost
        sampleOrderId,  // G Sample Order Id (сохраняем старое при наличии)
        type            // H Type
      ]);
    });
  });

  // --- 2. Полностью перезаписываем лист (без дублей) ---
  sh.clearContents();

  const header = [[
    'Modifier Group Name',  // A
    'Modifier Group Id',    // B
    'Modifier Name',        // C
    'Modifier Id',          // D
    'Modifier Price',       // E
    'Modifier Cost',        // F
    'Sample Order Id',      // G
    'Type'                  // H
  ]];

  sh.getRange(1, 1, 1, header[0].length).setValues(header);

  if (rows.length) {
    sh.getRange(2, 1, rows.length, header[0].length).setValues(rows);
  }

  Logger.log('[syncModifiersMap] written ' + rows.length + ' modifiers (no duplicates).');
}

// syncing MappingEmployee
function syncEmployeesMap() {
  const ss = SpreadsheetApp.getActive();
  const sh = ss.getSheetByName(EMPLOYEESMAP_SHEET);
  if (!sh) throw new Error('Sheet "' + EMPLOYEESMAP_SHEET + '" not found');

  // --- 1. Ensure header exists (employeeId, name) ---
  const header = [['employeeId', 'name']];
  const lastRow = sh.getLastRow();

  if (lastRow === 0) {
    // Sheet is completely empty → write header
    sh.getRange(1, 1, 1, header[0].length).setValues(header);
  } else {
    // Optional: if A1/B1 are empty, also fix header
    const firstRow = sh.getRange(1, 1, 1, header[0].length).getValues()[0];
    if (!firstRow[0] || !firstRow[1]) {
      sh.getRange(1, 1, 1, header[0].length).setValues(header);
    }
  }

  // --- 2. Build a Set of existing employee IDs from the sheet ---
  const lastRowAfterHeader = sh.getLastRow();
  const existingIds = new Set();

  if (lastRowAfterHeader > 1) {
    const existingRange = sh.getRange(2, 1, lastRowAfterHeader - 1, 1); // column A (ids)
    const existingValues = existingRange.getValues();

    existingValues.forEach(row => {
      const id = String(row[0] || '').trim();
      if (id) existingIds.add(id);
    });
  }

  // --- 3. Fetch current employees from Clover ---
  const employees = cloverFetchAll('/employees');

  // --- 4. Collect only NEW employees (by id) ---
  const newRows = [];
  employees.forEach(e => {
    const id = (e.id ? String(e.id).trim() : '');
    if (!id) return;

    if (!existingIds.has(id)) {
      newRows.push([
        id,
        e.name || ''
      ]);
    }
  });

  // --- 5. Append new employees at the bottom ---
  if (newRows.length) {
    const appendStartRow = lastRowAfterHeader + 1;
    sh.getRange(appendStartRow, 1, newRows.length, header[0].length).setValues(newRows);
  }
}


/**
 * HELPERS
 */

// generic fetch helper (for syncEmployeesMap)
function cloverFetch(path, params) {
  let url = CLOVER_BASE_URL + path;
  if (params && Object.keys(params).length) {
    const qs = Object.keys(params)
      .map(k => k + '=' + encodeURIComponent(params[k]))
      .join('&');
    url += '?' + qs;
  }

  const resp = UrlFetchApp.fetch(url, {
    method: 'get',
    headers: {
      Authorization: 'Bearer ' + CLOVER_TOKEN
    },
    muteHttpExceptions: true,
  });

  const code = resp.getResponseCode();
  if (code !== 200) {
    throw new Error('Clover error ' + code + ': ' + resp.getContentText());
  }
  return JSON.parse(resp.getContentText());
}

// pagination helper: limit/offset style (for syncEmployeesMap)
function cloverFetchAll(path) {
  const all = [];
  let offset = 0;
  const limit = 100;

  while (true) {
    const json = cloverFetch(path, { limit, offset });
    const elements = json.elements || [];
    if (!elements.length) break;

    all.push.apply(all, elements);
    if (elements.length < limit) break;

    offset += limit;
  }
  return all;
}

// fetching items with categories (for syncItemsMap)
function cloverFetchAllItemsWithCategories() {
  const all = [];
  let offset = 0;
  const limit = 100;

  while (true) {
    const path = '/items?expand=categories&limit=' + limit + '&offset=' + offset;

    const data = cloverFetch(path); // твой низкоуровневый fetch
    if (!data || !Array.isArray(data.elements) || data.elements.length === 0) {
      break;
    }

    all.push.apply(all, data.elements);

    if (!data.hasMore && data.elements.length < limit) {
      break;
    }

    offset += limit;
  }

  return all;
}

// 0) refresh all mappings
function syncAllhMappings() {
  syncItemsMap();
  syncModifiersMap();
  syncEmployeesMap();
}
