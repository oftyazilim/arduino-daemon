import "dotenv/config";
import pool from "./db.js";
import { SerialPort } from "serialport";
import { ReadlineParser } from "@serialport/parser-readline";
import fs from "node:fs";
import path from "node:path";
import axios from "axios";
import winston from "winston";

const {
  PORT_NAME = "/dev/cu.usbmodem1101",
  BAUD_RATE = "9600",
  LOG_PATH = `${process.env.HOME}/arduino-daemon/logs/daemon.log`,
  POST_URL,
  POST_TIMEOUT_MS = "3000",
  ISTASYON_ID = "375",
  EVENT_FUNC_SCHEMA = "public",
  MACHINE_EVENTS_MANUAL_INCREMENT = "false",
  TIME_ZONE = "Europe/Istanbul",
  SHIFT_SLICES = "00:40-08:00, 08:00-18:00, 18:00-00:40",
  SHIFT_BREAK_CODE = "000",
  SHIFT_BREAK_DESCRIPTION = "SHIFT",
  SHIFT_OEE_LOG = "true",
  COUNTER_DEBUG = "true",
  COUNTER_FLUSH_MS = "5000",
  COUNTER_BUFFER_MAX = "3000",
  COUNTER_FIELD_INDEX = "7",
} = process.env;

// --- Logger ---
function tsFormat() {
  return winston.format.timestamp({
    format: () =>
      new Date().toLocaleString('tr-TR', { timeZone: TIME_ZONE }).replace(',', '')
  });
}

try {
  fs.mkdirSync(path.dirname(LOG_PATH), { recursive: true });
} catch (e) {
  // ignore mkdir errors; console transport will still work
}

const logger = winston.createLogger({
  level: 'info',
  transports: [
    new winston.transports.File({ filename: LOG_PATH, maxsize: 5_000_000, maxFiles: 3 }),
    new winston.transports.Console(),
  ],
  format: winston.format.combine(
    tsFormat(),
    winston.format.printf(({ level, message, timestamp }) => `${timestamp} [${level}] ${message}`)
  ),
});

let port,
  parser,
  stopping = false,
  reconnectTimer = null,
  detectedPort = null;
// Arduino portunu otomatik bul
async function findArduinoPort() {
  const { SerialPort } = await import('serialport');
  const ports = await SerialPort.list();
  // Arduino, CH340, USB Serial gibi açıklamalara bak
  const arduinoPort = ports.find(p => {
    const info = `${p.manufacturer || ''} ${p.vendorId || ''} ${p.productId || ''} ${p.path || ''} ${p.friendlyName || ''}`.toLowerCase();
    return info.includes('arduino') || info.includes('ch340') || info.includes('usb seri');
  });
  return arduinoPort ? arduinoPort.path : null;
}
let sayac = 0;
// oftt_works_info güncelleme eşiği: sadece değişiklik olduğunda UPDATE çalıştır
let lastStatus = null; // son gönderilen statu_id
let lastCounter = null; // makineden okunan son sayaç (mutlak değer)
let lastPulse = 0; // pulse (arr[6]) için rising-edge tespiti

// Sayaç artışlarını buffer'da toplayıp topluca yazmak için değişkenler
let counterBuffer = 0;
let bufferTimer = null;
let flushInProgress = false;
const BUFFER_INTERVAL_MS = parseInt(COUNTER_FLUSH_MS, 10) || 500; // ms
const BUFFER_MAX = parseInt(COUNTER_BUFFER_MAX, 10) || 100; // eşiği aşınca anında flush
const COUNTER_INDEX = Number.isFinite(parseInt(COUNTER_FIELD_INDEX, 10)) ? parseInt(COUNTER_FIELD_INDEX, 10) : 7;

async function flushCounterBuffer() {
  if (flushInProgress) return;
  const toWrite = counterBuffer;
  if (toWrite <= 0) return;
  flushInProgress = true;
  const startedAt = Date.now();
  try {
    if (String(COUNTER_DEBUG).toLowerCase() === 'true') {
      logger.info(`[FLUSH] start toWrite=${toWrite} buf=${counterBuffer}`);
    }
    const res = await pool.query(
      "UPDATE oftt_works_info SET counter = counter + $1 WHERE wstation_id = $2",
      [toWrite, WSTATION_ID]
    );
    // Başarı: yazılan kadar buffer'dan düş
    counterBuffer -= toWrite;
    if (counterBuffer < 0) counterBuffer = 0;
    const took = Date.now() - startedAt;
    logger.info(`Sayaç buffer yazıldı: +${toWrite}, kalan_buf=${counterBuffer}, rowCount=${res.rowCount}, ${took}ms`);
    // Debug modda, DB counter değerini de logla
    if (String(COUNTER_DEBUG).toLowerCase() === 'true') {
      try {
        const { rows } = await pool.query(
          'SELECT counter FROM oftt_works_info WHERE wstation_id = $1 LIMIT 1',
          [WSTATION_ID]
        );
        if (rows.length) {
          logger.info(`[FLUSH] db.counter=${rows[0].counter}`);
        }
      } catch (re) {
        logger.warn(`[FLUSH] db.counter okunamadı: ${re.message}`);
      }
    }
  } catch (e) {
    logger.error(`Sayaç buffer yazılamadı: ${e.message} (toWrite=${toWrite})`);
  } finally {
    flushInProgress = false;
  }
}

function startBufferTimer() {
  if (bufferTimer) clearInterval(bufferTimer);
  bufferTimer = setInterval(flushCounterBuffer, BUFFER_INTERVAL_MS);
}
startBufferTimer();
let itemLengthTimer = null; // 60 sn periyodik sorgu timer
let dailyEnsureTimer = null; // gün değişiminde partition hazırlığı
let partitionSchedulerStarted = false; // günlük scheduler tek sefer başlasın
let initialPartitionEnsured = false; // açılışta bir defa ensure çalışsın
let shiftTimers = []; // vardiya başlangıç timer'ları
let shiftSlices = []; // {id,start,end,startMin,endMin}
let shiftWatcherTimer = null; // sürekli vardiya kontrolü
let lastKnownShiftId = null; // son bilinen vardiya
let handlingShiftChange = false; // reentrancy guard

const WSTATION_ID = parseInt(ISTASYON_ID, 10) || 378;
let eventFuncSchema = EVENT_FUNC_SCHEMA; // runtime'da otomatik güncellenecek
let ensureFuncSchema = EVENT_FUNC_SCHEMA; // ensure_machine_events_month_part için şema
let insertEventAvailable = false; // insert_machine_event var mı?
let insertEventWarned = false; // tekrar tekrar loglamamak için

async function resolveInsertEventFuncSchema() {
  try {
    const { rows } = await pool.query(
      `SELECT n.nspname AS schema, p.proname AS name, pg_get_function_identity_arguments(p.oid) AS args
       FROM pg_proc p
       JOIN pg_namespace n ON n.oid = p.pronamespace
       WHERE p.proname = 'insert_machine_event'
       ORDER BY n.nspname`
    );
    if (rows.length === 1) {
      eventFuncSchema = rows[0].schema;
      insertEventAvailable = true;
      logger.info(`insert_machine_event bulundu: ${eventFuncSchema}.${rows[0].name}(${rows[0].args})`);
      // logger.info(`insert_machine_event bulundu: ${eventFuncSchema}.${rows[0].name}(${rows[0].args})`);
    } else if (rows.length > 1) {
      // ENV öncelikli; yoksa ilk satırı seç
      const envMatch = rows.find(r => r.schema === EVENT_FUNC_SCHEMA);
      if (envMatch) {
        eventFuncSchema = envMatch.schema;
      } else {
        eventFuncSchema = rows[0].schema;
      }
      insertEventAvailable = true;
      logger.info(`insert_machine_event birden fazla şemada; kullanılacak: ${eventFuncSchema}`);
    } else {
      insertEventAvailable = false;
      logger.error('insert_machine_event fonksiyonu bulunamadı (pg_proc). EVENT_FUNC_SCHEMA doğru mu? DB tarafında fonksiyon oluşturulmalı.');
    }
  } catch (e) {
    insertEventAvailable = false;
    logger.error(`insert_machine_event şema tespiti hatası: ${e.message}`);
  }
}

async function resolveEnsureFuncSchema() {
  try {
    const { rows } = await pool.query(
      `SELECT n.nspname AS schema, p.proname AS name, pg_get_function_identity_arguments(p.oid) AS args
       FROM pg_proc p
       JOIN pg_namespace n ON n.oid = p.pronamespace
       WHERE p.proname = 'ensure_machine_events_month_part'
       ORDER BY n.nspname`
    );
    if (rows.length === 1) {
      ensureFuncSchema = rows[0].schema;
      logger.info(`ensure_machine_events_month_part bulundu: ${ensureFuncSchema}.${rows[0].name}(${rows[0].args})`);
    } else if (rows.length > 1) {
      const envMatch = rows.find((r) => r.schema === EVENT_FUNC_SCHEMA);
      ensureFuncSchema = envMatch ? envMatch.schema : rows[0].schema;
      logger.info(`ensure_machine_events_month_part birden fazla şemada; kullanılacak: ${ensureFuncSchema}`);
    } else {
      logger.warn("ensure_machine_events_month_part fonksiyonu bulunamadı (pg_proc). Varsayılan şema kullanılacak.");
    }
  } catch (e) {
    logger.error(`ensure_machine_events_month_part şema tespiti hatası: ${e.message}`);
  }
}

async function ensureEventPartitions() {
  try {
    logger.info("Partition hazırlığı (bu ay + gelecek ay) başlıyor...");
    await pool.query(
      `SELECT ${ensureFuncSchema}.ensure_machine_events_month_part(date_trunc('month', now())::date)`
    );
    await pool.query(
      `SELECT ${ensureFuncSchema}.ensure_machine_events_month_part(date_trunc('month', now() + interval '1 month')::date)`
    );
    logger.info("Partition hazırlığı tamamlandı.");
  } catch (e) {
    logger.error(`Partition hazırlığı hatası: ${e.message}`);
  }
}

function msUntilNextRun(hour = 0, minute = 5) {
  const now = new Date();
  const next = new Date(now);
  next.setHours(hour, minute, 0, 0);
  if (next <= now) {
    next.setDate(next.getDate() + 1);
  }
  return next.getTime() - now.getTime();
}

function scheduleDailyEnsurePartitions() {
  if (dailyEnsureTimer) clearTimeout(dailyEnsureTimer);
  const delay = msUntilNextRun(0, 5); // her gün 00:05'te çalışsın
  logger.info(`Günlük partition hazırlığı ${Math.round(delay / 1000)} sn sonra planlandı.`);
  dailyEnsureTimer = setTimeout(async () => {
    await ensureEventPartitions();
    // Bir sonraki gün için tekrar planla
    scheduleDailyEnsurePartitions();
  }, delay);
}

function sendToArduino(msg) {
  if (!port || !port.isOpen) {
    logger.warn(`Arduino'ya gönderilemedi (port kapalı): ${msg}`);
    return;
  }
  port.write(`${msg}\n`, (err) => {
    if (err) logger.error(`Arduino'ya yazarken hata: ${err.message}`);
    else logger.info(`Arduino'ya gönderildi: ${msg}`);
  });
}

async function fetchItemLengthAndSend() {
  try {
    const { rows } = await pool.query(
      "SELECT item_length FROM oftt_works_info WHERE wstation_id = $1 LIMIT 1",
      [WSTATION_ID]
    );
    if (rows.length && rows[0].item_length != null) {
      const lengthVal = String(rows[0].item_length).trim();
      const payload = `b${lengthVal}`;
      sendToArduino(payload);
    } else {
      logger.warn(`item_length bulunamadı (wstation_id=${WSTATION_ID})`);
    }
  } catch (e) {
    logger.error(`item_length sorgu hatası: ${e.message}`);
  }
}

async function openPort() {
  // Önce otomatik port bul
  detectedPort = await findArduinoPort();
  const portToUse = detectedPort || PORT_NAME;
  if (!detectedPort) {
    logger.warn(`Arduino portu otomatik bulunamadı, varsayılan port (${PORT_NAME}) kullanılacak.`);
  } else {
    logger.info(`Arduino portu otomatik bulundu: ${detectedPort}`);
  }
  logger.info(`PORT açılıyor: ${portToUse} @ ${BAUD_RATE}`);
  port = new SerialPort({
    path: portToUse,
    baudRate: parseInt(BAUD_RATE, 10),
    autoOpen: false,
    parity: "none",
    dataBits: 8,
    stopBits: 1,
    rtscts: false,
    lock: true,
  });

  port.open((err) => {
    if (err) {
      logger.error(`PORT açılamadı: ${err.message}`);
      scheduleReconnect();
      return;
    }
    logger.info(`PORT açık: ${portToUse}`);

    // macOS/Arduino’da reseti azaltmak için DTR’i sabitlemek bazen faydalı
    try {
      port.set({ dtr: true, rts: false }, (e) => {
        if (e) logger.warn(`port.set uyarı: ${e.message}`);
        else logger.info("DTR=1, RTS=0 ayarlandı");
      });
    } catch (e) {
      logger.warn(`port.set çağrısı başarısız: ${e.message}`);
    }

    parser = port.pipe(new ReadlineParser({ delimiter: "\n" }));
    parser.on("data", onData);

    // 60 sn periyotta item_length gönderimi başlat
    if (itemLengthTimer) clearInterval(itemLengthTimer);
    itemLengthTimer = setInterval(fetchItemLengthAndSend, 60_000);

    port.on("error", (err) => {
      logger.error(`PORT ERROR: ${err.message}`);
    });

    port.on("close", () => {
      logger.warn("PORT kapandı");
      if (itemLengthTimer) {
        clearInterval(itemLengthTimer);
        itemLengthTimer = null;
      }
      if (!stopping) scheduleReconnect();
    });
  });
  fetchItemLengthAndSend();
  // Fonksiyon şemasını tespit et (bir kez)
  // resolveInsertEventFuncSchema();
}

// Basit seri kuyruğa alma: her satırı sırayla işle
let serial = Promise.resolve();
function onData(line) {
  serial = serial
    .then(() => handleLine(line))
    .catch((e) => logger.error(`[SERIAL] Hata: ${e?.message || e}`));
}

async function handleLine(line) {
  const raw = line.trim();
  if (!raw) return;
  // # ile sonlandırılmış kısmı al
  const hashIndex = raw.indexOf("#");
  if (hashIndex === -1) {
    logger.warn(`Geçersiz veri (sonda # yok): ${raw}`);
    return;
  }
  const dataStr = raw.substring(0, hashIndex);
  const arr = dataStr.split("|");
  // Simülasyon mantığı
  let simText = "";
  let durum = 0; // statu_id: 0=kapalı, 1=şalter açık, 2=çalışıyor
  if (arr[0] === "1") {
    simText += "Şalter açık, ";
    durum = 1;
    if (arr[1] === "1") {
      simText += "Çalışıyor, ";
      durum = 2;
    } else {
      simText += "Beklemede";
    }
  } else {
    simText += "Şalter kapalı";
  }
  // logger.info(`SIM: ${simText}`);
  logger.info(`DATA: ${raw}`);

  // --- PGSQL oftt_works_info tablosu güncelleme ---
  try {
    // Abs (COUNTER_INDEX) sadece event total_counter için (varsa). Sayaç artışı sadece pulse (arr[6]) rising-edge ile.
    let absVal = null;
    let pulseVal = 0;
    if (arr[0] === "1" && arr[1] === "1") {
      const primary = arr[COUNTER_INDEX];
      if (primary != null && primary !== '') {
        const n = parseInt(String(primary).trim(), 10);
        if (!Number.isNaN(n)) absVal = n;
      }
      const pulseRaw = arr[6];
      if (pulseRaw != null && pulseRaw !== '') {
        const p = parseInt(String(pulseRaw).trim(), 10);
        if (!Number.isNaN(p)) pulseVal = p > 0 ? 1 : 0;
      }
    } else {
      pulseVal = 0; // çalışmıyorken reset kabul et
    }

    const statusChanged = lastStatus === null || durum !== lastStatus;
    const edge = (lastPulse === 0 && pulseVal === 1) ? 1 : 0;
    const delta = edge;

    if (String(COUNTER_DEBUG).toLowerCase() === "true") {
      logger.info(`[COUNTER] abs=${absVal ?? 'null'} pulse=${pulseVal} lastPulse=${lastPulse} edge=${edge} delta=${delta} buf=${counterBuffer}`);
    }

    if (statusChanged || delta > 0) {
      // Sayaç artışını buffer'a ekle ve logla
      if (delta > 0) {
        const beforeBuf = counterBuffer;
        counterBuffer += delta;
        const afterBuf = counterBuffer;
        if (String(COUNTER_DEBUG).toLowerCase() === "true") {
          logger.info(`[COUNTER] inc buf:${beforeBuf}->${afterBuf}`);
        }
        // Eşik aşıldıysa anında flush dene (await yok)
        if (afterBuf >= BUFFER_MAX) {
          flushCounterBuffer().catch(() => {});
        }
      }

      // Hız (arr[2]) - en yakın tam sayıya yuvarla
      const parsedSpeed =
        arr[2] != null && arr[2] !== "" ? parseFloat(arr[2].trim()) : NaN;
      const speedVal = Number.isFinite(parsedSpeed)
        ? Math.round(parsedSpeed)
        : 0;

      // statu değiştiyse oftt_work_order hareket kayıtlarını işle
      if (statusChanged) {
        try {
          const textStatus = durum === 2 ? 'RUNNING' : durum === 1 ? 'DOWN' : 'IDLE';
          const breakReasonCode = textStatus === 'DOWN' ? '000' : null;
          const breakReasonDescription = textStatus === 'DOWN' ? 'GİRİLMEDİ' : null;
          if (!insertEventAvailable) {
            if (!insertEventWarned) {
              logger.warn(`insert_machine_event fonksiyonu bulunamadı. EVENT_FUNC_SCHEMA='${EVENT_FUNC_SCHEMA}'. DB'de fonksiyon oluşturulmalı veya şema düzeltilmeli.`);
              insertEventWarned = true;
            }
          } else {
          const funcSql = `SELECT ${eventFuncSchema}.insert_machine_event(
            $1::bigint,              -- p_machine_id
            now(),                   -- p_event_ts
            $2::public.machine_state,-- p_state (enum)
            $7::text,            -- p_break_reason_code
            $8::text,            -- p_break_reason_description
            (SELECT worder_id::bigint FROM oftt_works_info WHERE wstation_id = $1::int),  -- p_work_order_id
            (SELECT item_id::bigint FROM oftt_works_info WHERE wstation_id = $1::int),    -- p_product_id
            (SELECT operator_id::bigint FROM oftt_works_info WHERE wstation_id = $1::int),-- p_operator_id
            $3::int,                 -- p_good_count_inc (delta)
            $4::int,                 -- p_scrap_count_inc
            $5::bigint,              -- p_total_counter (mutlak sayaç)
            $6::numeric              -- p_speed_actual
          )`;
          try {
            const totalForEvent = absVal != null ? absVal : null;
            await pool.query(funcSql, [WSTATION_ID, textStatus, delta, 0, totalForEvent, speedVal, breakReasonCode, breakReasonDescription]);
          } catch (e) {
            logger.error(
              `insert_machine_event çağrı hatası: ${e.message} | schema=${eventFuncSchema} args=` +
              JSON.stringify([
                { machine_id: typeof WSTATION_ID },
                { state: textStatus },
                { good_inc: delta },
                { scrap_inc: 0 },
                { total_counter: absVal ?? null },
                { speed: speedVal },
                { break_code: breakReasonCode },
                { break_desc: breakReasonDescription }
              ])
            );
            insertEventAvailable = false;
            throw e;
          }
          // insert sonrası shift_id güncelle
          try {
            const currentShiftId = getCurrentShiftId();
            if (currentShiftId != null) {
              const updSql = `WITH last_evt AS (
                SELECT id FROM ${eventFuncSchema}.machine_events
                WHERE wstation_id = $1::bigint
                ORDER BY event_ts DESC NULLS LAST
                LIMIT 1
              )
              UPDATE ${eventFuncSchema}.machine_events e
              SET shift_id = $2::int
              FROM last_evt
              WHERE e.id = last_evt.id`;
              const rUpd = await pool.query(updSql, [WSTATION_ID, currentShiftId]);
              // logger.info(`Event shift_id güncellendi (status change): shift_id=${currentShiftId}, rowCount=${rUpd.rowCount}`);
            } else {
              logger.warn('Geçerli vardiya bulunamadı (status change sonrası shift_id atanamadı)');
            }
          } catch (se) {
            logger.error(`shift_id güncelleme hatası (status change): ${se.message}`);
          }
          }
        } catch (e) {
          logger.error(`work_order hareket kaydı hatası: ${e.message}`);
        }
      }

      // statu değiştiyse statu_time'ı da güncelle, değişmediyse dokunma
      if (statusChanged) {
        await pool.query(
          "UPDATE oftt_works_info SET statu_id = $1, statu_time = NOW(), speed = $2, break_description = '' WHERE wstation_id = $3",
          [durum, speedVal, WSTATION_ID]
        );
      } else {
        await pool.query(
          "UPDATE oftt_works_info SET speed = $1 WHERE wstation_id = $2",
          [speedVal, WSTATION_ID]
        );

        // Aynı anda son machine_events kaydında good_count_inc ve total_counter'ı 1 artır
        // İsteğe bağlı: machine_events manuel artırım (trigger yoksa açın)
        if (String(MACHINE_EVENTS_MANUAL_INCREMENT).toLowerCase() === 'true') {
          try {
            const incSql = `WITH last_evt AS (
              SELECT id FROM ${eventFuncSchema}.machine_events
              WHERE wstation_id = $1::bigint
              ORDER BY event_ts DESC NULLS LAST
              LIMIT 1
            )
            UPDATE ${eventFuncSchema}.machine_events e
            SET good_count_inc = COALESCE(e.good_count_inc, 0) + $2::int,
                total_counter = COALESCE(e.total_counter, 0) + $2::int
            FROM last_evt
            WHERE e.id = last_evt.id`;
            // Talebe göre: delta>0 ise 1 artır
            const incBy = delta > 0 ? 1 : 0;
            const res = await pool.query(incSql, [WSTATION_ID, incBy]);
            logger.debug(`machine_events manuel artırım: incBy=${incBy}, rowCount=${res.rowCount}`);
          } catch (e) {
            logger.error(`machine_events artırım hatası: ${e.message}`);
          }
        } else {
          logger.debug('machine_events manuel artırım kapalı (MACHINE_EVENTS_MANUAL_INCREMENT=false)');
        }
      }

      // Başarılı ise son değerleri güncelle (status burada da set edilecek)
    }
    // Her durumda son değerleri güncelle: edge kaçarsa kaçmasın, takip düzgün olsun
    lastStatus = durum;
    lastPulse = pulseVal;
  } catch (err) {
    logger.error(`PGSQL info güncelleme hatası: ${err.message}`);
  }
}

function scheduleReconnect() {
  if (reconnectTimer || stopping) return;
  logger.info("Yeniden bağlanma 3 sn içinde denenecek...");
  reconnectTimer = setTimeout(async () => {
    reconnectTimer = null;
    openPort();
  }, 3000);
}

function shutdown() {
  stopping = true;
  clearTimeout(reconnectTimer);
  if (itemLengthTimer) {
    clearInterval(itemLengthTimer);
    itemLengthTimer = null;
  }
  if (dailyEnsureTimer) {
    clearTimeout(dailyEnsureTimer);
    dailyEnsureTimer = null;
  }
  if (shiftWatcherTimer) {
    clearInterval(shiftWatcherTimer);
    shiftWatcherTimer = null;
  }
  logger.info("Kapanıyor...");
  const finalize = () => {
    if (port && port.isOpen) {
      port.close((err) => {
        if (err) logger.error(`Kapanırken hata: ${err.message}`);
        process.exit(0);
      });
    } else {
      process.exit(0);
    }
  };
  // Kapanışta buffer'ı yazmaya çalış
  Promise.resolve()
    .then(() => flushCounterBuffer())
    .finally(finalize);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
process.on("uncaughtException", (err) => {
  logger.error(`UNCAUGHT: ${err.stack || err.message}`);
  scheduleReconnect();
});
process.on("unhandledRejection", (err) => {
  logger.error(`UNHANDLED REJECTION: ${err?.stack || err}`);
});

function ensureEventPartitionsOnce() {
  if (initialPartitionEnsured) return;
  initialPartitionEnsured = true;
  resolveEnsureFuncSchema().finally(() => {
    ensureEventPartitions();
  });
}

function startPartitionSchedulerOnce() {
  if (partitionSchedulerStarted) return;
  partitionSchedulerStarted = true;
  // şema tespiti ve açılış ensure'i
  ensureEventPartitionsOnce();
  // günlük planlama
  scheduleDailyEnsurePartitions();
}

// Uygulama başlarken bir defa partition scheduler başlat
startPartitionSchedulerOnce();

// insert_machine_event fonksiyon şeması (bir kere tespit)
resolveInsertEventFuncSchema();

// ---------------- VARDİYA PLANLAMA ----------------
function toMinutes(hhmm) {
  const [h, m] = hhmm.split(':').map(Number);
  return h * 60 + m;
}

function parseShiftSlices(defStr) {
  let id = 0;
  return defStr
    .split(/\s*,\s*/)
    .map((seg) => {
      const [start, end] = seg.split(/\s*-\s*/);
      return { start, end };
    })
    .filter(
      (s) => /\d{2}:\d{2}/.test(s.start) && /\d{2}:\d{2}/.test(s.end)
    )
    .map((s) => {
      id += 1;
      return {
        id,
        start: s.start,
        end: s.end,
        startMin: toMinutes(s.start),
        endMin: toMinutes(s.end),
      };
    });
}

function nowInTZ() {
  try {
    const dtf = new Intl.DateTimeFormat('tr-TR', {
      timeZone: TIME_ZONE,
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
    });
    const parts = dtf
      .formatToParts(new Date())
      .reduce((a, p) => ((a[p.type] = p.value), a), {});
    return { h: parseInt(parts.hour, 10), m: parseInt(parts.minute, 10) };
  } catch {
    const d = new Date();
    return { h: d.getUTCHours(), m: d.getUTCMinutes() };
  }
}

function getCurrentShiftId() {
  if (!shiftSlices.length) return null;
  const { h, m } = nowInTZ();
  const curMin = h * 60 + m;
  for (const s of shiftSlices) {
    // Normal dilim
    if (s.endMin > s.startMin) {
      if (curMin >= s.startMin && curMin < s.endMin) return s.id;
    } else {
      // Gece devreden (örn 23:00-08:00)
      if (curMin >= s.startMin || curMin < s.endMin) return s.id;
    }
  }
  return null;
}

function toTimeWithSeconds(hhmm) {
  const s = (hhmm || '').trim();
  if (!/^\d{2}:\d{2}$/.test(s)) return null;
  return `${s}:00`;
}

function nextOccurrenceOf(timeHHMM) {
  const [h, m] = timeHHMM.split(":").map(Number);
  const now = new Date();
  const dt = new Date(now);
  dt.setHours(h, m, 0, 0);
  if (dt <= now) dt.setDate(dt.getDate() + 1);
  return dt;
}

// Vardiya değiştiğinde (statü değişmese bile) açık event'i kapatıp, aynı statüyle yeni event açar
async function handleShiftChange(newShiftId) {
  if (handlingShiftChange) return;
  handlingShiftChange = true;
  try {
    // 1) Mevcut açık event'i (end_ts IS NULL) varsa kapat
    const { rows: openRows } = await pool.query(
      `SELECT id FROM ${eventFuncSchema}.machine_events
       WHERE wstation_id = $1::bigint AND end_ts IS NULL
       ORDER BY event_ts DESC NULLS LAST LIMIT 1`,
      [WSTATION_ID]
    );
    if (openRows.length) {
      const { id } = openRows[0];
      try {
        const r = await pool.query(
          `UPDATE ${eventFuncSchema}.machine_events SET end_ts = now() WHERE id = $1`,
          [id]
        );
        logger.info(`Açık event kapatıldı (vardiya değişimi): id=${id}, rowCount=${r.rowCount}`);
      } catch (e) {
        logger.error(`Açık event kapatılamadı: ${e.message}`);
      }
    } else {
      logger.info('Kapatılacak açık event bulunamadı (vardiya değişimi).');
    }

    // 2) oftt_works_info'dan son statüyü çek ve aynı statüyle yeni event aç
    const { rows } = await pool.query(
      `SELECT statu_id, counter, speed, worder_id, item_id, operator_id
       FROM oftt_works_info WHERE wstation_id=$1 LIMIT 1`,
      [WSTATION_ID]
    );
    if (!rows.length) {
      logger.warn('Vardiya değişimi: oftt_works_info kaydı yok, yeni event açılamadı.');
      lastKnownShiftId = newShiftId; // yine de vardiya bilgisini güncelle
      return;
    }
    const r = rows[0];
    const durum = r.statu_id ?? 0;
    const textStatus = durum === 2 ? 'RUNNING' : durum === 1 ? 'DOWN' : 'IDLE';
    const totalCounter = r.counter || 0;
    const speedVal = r.speed || 0;

    if (!insertEventAvailable) {
      if (!insertEventWarned) {
        logger.warn(`insert_machine_event bulunamadı. EVENT_FUNC_SCHEMA='${EVENT_FUNC_SCHEMA}'. Vardiya değişimi event açılamadı.`);
        insertEventWarned = true;
      }
    } else {
      const funcSql = `SELECT ${eventFuncSchema}.insert_machine_event(
        $1::bigint,
        now(),
        $2::public.machine_state,
        $8::text,
        $9::text,
        $3::bigint,
        $4::bigint,
        $5::bigint,
        0::int,
        0::int,
        $6::bigint,
        $7::numeric
      )`;
      await pool.query(funcSql, [
        WSTATION_ID,
        textStatus,
        r.worder_id || null,
        r.item_id || null,
        r.operator_id || null,
        totalCounter,
        speedVal,
        SHIFT_BREAK_CODE,
        SHIFT_BREAK_DESCRIPTION
      ]);
      // Açılan yeni event'in shift_id'sini ata
      try {
        const updSql = `WITH last_evt AS (
          SELECT id FROM ${eventFuncSchema}.machine_events
          WHERE wstation_id = $1::bigint
          ORDER BY event_ts DESC NULLS LAST
          LIMIT 1
        )
        UPDATE ${eventFuncSchema}.machine_events e
        SET shift_id = $2::int
        FROM last_evt
        WHERE e.id = last_evt.id`;
        await pool.query(updSql, [WSTATION_ID, newShiftId]);
        logger.info(`Vardiya değişimi: Yeni event açıldı, shift_id=${newShiftId}, status=${textStatus}`);
      } catch (se) {
        logger.error(`Vardiya değişimi: shift_id atanamadı: ${se.message}`);
      }
    }
    lastKnownShiftId = newShiftId;
  } catch (e) {
    logger.error(`handleShiftChange hatası: ${e.message}`);
  } finally {
    handlingShiftChange = false;
  }
}

async function onShiftStart(slice) {
  try {
    // oftt_works_info'dan mevcut durum çek
    const { rows } = await pool.query(
      `SELECT statu_id, counter, speed, worder_id, item_id, operator_id FROM oftt_works_info WHERE wstation_id=$1 LIMIT 1`,
      [WSTATION_ID]
    );
    if (!rows.length) {
      logger.warn(`Vardiya başlangıcı: wstation_id=${WSTATION_ID} için kayıt yok.`);
    } else {
      const r = rows[0];
      const durum = r.statu_id ?? 0;
      const textStatus = durum === 2 ? 'RUNNING' : durum === 1 ? 'DOWN' : 'IDLE';
      const totalCounter = r.counter || 0;
      const speedVal = r.speed || 0;
      // İstenirse vardiya dilimi için oee_slice hesapla (base date: vardiya başlangıç günü)
      if (String(SHIFT_OEE_LOG).toLowerCase() === 'true') {
        try {
          const tzNow = new Date();
          // TIME_ZONE'a göre YYYY-MM-DD formatla
          const fmt = new Intl.DateTimeFormat('tr-TR', { timeZone: TIME_ZONE, year: 'numeric', month: '2-digit', day: '2-digit' });
          const parts = fmt.formatToParts(tzNow).reduce((a,p)=> (a[p.type]=p.value,a), {});
          const baseDate = `${parts.year}-${parts.month}-${parts.day}`; // shift start tarihi
          const tStart = toTimeWithSeconds(slice.start);
          const tEnd = toTimeWithSeconds(slice.end);
          if (!tStart || !tEnd) {
            logger.warn(`OEE SLICE atlandı: geçersiz vardiya saatleri start='${slice.start}' end='${slice.end}'`);
          } else {
            const { rows: oeeRows } = await pool.query(
              `SELECT * FROM public.oee_slice($1::int, $2::date, $3::time, $4::time)`,
              [WSTATION_ID, baseDate, tStart, tEnd]
            );
            if (oeeRows.length) {
              logger.info(`OEE SLICE ${slice.start}-${slice.end} (${baseDate}) -> ${JSON.stringify(oeeRows[0])}`);
            } else {
              logger.info(`OEE SLICE ${slice.start}-${slice.end} (${baseDate}) boş sonuç.`);
            }
          }
        } catch (oe) {
          logger.error(`oee_slice hata (${slice.start}-${slice.end}): ${oe.message}`);
        }
      }
      // Vardiya değişimini tek noktadan yönet
      await handleShiftChange(slice.id);
    }
  } catch (e) {
    logger.error(`Vardiya başlangıcı event hatası (${slice.start}-${slice.end}): ${e.message}`);
  }
  // Bir sonraki aynı vardiya başlangıcını planla (24 saat sonra)
  scheduleShiftSlice(slice);
}

function scheduleShiftSlice(slice) {
  const when = nextOccurrenceOf(slice.start);
  const delay = when.getTime() - Date.now();
  const t = setTimeout(() => onShiftStart(slice), delay);
  shiftTimers.push(t);
  logger.info(`Vardiya başlangıcı planlandı: ${slice.start}-${slice.end} ~ ${Math.round(delay/1000)} sn sonra.`);
}

function startShiftScheduler() {
  shiftSlices = parseShiftSlices(SHIFT_SLICES);
  if (!shiftSlices.length) {
    logger.warn('SHIFT_SLICES boş veya geçersiz. Vardiya zamanlayıcı başlatılmadı.');
    return;
  }
  shiftSlices.forEach(scheduleShiftSlice);
  logger.info(`Toplam ${shiftSlices.length} vardiya başlangıcı zamanlandı (ID'ler 1..${shiftSlices.length}).`);
  // Sürekli vardiya izleyici: her 30 saniyede bir kontrol et
  if (shiftWatcherTimer) clearInterval(shiftWatcherTimer);
  lastKnownShiftId = getCurrentShiftId();
  logger.info(`Vardiya izleme başlatıldı. İlk vardiya: ${lastKnownShiftId ?? 'bilinmiyor'}`);
  shiftWatcherTimer = setInterval(async () => {
    try {
      const sid = getCurrentShiftId();
      if (sid == null) return;
      if (lastKnownShiftId == null) {
        lastKnownShiftId = sid; // ilk tespit
        return;
      }
      if (sid !== lastKnownShiftId) {
        logger.info(`Vardiya değişti: ${lastKnownShiftId} -> ${sid}`);
        await handleShiftChange(sid);
      }
    } catch (e) {
      logger.error(`Vardiya izleyici hatası: ${e.message}`);
    }
  }, 30_000);
}

startShiftScheduler();
// --------------------------------------------------

openPort();