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
  ISTASYON_ID = "378",
} = process.env;

// --- Logger ---
fs.mkdirSync(path.dirname(LOG_PATH), { recursive: true });
const logger = winston.createLogger({
  level: "info",
  transports: [
    new winston.transports.File({
      filename: LOG_PATH,
      maxsize: 5_000_000,
      maxFiles: 3,
    }),
    new winston.transports.Console(),
  ],
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(
      ({ level, message, timestamp }) => `${timestamp} [${level}] ${message}`
    )
  ),
});

let port,
  parser,
  stopping = false,
  reconnectTimer = null;
let sayac = 0;
// oftt_works_info güncelleme eşiği: sadece değişiklik olduğunda UPDATE çalıştır
let lastStatus = null; // son gönderilen statu_id
let lastCounter = null; // makineden okunan son sayaç (mutlak değer)
let itemLengthTimer = null; // 60 sn periyodik sorgu timer

const WSTATION_ID = parseInt(ISTASYON_ID, 10) || 378;

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

function openPort() {
  logger.info(`PORT açılıyor: ${PORT_NAME} @ ${BAUD_RATE}`);
  port = new SerialPort({
    path: PORT_NAME,
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
    logger.info(`PORT açık: ${PORT_NAME}`);

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
}

async function onData(line) {
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
  logger.info(`SIM: ${simText}`);
  logger.info(`DATA: ${raw}`);

  // --- PGSQL oftt_works_info tablosu güncelleme ---
  try {
    // Makinenin gönderdiği mutlak sayaç değeri (yalnızca şalter ve çalışma açıkken geçerli)
    let machineCounter = 0;
    if (arr[0] === "1" && arr[1] === "1") {
      machineCounter = arr[6] ? parseInt(arr[6], 10) : 0;
    }

    // Değişiklik tespiti
    const statusChanged = lastStatus === null || durum !== lastStatus;
    const counterChanged = lastCounter === null || machineCounter !== lastCounter;

    if (statusChanged || counterChanged) {
      // Delta: sadece artış kadar sayaç artır (azalma/reset durumunda 0 kabul et)
      let delta = 0;
      if (lastCounter !== null && machineCounter > lastCounter) {
        delta = machineCounter - lastCounter;
      }

  // Hız (arr[2]) - en yakın tam sayıya yuvarla
  const parsedSpeed = arr[2] != null && arr[2] !== '' ? parseFloat(arr[2].trim()) : NaN;
  const speedVal = Number.isFinite(parsedSpeed) ? Math.round(parsedSpeed) : 0;

      // statu değiştiyse statu_time'ı da güncelle, değişmediyse dokunma
      if (statusChanged) {
        await pool.query(
          "UPDATE oftt_works_info SET statu_id = $1, statu_time = NOW(), counter = counter + $2, speed = $3 WHERE wstation_id = $4",
          [durum, delta, speedVal, WSTATION_ID]
        );
      } else {
        await pool.query(
          "UPDATE oftt_works_info SET counter = counter + $1, speed = $2 WHERE wstation_id = $3",
          [delta, speedVal, WSTATION_ID]
        );
      }

      // Başarılı ise son değerleri güncelle
      lastStatus = durum;
      lastCounter = machineCounter;
    }
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
  logger.info("Kapanıyor...");
  if (port && port.isOpen) {
    port.close((err) => {
      if (err) logger.error(`Kapanırken hata: ${err.message}`);
      process.exit(0);
    });
  } else {
    process.exit(0);
  }
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

openPort();
