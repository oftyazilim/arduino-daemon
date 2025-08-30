import 'dotenv/config'
import pool from './db.js'
import { SerialPort } from 'serialport'
import { ReadlineParser } from '@serialport/parser-readline'
import fs from 'node:fs'
import path from 'node:path'
import axios from 'axios'
import winston from 'winston'

const {
  PORT_NAME = '/dev/tty.usbmodem1101',
  BAUD_RATE = '9600',
  LOG_PATH = `${process.env.HOME}/arduino-daemon/logs/daemon.log`,
  POST_URL,
  POST_TIMEOUT_MS = '3000',
} = process.env

// --- Logger ---
fs.mkdirSync(path.dirname(LOG_PATH), { recursive: true })
const logger = winston.createLogger({
  level: 'info',
  transports: [
    new winston.transports.File({ filename: LOG_PATH, maxsize: 5_000_000, maxFiles: 3 }),
    new winston.transports.Console()
  ],
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ level, message, timestamp }) => `${timestamp} [${level}] ${message}`)
  ),
})

let port, parser, stopping = false, reconnectTimer = null
let sayac = 0

function openPort () {
  logger.info(`PORT açılıyor: ${PORT_NAME} @ ${BAUD_RATE}`)
  port = new SerialPort({
    path: PORT_NAME,
    baudRate: parseInt(BAUD_RATE, 10),
    autoOpen: false,
    parity: 'none',
    dataBits: 8,
    stopBits: 1,
    rtscts: false,
    lock: true
  })

  port.open(err => {
    if (err) {
      logger.error(`PORT açılamadı: ${err.message}`)
      scheduleReconnect()
      return
    }
    logger.info(`PORT açık: ${PORT_NAME}`)

    // macOS/Arduino’da reseti azaltmak için DTR’i sabitlemek bazen faydalı
    try {
      port.set({ dtr: true, rts: false }, e => {
        if (e) logger.warn(`port.set uyarı: ${e.message}`)
        else logger.info('DTR=1, RTS=0 ayarlandı')
      })
    } catch (e) {
      logger.warn(`port.set çağrısı başarısız: ${e.message}`)
    }

    parser = port.pipe(new ReadlineParser({ delimiter: '\n' }))
    parser.on('data', onData)

    port.on('error', err => {
      logger.error(`PORT ERROR: ${err.message}`)
    })

    port.on('close', () => {
      logger.warn('PORT kapandı')
      if (!stopping) scheduleReconnect()
    })
  })
}

async function onData (line) {
  const raw = line.trim()
  if (!raw) return
  // # ile sonlandırılmış kısmı al
  const hashIndex = raw.indexOf('#')
  if (hashIndex === -1) {
    logger.warn(`Geçersiz veri (sonda # yok): ${raw}`)
    return
  }
  const dataStr = raw.substring(0, hashIndex)
  const arr = dataStr.split('|')
  // Simülasyon mantığı
  let simText = ''
  let durum = 'k' // default: kapalı
  if (arr[0] === '1') {
    simText += 'Şalter açık, '
    durum = 'd'
    if (arr[1] === '1') {
      simText += 'Çalışıyor, '
      durum = 'c'
    } else {
      simText += 'Beklemede'
    }
  } else {
    simText += 'Şalter kapalı'
  }
  logger.info(`SIM: ${simText}`)
  logger.info(`DATA: ${raw}`)

  // --- PGSQL caldurum tablosu güncelleme ---
  try {
    // Sayaç değeri
    const sayac = arr[4] ? parseInt(arr[4], 10) : 0
    // Önce mevcut kaydı çek
    const { rows } = await pool.query("SELECT sayac FROM caldurum WHERE hat = 'MA-1' LIMIT 1")
    if (rows.length > 0) {
    //   let yeniId = rows[0].id
    //   let eskiSayac = rows[0].sayac || 0
      // Sayaç artışı varsa id'yi 1 arttır
    //   if (sayac > 0) yeniId = yeniId + 1
      await pool.query(
        "UPDATE caldurum SET durum = $1, sayac = sayac + $2 WHERE hat = 'MA-1'",
        [durum, sayac]
      )
    } else {
      // Kayıt yoksa ekle
      await pool.query(
        "INSERT INTO caldurum (hat, durum, sayac) VALUES ('MA-1', $1, $2)",
        [durum, sayac]
      )
    }
  } catch (err) {
    logger.error(`PGSQL caldurum güncelleme hatası: ${err.message}`)
  }

  if (POST_URL) {
    try {
      await axios.post(POST_URL, { data: raw, ts: new Date().toISOString() }, { timeout: parseInt(POST_TIMEOUT_MS,10) })
      logger.info(`POST OK -> ${POST_URL}`)
    } catch (e) {
    //   logger.error(`POST FAIL: ${e.message}`)
    }
  }
}

function scheduleReconnect () {
  if (reconnectTimer || stopping) return
  logger.info('Yeniden bağlanma 3 sn içinde denenecek...')
  reconnectTimer = setTimeout(async () => {
    reconnectTimer = null
    openPort()
  }, 3000)
}

function shutdown () {
  stopping = true
  clearTimeout(reconnectTimer)
  logger.info('Kapanıyor...')
  if (port && port.isOpen) {
    port.close(err => {
      if (err) logger.error(`Kapanırken hata: ${err.message}`)
      process.exit(0)
    })
  } else {
    process.exit(0)
  }
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)
process.on('uncaughtException', err => {
  logger.error(`UNCAUGHT: ${err.stack || err.message}`)
  scheduleReconnect()
})
process.on('unhandledRejection', err => {
  logger.error(`UNHANDLED REJECTION: ${err?.stack || err}`)
})

openPort()
