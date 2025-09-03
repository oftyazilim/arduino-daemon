import { Pool } from 'pg'

const pool = new Pool({
  host: process.env.DB_HOST || '192.6.2.10',
  port: process.env.DB_PORT ? parseInt(process.env.DB_PORT, 10) : 5432,
  user: process.env.DB_USERNAME || 'uyum',
  password: process.env.DB_PASSWORD || 'Cnvt*Bk571',
  database: process.env.DB_DATABASE || 'oft_mes',
})

export default pool
