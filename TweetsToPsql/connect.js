const env = 'dev' // change to prod
const creds = require('./env')[env]
console.log(creds)

// const { MongoClient } = require('mongodb')
const { Pool, Client } = require('pg')
// const mongoose = require('mongoose')

// POSTGRESQL connection

const pool = new Pool({
  user: creds['user'],
  host: creds['host'],
  database: creds['database'],
  password: creds['password'],
  port: creds['port']
})

const client = new Client({
  user: creds['user'],
  host: creds['host'],
  database: creds['database'],
  password: creds['password'],
  port: creds['port']
})

client.connect()

client.query('SELECT NOW()', (res, err) => {
  if (err) {
    console.log(err)
  }
  console.log('OK')
  return res
})

module.exports.connection = client