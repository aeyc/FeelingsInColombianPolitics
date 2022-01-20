const connection = require('./connect')
const client = connection.connection
var JSONbig = require('json-bigint')
const fs = require('fs')
const file =  process.argv
const jsonFile = fs.readFileSync(file[2], {encoding: 'utf-8'})
const usersData = JSONbig.parse(jsonFile)

async function insertUsers(src) {
    let i = 0
    for (const u of src) {
        const username = u['username']
        const userid = BigInt(u['userid'])
        let exists = await userExists(userid)
        if (!exists) {
            const query = {
                text: 'INSERT INTO users (userid, username) VALUES ($1, $2)',
                values: [userid, username]
            }
            inserted = await insertUser(query)
            if (inserted) i = i + 1
        }
    }
    console.log(i + ' users inserted out of ' + src.length)
    client.end()
}

async function userExists(id) {
    const query = {
        text: 'SELECT userid FROM users WHERE userid = $1',
        values: [id]
    }
    return client.query(query)
    .then((res) => {
        if (res.rowCount > 0) {
            return true
        } else {
            return false
        }
    })
    .catch(err => {
        console.log('ERROR finding user')
        console.log(err)
    })
}

async function insertUser(query) {
    return client.query(query)
    .then((res) => {
        return true
    })
    .catch(err => {
        console.log('ERROR inserting user')
        console.log(err)
        return false
    })
}

insertUsers(usersData)