const connection = require('./connect')
const client = connection.connection
var JSONbig = require('json-bigint')
const fs = require('fs')
const file =  process.argv
const jsonFile = fs.readFileSync(file[2], {encoding: 'utf-8'})
const tweetData = JSONbig.parse(jsonFile)

async function insertTweets(src) {
    for (const tweet of src) {
        const tweetId = BigInt(tweet['id'])
        const authorId = BigInt(tweet['author_id'])
        const createdAt = tweet['created_at']
        const text = tweet['text']
        let inReply = tweet['tweet_id'] ? BigInt(tweet['tweet_id']) : null
        let geo = tweet['geo'] || null
        if (geo) {
            geo = geo['place_id']
        }
        const retweets = tweet['public_metrics']['retweet_count']
        const replies = tweet['public_metrics']['reply_count']
        const likes = tweet['public_metrics']['like_count']
        const quotes = tweet['public_metrics']['quote_count']
        const lang = tweet['lang']
        const entities = tweet['entities'] || null
        let hashtags = []
        let mentions = []
        if (entities) {
            hashtags = tweet['entities']['hashtags'] || []
            mentions = tweet['entities']['mentions'] || []
        }
        
        hashtagsIds = []
        for (const h of hashtags) {
            // insert h, get IDs 
            let h_id = await insertHashtag(h.tag)
            hashtagsIds.push(h_id)
        }

        // insert tweet
        let id = await insertTweet(
            tweetId, authorId, text, createdAt, null, inReply, geo, retweets, likes, replies, quotes, lang, null
        )
        // insert hashtag-tweet
        for (const h_id of hashtagsIds) {
            await insertHashtagTweet(id, h_id)
        }
        // insert mention
        for (const m of mentions) {
            // insert mentions
            const username = m['username']
            const userId = BigInt(m['id'])
            const isReply = userId.toString().length > 10
            await insertMention(id, userId, username, isReply)
        }
    }
    console.log('cest fini')
}

function insertHashtag(tag) {
    return findHashtagId(tag)
        .then((id) => {
            if (id) { return id }
            else {
                const query = {
                    text: 'INSERT INTO hashtag (tag) VALUES ($1) RETURNING id',
                    values: [tag]
                }
                return client.query(query)
            }
        })
        .then((res) => {
            if (res.rows) { return res.rows[0].id }
            else { return res }
        })
        .catch(err => {
            console.log('error')
            console.log(err)
        })
}

function findHashtagId(tag) {
    const query = {
        text: 'SELECT id FROM hashtag WHERE tag = $1',
        values: [tag]
    }
    return client.query(query)
    .then((res) => {
        if (res.rowCount > 0) {
            return res.rows[0].id
        } else {
            return null
        }
    })
    .catch(err =>{
        console.log('ERROR finding hashtag')
        console.log(err)
    })
}

function insertHashtagTweet(tweetId, hashtagId) {
    const findCombinationQuery = {
        text: 'SELECT tweet_id, hashtag_id FROM tweet_hashtag WHERE tweet_id= $1 AND hashtag_id= $2',
        values: [tweetId, hashtagId]
    }
    return client.query(findCombinationQuery)
        .then((res) => {
            if (res.rowCount > 0) {
                return res
            } else {
                const query = {
                    text: 'INSERT INTO tweet_hashtag (tweet_id, hashtag_id) VALUES ($1, $2)',
                    values: [tweetId, hashtagId]
                }
                return client.query(query)
            }
        })
        .then((res) => {
            return res
        })
        .catch(err => {
            console.log('ERROR INSERTING TWEET-HASHTAG')
            console.log(err)
        })
}

function insertTweet(twitter_id, user_id, full_text, created_at, src, in_reply_twitter_id,
    geo_id, retweet_cnt, favorited_cnt, replies, quotes, lang, is_sensitive) {

    const findTweetQuery = {
        text: 'SELECT id FROM tweet WHERE twitter_id = $1',
        values: [twitter_id]
    }
    return client.query(findTweetQuery)
        .then((res) => {
            if (res.rowCount > 0) {
                return res
            } else {
                const query = {
                    text: 'INSERT INTO tweet (twitter_id, user_id, full_text, created_at, src, in_reply_twitter_id, ' +
                        'geo_id, retweet_cnt, favorited_cnt, replies, quotes, lang, is_sensitive)' +
                        'VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) RETURNING id',
                    values: [twitter_id, user_id, full_text, created_at, src, in_reply_twitter_id,
                        geo_id, retweet_cnt, favorited_cnt, replies, quotes, lang, is_sensitive]
                }
                return client.query(query)
            }
        })
        .then((res) => {
            return res.rows[0].id
        })
        .catch(err => {
            console.log('ERROR INSERTING TWEET')
            console.log(err)
        })

}

function insertMention(tweetId, userId, userName, isReply) {
    const findMentionQuery = {
        text: 'SELECT * FROM mention where tweet_id = $1 AND id = $2',
        values: [tweetId, userId]
    }
    return client.query(findMentionQuery)
        .then((res) => {
            if (res.rowCount > 0) {
                return null
            } else {
                const insertQuery = {
                    text: 'INSERT INTO mention (id, tweet_id, username, is_reply) VALUES ($1, $2, $3, $4)',
                    values: [userId, tweetId, userName, isReply]
                }
                return client.query(insertQuery)
            }
        })
        .then((res) => {
            return res
        })
        .catch(err => {
            console.log('ERROR inserting mention')
            console.log(tweetId)
            console.log(userId)
            console.log(userName)
            console.log(err)
        })
}

insertTweets(tweetData)
// insertHashtag('ElDebate')
// insertHashtag('LigaAnticorrupcion')
// insertHashtag('RodolfoHernandez')

// client.end()