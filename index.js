const http = require('http');
const WebSocket = require('ws');
const server = http.createServer(); // Use HTTP server
const wss = new WebSocket.Server({
    server,
    path: '/index-ws'
});

const {
    createClient
} = require("@supabase/supabase-js");
const supabaseUrl = "https://jjtqvxvprcmblezstaks.supabase.co";
const supabaseKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImpqdHF2eHZwcmNtYmxlenN0YWtzIiwicm9sZSI6ImFub24iLCJpYXQiOjE2OTE3NjAxMjAsImV4cCI6MjAwNzMzNjEyMH0.glxbp12RNVsu6TaSqPGH_CUDs9AH7T1jNkfwLtz3ZQI";
const supabase = createClient(supabaseUrl, supabaseKey);

const apiToken = "227cbd70-db72-4532-a285-bfaf74481af5";
const marketData = require("api")("@mobula-api/v1.0#4cpc4om4lkxxs6mc");
const tradeHistory = require("api")("@mobula-api/v1.0#1y6qv6aclmauztal");

let isWebSocketActive = false; // Flag to track WebSocket activity
let noConnectionInterval;

const connections = new Set(); // Set to track WebSocket connections

function broadcast(message) {
    // Send a message to all connected WebSocket clients
    connections.forEach((ws) => {
        ws.send(message);
    });
}

async function processTradeData(record) {
    // Fetch all available trade data, not just the latest
    const tradeData = await tradeHistory.getTradeHistory({
        asset: record.name,
        maxResults: 'all' // Fetch all available data
    });

    const batchSize = 25; // Set an appropriate batch size
    let startIndex = 0;

    while (startIndex < tradeData.data.data.length) {
        const batch = tradeData.data.data.slice(startIndex, startIndex + batchSize);

        for (const trade of batch) {
            trade.asset = record.name;

            const tradeRecordIdentifier = `${trade.date}-${trade.hash}-${trade.value_usd}-${trade.token_amount}-${trade.token_price}-${trade.type}-${trade.blockchain}`;
            trade.hash_iq = tradeRecordIdentifier;

            const { data: existingRecords, error } = await supabase
                .from("trades")
                .select()
                .eq("hash_iq", trade.hash_iq)
                .single();

            if (error) {
                console.error("Error querying existing records:", error);
                return;
            }

            if (!existingRecords) {
                deduplicatedTradeData.push(trade);
            } else {
                console.log(`Duplicate record skipped: ${tradeRecordIdentifier}`);
            }
        }

        startIndex += batchSize;
    }
}

async function checkApi() {
    try {
        // Your API authentication logic
        marketData.auth(apiToken);
        tradeHistory.auth(apiToken);
        // Clear the arrays at the beginning of each run
        const cryptoLogsToUpsert = [];
        const cryptoToUpsert = [];
        const tradeDataToUpsert = [];

        // Fetch data from the marketData API
        const response = await marketData.multiData({
            assets: "bitcoin,litecoin,ethereum,tether,dogecoin,xrp,bnb,polygon,solana"
        });
        const cryptocurrencies = response.data.data;
        const records = [];

        for (const [name, cryptoData] of Object.entries(cryptocurrencies)) {
            const record = {
                name: name,
                market_cap: cryptoData.market_cap,
                liquidity: cryptoData.liquidity,
                price: cryptoData.price,
                volume: cryptoData.volume,
                volume_7d: cryptoData.volume_7d,
                is_listed: cryptoData.is_listed,
                price_change_24h: cryptoData.price_change_24h,
                updated_at: new Date().toISOString(),
            };

            records.push(record);
        }

        // Process trade data asynchronously using Promise.all
        await Promise.all(records.map(processTradeData));

        // Deduplicate tradeDataToUpsert
        const uniqueTradeData = new Set();
        const deduplicatedTradeData = [];
        const conflictingTradeData = []; // To capture conflicting trade data

        for (const tradeRecord of tradeDataToUpsert) {
            const tradeRecordIdentifier = `${tradeRecord.date}-${tradeRecord.hash}-${tradeRecord.value_usd}-${tradeRecord.token_amount}-${tradeRecord.token_price}-${tradeRecord.type}-${tradeRecord.blockchain}`;
            if (!uniqueTradeData.has(tradeRecordIdentifier)) {
                uniqueTradeData.add(tradeRecordIdentifier);
                deduplicatedTradeData.push(tradeRecord);
            } else {
                // Log the conflicting trade data
                console.log("Conflicting trade record:", tradeRecord);
                conflictingTradeData.push(tradeRecord);
            }
        }

        // Log the conflicting trade data
        console.log("Conflicting trade records:", conflictingTradeData);

        // Perform batch upserts for crypto_logs, crypto, and trades
        if (cryptoLogsToUpsert.length > 0) {
            const logResult = await supabase.from("crypto_logs").upsert(cryptoLogsToUpsert);
            console.log("Batch upsert successful into crypto_logs:", logResult.data);
        }

        if (cryptoToUpsert.length > 0) {
            const cryptoResult = await supabase
                .from("crypto")
                .upsert(cryptoToUpsert, {
                    onConflict: ["name"]
                })
                .select();
            console.log("Batch upsert successful into crypto");
        }

        if (deduplicatedTradeData.length > 0) { // Use the deduplicated data
            try {
                const {
                    data,
                    error
                } = await supabase
                    .from("trades")
                    .upsert(deduplicatedTradeData) // Use the deduplicated data
                    .select();
                console.log("Batch upsert successful into trades:", error);
            } catch (error) {
                if (error.code === '23505') {
                    console.warn(`Duplicate records skipped in trades: ${error.message}`);
                } else {
                    console.error("Error upserting into trades:", error);
                }
            }
        }
    } catch (error) {
        console.error("Error upserting:", error);
    }
}

let interval;

function startCheckApiInterval() {
    if (interval) {
        clearInterval(interval);
        interval = undefined;
    }

    if (connections.size > 0) {
        interval = setInterval(() => {
            checkApi();
        }, 3000);
        console.log('Interval has been updated to 3 seconds since there is an active connection.');

        // Call checkApi immediately after setting the new interval
        checkApi();
    } else {
        interval = setInterval(() => {
            checkApi();
        }, 300000);
        console.log('Interval has been updated to 5 minutes since there are no active connections.');

        // Call checkApi immediately after setting the new interval
        checkApi();
    }
}

wss.on('connection', (ws, request) => {
    connections.add(ws);

    ws.on('message', (message) => {
        broadcast('Connection open');
        const messageText = message.toString();
        if (messageText === 'startFetching') {
            if (connections.size > 0 && connections.size < 2) {
                startCheckApiInterval();
            }
        } else if (messageText.startsWith('ping:')) {
            const originalPingTimestamp = messageText.split(':')[1];
            const pongTimestamp = new Date().getTime();
            ws.send(`pong:${pongTimestamp}:${originalPingTimestamp}`);
        }
    });

    ws.on('close', () => {
        connections.delete(ws);
        broadcast('Connection closed');
        if (connections.size === 0) {
            startCheckApiInterval(); // Start or update the interval when a connection is closed and there are no connections left
        }
    });
});

const PORT = process.env.PORT || 4000;
server.listen(PORT, () => {
    console.log(`Node server listening on port ${PORT}`);
});
