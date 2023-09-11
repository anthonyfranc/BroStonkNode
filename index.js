const http = require('http');
const WebSocket = require('ws');
const server = http.createServer(); // Use HTTP server
const wss = new WebSocket.Server({ server, path: '/index-ws' });

const { createClient } = require("@supabase/supabase-js");
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

let isApiRunning = false; // Flag to track whether checkApi is already running

async function checkApi() {
  // Check if the API is already running, and if so, exit the function
  if (isApiRunning) {
    console.log('API is already running, skipping this execution.');
    return;
  }

  // Set the flag to true at the beginning
  isApiRunning = true;

  try {
    // Your API authentication logic
    marketData.auth(apiToken);
    tradeHistory.auth(apiToken);

    // Fetch data from the marketData API
    const response = await marketData.multiData({ assets: "bitcoin,litecoin,ethereum,tether,dogecoin,xrp,bnb,polygon,solana" });
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
    
      try {
        // Create an array to store records that need to be upserted into crypto_logs
        const cryptoLogsToUpsert = [];

        // Create an array to store records that need to be upserted into crypto
        const cryptoToUpsert = [];

        // Create an array to store trade data records that need to be upserted into trades
        const tradeDataToUpsert = [];

        // Initialize a cache object to store recent price data
        const priceCache = {};

        for (const record of records) {
          const cachedPrice = priceCache[record.name];

          if (cachedPrice !== undefined && cachedPrice === record.price) {
            // If the price is in the cache and matches the current price, no need to query the database
            console.log(`No price change for ${record.name} in crypto_logs (cached).`);
          } else {
            // Add the record to the array for upserting into crypto_logs
            cryptoLogsToUpsert.push(record);
          }

          // Update the cache with the new price, whether it was queried or not
          priceCache[record.name] = record.price;

          // Add the record to the array for upserting into crypto (regardless of changes)
          cryptoToUpsert.push(record);

          // Make the second API call for each asset name
          const tradeData = await tradeHistory.getTradeHistory({ asset: record.name, maxResults: '1' });

          // Modify the tradeData object to include the 'asset' column
          tradeData.data.data.forEach((trade) => {
            trade.asset = record.name;
          });

          // Add the trade data records to the array for upserting into trades
          tradeDataToUpsert.push(...tradeData.data.data);
        }

        // Perform batch upserts for crypto_logs, crypto, and trades
        if (cryptoLogsToUpsert.length > 0) {
          const logResult = await supabase.from("crypto_logs").upsert(cryptoLogsToUpsert);
          console.log("Batch upsert successful into crypto_logs:", logResult.data);
        }

        if (cryptoToUpsert.length > 0) {
          const cryptoResult = await supabase
            .from("crypto")
            .upsert(cryptoToUpsert, { onConflict: ["name"] })
            .select();
          console.log("Batch upsert successful into crypto");
        }

        if (tradeDataToUpsert.length > 0) {
          try {
            const tradeInsertResult = await supabase
              .from("trades")
              .upsert(tradeDataToUpsert);
            console.log("Batch upsert successful into trades:", tradeInsertResult.data);
          } catch (error) {
            if (error.code === '23505') {
              console.warn(`Duplicate records skipped in trades: ${error.message}`);
            } else {
              console.error("Error upserting into trades:", error);
            }
          }
        }
  // Your existing code for processing and upserting data...

  } catch (error) {
    console.error("Error upserting:", error);
  } finally {
    // Ensure the flag is reset even in case of an error
    isApiRunning = false;
  }
}
}

// Add this line to close the checkApi function properly
checkApi().catch((err) => console.error(err));

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
  const clientIP = request.headers['x-forwarded-for'];
  if (clientIP) {
    console.log(`New connection from IP: ${clientIP}`);
    startCheckApiInterval(); // Start or update the interval when a new connection is established
  } else {
    console.log('x-forwarded-for header not found in request.');
  }

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
