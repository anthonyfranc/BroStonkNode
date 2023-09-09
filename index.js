const http = require('http');
const WebSocket = require('ws');
const server = http.createServer();
const wss = new WebSocket.Server({ server, path: '/index-ws' });

const { createClient } = require("@supabase/supabase-js");

const supabaseUrl = "https://jjtqvxvprcmblezstaks.supabase.co";
const supabaseKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImpqdHF2eHZwcmNtYmxlenN0YWtzIiwicm9sZSI6ImFub24iLCJpYXQiOjE2OTE3NjAxMjAsImV4cCI6MjAwNzMzNjEyMH0.glxbp12RNVsu6TaSqPGH_CUDs9AH7T1jNkfwLtz3ZQI";
const supabase = createClient(supabaseUrl, supabaseKey);

const apiToken = "227cbd70-db72-4532-a285-bfaf74481af5";
const marketData = require("api")("@mobula-api/v1.0#4cpc4om4lkxxs6mc");
const tradeHistory = require("api")("@mobula-api/v1.0#1y6qv6aclmauztal");

marketData.auth(apiToken);
tradeHistory.auth(apiToken);


let isWebSocketActive = false; // Flag to track WebSocket activity
let interval;
let noConnectionInterval;

const connections = new Set(); // Set to track WebSocket connections

function broadcast(message) {
  // Send a message to all connected WebSocket clients
  connections.forEach((ws) => {
    ws.send(message);
  });
}

// Function to log the number of active connections
function logActiveConnections() {
  console.log(`Active WebSocket connections: ${connections.size}`);
}

function startCheckApiInterval() {
  if (!isWebSocketActive) {
    // Start the interval to run checkApi() every 10 seconds
    interval = setInterval(checkApi, 10000); // Change to 10 seconds
    isWebSocketActive = true;
  }
}

function stopCheckApiInterval() {
  if (isWebSocketActive) {
    // Stop the interval
    clearInterval(interval);
    isWebSocketActive = false;
  }
}

// Function to run checkApi() every 60 seconds when there are no active connections
function startNoConnectionInterval() {
  if (!isWebSocketActive && !noConnectionInterval) {
    console.log('No connections, running to keep data fresh.');
    checkApi(); // Run checkApi immediately when there are no connections

    // Then set the interval to run checkApi every 5 minutes
    noConnectionInterval = setInterval(() => {
      if (!isWebSocketActive) {
        console.log('Running to keep data fresh.');
        checkApi();
      } else {
        console.log('WebSocket connection active, stopping no connection interval.');
        stopNoConnectionInterval(); // Stop the interval when WebSocket connection is active
      }
    }, 300000); // 5 minutes interval
  }
}

function stopNoConnectionInterval() {
  clearInterval(noConnectionInterval);
}

async function checkApi() {
  marketData
    .multiData({ assets: "bitcoin,litecoin,ethereum,tether,dogecoin,xrp,bnb,polygon,solana" })
    .then(async (response) => {
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
      } catch (error) {
        console.error("Error upserting:", error);
      }
    })
    .catch((err) => console.error(err));
}

startNoConnectionInterval(); //will run once when the server starts, and it will start the interval if there are no initial connections. It will also continue to work as expected when connections are established and closed.

wss.on('connection', (ws, request) => {
  connections.add(ws); // Add the new connection to the set
  const clientIP = request.headers['x-forwarded-for']; // Use x-forwarded-for header
  if (clientIP) {
    console.log(`New connection from IP: ${clientIP}`);
  } else {
    console.log('x-forwarded-for header not found in request.');
  }

  logActiveConnections(); // Log the number of active connections

  ws.on('message', (message) => {
    const messageText = message.toString();
    if (messageText === 'startFetching') {
      startCheckApiInterval(); // Start the interval only for the first connection
      stopNoConnectionInterval(); // Stop the no connection interval when there is an active connection
    } else if (messageText.startsWith('ping:')) {
      const originalPingTimestamp = messageText.split(':')[1];
      const pongTimestamp = new Date().getTime();
      ws.send(`pong:${pongTimestamp}:${originalPingTimestamp}`);
    }
  });

  ws.on('close', () => {
    connections.delete(ws); // Remove the closed connection from the set

    logActiveConnections(); // Log the number of active connections

    // Check if there are still other active connections
    if (connections.size === 0) {
      isWebSocketActive = false; // Set WebSocket as inactive
      stopCheckApiInterval(); // Stop the checkApi interval when there are no active connections
      startNoConnectionInterval(); // Start the no connection interval when there are no active connections
    }
  });
});


const PORT = process.env.PORT || 4000;
server.listen(PORT, () => {
  console.log(`Node server listening on port ${PORT}`);
});
