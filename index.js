const http = require('http');
const WebSocket = require('ws');
const server = http.createServer();
const wss = new WebSocket.Server({server, path: '/index-ws' });

const { createClient } = require("@supabase/supabase-js");

const supabaseUrl = "https://jjtqvxvprcmblezstaks.supabase.co";
const supabaseKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImpqdHF2eHZwcmNtYmxlenN0YWtzIiwicm9sZSI6ImFub24iLCJpYXQiOjE2OTE3NjAxMjAsImV4cCI6MjAwNzMzNjEyMH0.glxbp12RNVsu6TaSqPGH_CUDs9AH7T1jNkfwLtz3ZQI";
const supabase = createClient(supabaseUrl, supabaseKey);

const sdk = require("api")("@mobula-api/v1.0#4cpc4om4lkxxs6mc");
sdk.auth("227cbd70-db72-4532-a285-bfaf74481af5"); // Set the authorization header using the auth method

let isWebSocketActive = false; // Flag to track WebSocket activity
let interval;
let noConnectionInterval;

function startCheckApiInterval() {
  if (!isWebSocketActive) {
    // Start the interval to run checkApi() every 10 seconds
    interval = setInterval(checkApi, 5000); // Change to 10 seconds
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

// Function to run checkApi() every 10 minutes when there are no active connections
function startNoConnectionInterval() {
  if (!isWebSocketActive) {
    noConnectionInterval = setInterval(checkApi, 600000); // 10 minutes interval
  }
}

function stopNoConnectionInterval() {
  clearInterval(noConnectionInterval);
}

function checkApi() {
  sdk
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
        for (const record of records) {
          // Query the most recent "price" for the cryptocurrency based on the "id" column
          const recentPriceRecord = await supabase
            .from("crypto_logs")
            .select("price")
            .eq("name", record.name)
            .order("id", { ascending: false }) // Order by 'id' in descending order
            .limit(1)
            .single();

          if (
            !recentPriceRecord || // If there's no recent record
            recentPriceRecord.data === null || // If the recent record has no data
            recentPriceRecord.data.price !== record.price // If the new price is different from the recent price
          ) {
            // Insert the data into the "crypto_logs" table
            const logResult = await supabase.from("crypto_logs").upsert([record]);

            if (logResult.error) {
              console.error("Error upserting into crypto_logs:", logResult.error);
            } else {
              console.log("Upsert successful into crypto_logs:", logResult.data);
            }
          } else {
            console.log(`No price change for ${record.name} in crypto_logs.`);
          }

          // Insert the data into the "crypto" table regardless of changes
          const cryptoResult = await supabase
            .from("crypto")
            .upsert([record], { onConflict: ["name"] })
            .select();

          if (cryptoResult.error) {
            console.error("Error upserting into crypto:", cryptoResult.error);
          } else {
            console.log("Upsert successful into crypto:", cryptoResult.data);
          }
        }
      } catch (error) {
        console.error("Error upserting:", error);
      }
    })
    .catch((err) => console.error(err));
}


const connections = new Set(); // Set to track WebSocket connections

wss.on('connection', (ws) => {
  connections.add(ws); // Add the new connection to the set

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

wss.on('close', () => {
  connections.delete(ws); // Remove the closed connection from the set

  // Check if there are still other active connections
  if (connections.size === 0) {
    isWebSocketActive = false; // Set WebSocket as inactive
    startNoConnectionInterval(); // Start the no connection interval when there are no active connections
  }
});


const PORT = process.env.PORT || 4000;
server.listen(PORT, () => {
  console.log(`Node server listening on port ${PORT}`);
});
