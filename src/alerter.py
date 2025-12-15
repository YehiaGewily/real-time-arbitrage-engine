import json
import requests
from kafka import KafkaConsumer

# Config
KAFKA_TOPIC = "arbitrage-alerts"
# Note: Using list format for bootstrap_servers is robust, but string works too if single broker
BOOTSTRAP_SERVERS = ["kafka:29092"] 
WEBHOOK_URL = "https://discord.com/api/webhooks/1449852580959227935/fASNYf9Jcxh5teJpYhEPiI38Mq3V_PszMQwzbT0j8ZYmw6vJ-xHeNdmMlGPVl7p1QIrE" # <--- REPLACE THIS WITH YOUR ACTUAL DISCORD WEBHOOK URL
FEE_RATE = 0.001 # 0.1% per trade (Binance Taker / Institutional Tier)

def main():
    print("Starting Alerter Service...")
    
    # Initialize Consumer
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id='alerter_service',
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        return

    print(f"Listening on {KAFKA_TOPIC}...")

    # Event Loop
    for message in consumer:
        data = message.value
        
        # Extract fields
        symbol = data.get("symbol", "BTC")
        spread_diff = data.get("spread_diff", 0.0)
        coinbase_price = data.get("coinbase_price", 0.0)
        binance_price = data.get("binance_price", 0.0)
        
        # Threshold Logic
        if spread_diff > 20: # Keep generic threshold for visibility
            # Determine direction (Cheaper one is the Buy)
            if coinbase_price > binance_price:
                direction = "Buy: Binance / Sell: Coinbase"
            else:
                direction = "Buy: Coinbase / Sell: Binance"
                
            # Calculate Implications (assuming 1 unit)
            # Total Cost involved = Price 1 + Price 2
            total_trade_volume = coinbase_price + binance_price
            est_fees = total_trade_volume * FEE_RATE
            net_profit = spread_diff - est_fees
                
            msg_content = (
                f"🚨 **Arbitrage Found!** \n"
                f"Symbol: {symbol} \n"
                f"G. Spread: ${spread_diff:.2f} \n"
                f"Est. Fees (0.1%): -${est_fees:.2f} \n"
                f"**Net Profit:** ${net_profit:.2f} \n"
                f"{direction}"
            )
            
            # Send to Discord
            try:
                response = requests.post(WEBHOOK_URL, json={"content": msg_content})
                # 204 No Content is success for Discord Webhooks
                if response.status_code in [200, 204]:
                    print("Alert sent")
                else:
                    print(f"Failed to send alert. Status: {response.status_code}")
            except Exception as e:
                print(f"Error sending alert to Discord: {e}")

if __name__ == "__main__":
    main()
