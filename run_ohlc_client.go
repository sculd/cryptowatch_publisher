package main

import (
	"log"
	"os"

	"code.cryptowat.ch/cw-sdk-go/client/websocket"
)

var (
	_API_KEY = os.Getenv("CRYPTOWATCH_API_KEY")
	_SECRET_KEY = os.Getenv("CRYPTOWATCH_API_SECRET")
)

func main() {
	client, err := websocket.NewStreamClient(&websocket.StreamClientParams{
		WSParams: &websocket.WSParams{
			APIKey:    _API_KEY,
			SecretKey: _SECRET_KEY,
		},

		/*
		Subscriptions: []*websocket.StreamSubscription{
			&websocket.StreamSubscription{
				Resource: "instruments:232:ohlc",
			},
		},
		//*/

		Subscriptions: []*websocket.StreamSubscription{
			&websocket.StreamSubscription{
				Resource: "markets:86:trades", // Trade feed for Kraken BTCEUR
			},
			&websocket.StreamSubscription{
				Resource: "markets:87:trades", // Trade feed for Kraken BTCEUR
			},
		},		
	})
	
	if err != nil {
		log.Fatal("%s", err)
	}

	c.OnSubscriptionResult(func(sr websocket.SubscriptionResult) {
		// Verify subscriptions
	})

	client.OnError(func(err error, disconnecting bool) {
		// Handle errors
	})

	client.OnTradesUpdate(func(m websocket.Market, tu websocket.TradesUpdate) {
		// Handle live trade data
		log.Printf(
			"message: %v",
			tu,
		)
	})

	// Set more handlers for market and pair data

client.Connect()
}
