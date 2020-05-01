package main

import (
    "context"
	"encoding/json"
	"fmt"
    "io/ioutil"
	"os"
    "strconv"
    "sync/atomic"

	"cloud.google.com/go/pubsub"
	"github.com/gorilla/websocket"
)

// Initialize a connection using your API key
// You can generate an API key here: https://cryptowat.ch/account/api-access
// Paste your API key here:
var (
	APIKEY = os.Getenv("CRYPTOWATCH_API_KEY")
)

const (
	PROJECT_ID = "alpaca-trading-239601"
	TOPIC_ID   = "cryptowatch_stream"
)

func main() {
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "./credential.json")

	fmt.Println("starting main...")

	// parse args
	argsWithoutProg := os.Args[1:]
	exchange_id := argsWithoutProg[0]

    exchangesMapFile, err := os.Open("exchanges.json")
    if err != nil {
        fmt.Printf("%v\n", err)
        return
    }
    defer exchangesMapFile.Close()
    exchangesMapByteValue, _ := ioutil.ReadAll(exchangesMapFile)

    marketsMapFile, err := os.Open("markets.json")
    if err != nil {
        fmt.Printf("%v\n", err)
        return
    }
    defer marketsMapFile.Close()
    marketsMapByteValue, _ := ioutil.ReadAll(marketsMapFile)


	var id_to_symbol_exchanges map[string]string
	json.Unmarshal(exchangesMapByteValue, &id_to_symbol_exchanges)
	exchange, ok := id_to_symbol_exchanges[exchange_id] 
	if !ok {
		fmt.Printf("exchange id: %s is not found.\n", exchange_id)
		return
	}
	fmt.Println(exchange)

	var id_to_symbol_markets map[string]string
	json.Unmarshal(marketsMapByteValue, &id_to_symbol_markets)

	c, _, err := websocket.DefaultDialer.Dial("wss://stream.cryptowat.ch/connect?apikey="+APIKEY, nil)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	// pubsub init
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, PROJECT_ID)
	if err != nil {
		fmt.Errorf("pubsub.NewClient: %v", err)
		return
	}
	var totalErrors uint64
	topic := client.Topic(TOPIC_ID)

	// Read first message, which should be an authentication response
	_, message, err := c.ReadMessage()
	var authResult struct {
		AuthenticationResult struct {
			Status string `json:"status"`
		} `json:"authenticationResult"`
	}
	err = json.Unmarshal(message, &authResult)
	if err != nil {
		panic(err)
	}

	// Send a JSON payload to subscribe to a list of resources
	// Read more about resources here: https://docs.cryptowat.ch/websocket-api/data-subscriptions#resources
	resources := []string{
		fmt.Sprintf("exchanges:%s:ohlc", exchange_id),
	}
	subMessage := struct {
		Subscribe SubscribeRequest `json:"subscribe"`
	}{}
	// No map function in golang :-(
	for _, resource := range resources {
		subMessage.Subscribe.Subscriptions = append(subMessage.Subscribe.Subscriptions, Subscription{StreamSubscription: StreamSubscription{Resource: resource}})
	}
	msg, err := json.Marshal(subMessage)
	err = c.WriteMessage(websocket.TextMessage, msg)
	if err != nil {
		panic(err)
	}

	// Process incoming BTC/USD trades
	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			fmt.Println("Error reading from connection")
			fmt.Println(err.Error())
			return
		}

		var update Update
		err = json.Unmarshal(message, &update)
		if err != nil {
			panic(err)
		}

		market_symbol := strconv.FormatInt(int64(update.MarketUpdate.Market.MarketId), 10)
		if market_symbol == "0" {
			continue
		}

		symbol, ok  := id_to_symbol_markets[market_symbol]
		if !ok {
			fmt.Printf("market id: %d is not found. Update: %v\n", update.MarketUpdate.Market.MarketId, update)
			continue
		}

		for _, interval := range update.MarketUpdate.IntervalsUpdate.Intervals {
			if interval.PeriodName != "60" {
				continue
			}

		    msg, err := json.Marshal(interval.ToBar(exchange, symbol))
		    if err != nil {
				fmt.Printf("Failed to marshall json: %v\n", err)
		        continue
		    }
		    msg_str := string(msg) 

			fmt.Printf(
				"ohlc on market %d, interval: %v, msg: %v\n",
				update.MarketUpdate.Market.MarketId,
				interval,
				msg_str,
			)

			publish_result := topic.Publish(ctx, &pubsub.Message{
				Data: []byte(msg_str),
			})

			go func(res *pubsub.PublishResult) {
				// The Get method blocks until a server-generated ID or
				// an error is returned for the published message.
				_, err := res.Get(ctx)
				if err != nil {
					// Error handling code can be added here.
					fmt.Printf("Failed to publish: %v\n", err)
					atomic.AddUint64(&totalErrors, 1)
					return
				}
			}(publish_result)
		}
	}
}

// Helper types for JSON serialization

type Subscription struct {
	StreamSubscription `json:"streamSubscription"`
}

type StreamSubscription struct {
	Resource string `json:"resource"`
}

type SubscribeRequest struct {
	Subscriptions []Subscription `json:"subscriptions"`
}

type Update struct {
	MarketUpdate struct {
		Market struct {
			MarketId int `json:"marketId,string"`
		} `json:"market"`

		IntervalsUpdate struct {
			Intervals []Interval `json:"intervals"`
		} `json:"intervalsUpdate"`
	} `json:"marketUpdate"`
}

type Interval struct {
	Closetime  int    `json:"closetime,string"`
	PeriodName string `json:"periodName"`

	Ohlc struct {
		OpenStr  string `json:"openStr"`
		HighStr  string `json:"highStr"`
		LowStr   string `json:"lowStr"`
		CloseStr string `json:"closeStr"`
	} `json:"ohlc"`

	VolumeBaseStr  string `json:"volumeBaseStr"`
	VolumeQuoteStr string `json:"volumeQuoteStr"`
}

func marketIdToSymbol(i int) (string, error) {
	return "bar", nil
}

func (i Interval) ToBar(exchange string, symbol string) Bar {
	bar := Bar{exchange, symbol, i.Closetime, i.Ohlc.OpenStr, i.Ohlc.HighStr, i.Ohlc.LowStr, i.Ohlc.CloseStr, i.VolumeBaseStr, i.VolumeQuoteStr}
	return bar
}

type Bar struct {
	Exchange  string `json:"exchange"`
	Symbol    string `json:"symbol"`
	Closetime int    `json:"closetime,string"`

	OpenStr  string `json:"openStr"`
	HighStr  string `json:"highStr"`
	LowStr   string `json:"lowStr"`
	CloseStr string `json:"closeStr"`

	VolumeBaseStr  string `json:"volumeBaseStr"`
	VolumeQuoteStr string `json:"volumeQuoteStr"`
}
