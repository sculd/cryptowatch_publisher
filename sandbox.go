package main

import (
    //"context"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "os"
    //"strconv"
    //"sync/atomic"

    //"cloud.google.com/go/pubsub"
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
    /*
    ctx := context.Background()
    client, err := pubsub.NewClient(ctx, PROJECT_ID)
    if err != nil {
        fmt.Errorf("pubsub.NewClient: %v", err)
        return
    }
    var totalErrors uint64
    topic := client.Topic(TOPIC_ID)
    */

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
        fmt.Sprintf("exchanges:%s:book:deltas", exchange_id),
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

    for {
        _, message, err := c.ReadMessage()
        if err != nil {
            fmt.Println("Error reading from connection")
            fmt.Println(err.Error())
            return
        }

        fmt.Printf("%v", message)
        /*
        var update Update
        err = json.Unmarshal(message, &update)
        if err != nil {
            panic(err)
        }
        */

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

        OrderBookDeltaUpdate OrderBookDelta `json:"orderBookDeltaUpdate"`
    } `json:"marketUpdate"`
}

type SeqNum uint32

// OrderBookDelta represents an order book delta update, which is
// the minimum amount of data necessary to keep a local order book up to date.
// Since order book snapshots are throttled at 1 per minute, subscribing to
// the delta updates is the best way to keep an order book up to date.
type OrderBookDelta struct {
  // SeqNum is used to make sure deltas are processed in order.
  // See the SeqNum definition for more information.
  SeqNum SeqNum

  Bids OrderDeltas
  Asks OrderDeltas
}

// Empty returns whether OrderBookDelta doesn't contain any deltas for bids and
// asks.
func (delta OrderBookDelta) Empty() bool {
  return delta.Bids.Empty() && delta.Asks.Empty()
}

// OrderDeltas are used to update an order book, either by setting (adding)
// new PublicOrders, or removing orders at specific prices.
type OrderDeltas struct {
  // Set is a list of orders used to add or replace orders on an order book.
  // Each order in Set is guaranteed to be a different price. For each of them,
  // if the order at that price exists on the book, replace it. If an order
  // at that price does not exist, add it to the book.
  Set []PublicOrder

  // Remove is a list of prices. To apply to an order book, remove all orders
  // of that price from that book.
  Remove []decimal.Decimal
}

// Empty returns whether the OrderDeltas doesn't contain any deltas.
func (d OrderDeltas) Empty() bool {
  return len(d.Set) == 0 && len(d.Remove) == 0
}

/*
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
*/
