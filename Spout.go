package main

import (
	"encoding/json"
	"fmt"
	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
	"io/ioutil"
)

func main() {

	data, err := ioutil.ReadFile("./keys/twitter.json")
	if err != nil {
		panic(err)
	}

	auth := make(map[string]string)

	if err := json.Unmarshal(data, &auth); err != nil {
    panic(err)
	}

	config := oauth1.NewConfig(auth["consumerKey"], auth["consumerSecret"])
	token := oauth1.NewToken(auth["accessToken"], auth["accessSecret"])

	// http.Client will automatically authorize Requests
	httpClient := config.Client(oauth1.NoContext, token)

	// Twitter client
	client := twitter.NewClient(httpClient)

	params := &twitter.StreamFilterParams{
		Locations:     []string{"-74,40,-73,41"},
		StallWarnings: twitter.Bool(true),
	}
	stream, err := client.Streams.Filter(params)
  if(err != nil) {
    panic(err)
  }

	demux := twitter.NewSwitchDemux()
	demux.Tweet = func(tweet *twitter.Tweet) {
		fmt.Printf("%s: %s\n", tweet.User.Name, tweet.Text)
		fmt.Println(tweet.IDStr)
	}

	demux.HandleChan(stream.Messages)

}
