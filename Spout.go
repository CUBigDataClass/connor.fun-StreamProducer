package main

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
	"io/ioutil"
)

func main() {

	twit, err := ioutil.ReadFile("./keys/twitter.json")
	if err != nil {
		panic(err)
	}

	twitterAuth := make(map[string]string)

	if err := json.Unmarshal(twit, &twitterAuth); err != nil {
		panic(err)
	}

	config := oauth1.NewConfig(twitterAuth["consumerKey"], twitterAuth["consumerSecret"])
	token := oauth1.NewToken(twitterAuth["accessToken"], twitterAuth["accessSecret"])

	// http.Client will automatically authorize Requests
	httpClient := config.Client(oauth1.NoContext, token)

	// Twitter client
	client := twitter.NewClient(httpClient)

	params := &twitter.StreamFilterParams{
		Locations:     []string{"-74,40,-73,41"},
		StallWarnings: twitter.Bool(true),
	}

	sess := session.Must(session.NewSession()) // MUST BE RUN IN EC2 or WITH LOCAL 'exports' setup
	svc := kinesis.New(sess)

	stream, err := client.Streams.Filter(params)
	if err != nil {
		panic(err)
	}

	partitionKey := "1"
	streamName := "raw-tweets"
	streamInput := kinesis.PutRecordInput{Data: []byte(""), PartitionKey: &partitionKey, StreamName: &streamName}

	demux := twitter.NewSwitchDemux()
	demux.Tweet = func(tweet *twitter.Tweet) {
		fmt.Printf("%s: %s\n", tweet.User.Name, tweet.Text)
		fmt.Println(tweet.IDStr)

		streamInput.Data = []byte(tweet.Text)
		responce, err := svc.PutRecord(&streamInput)
		fmt.Println(responce)
		if err != nil {
			panic(err)
		}
	}

	demux.HandleChan(stream.Messages)

}
