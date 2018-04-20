// Copyright 2018 connor.fun. All rights reserved.
// Use of this source code is governed by a GNU-style
// liscense that can be found in the LICENSE file.

/*
* This file streams tweets from bounding boxes into a
*  Kinesis stream named "raw-tweets", using AWS and
*  Twitter credentials defined in enviroment variables.
*  The bounding boxes are read from "./locations.json".
 */

/*
* Required Enviroment Variables:
* TWITTER_CONSUMER_KEY = Consumer Key
* TWITTER_CONSUMER_SECRET = Consumer Secret
* TWITTER_ACCESS_KEY = Access Key
* TWITTER_ACCESS_SECRET = Access Secret
* AWS_ACCESS_KEY_ID = AWS Access Key
* AWS_SECRET_ACCESS_KEY = AWS Secret Key
* AWS_REGION = AWS Region
 */

package main

import (
	"encoding/json"
	"fmt"
	"github.com/CUBigDataClass/connor.fun-SectorGenerator/src"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

/*
* This contains all necessary data for any given location.
* Fields are subject to change.
 */
type rawTweet struct {
	ID         string `json:"ID"`
	Text       string `json:"text"`
	Region     string `json:"region"`
	RegionJSON string `json:"regionData"`
}

/*
* This will open streams for all defined regions, and output
*   rawTweet(s) into Kinesis.
 */
func main() {
	// Fetch tokens/keys from enviroment
	config := oauth1.NewConfig(os.Getenv("TWITTER_CONSUMER_KEY"),
		os.Getenv("TWITTER_CONSUMER_SECRET"))
	token := oauth1.NewToken(os.Getenv("TWITTER_ACCESS_KEY"),
		os.Getenv("TWITTER_ACCESS_SECRET"))

	// http.Client will automatically authorize Requests
	httpClient := config.Client(oauth1.NoContext, token)
	client := twitter.NewClient(httpClient)

	// Kafka Producer
	kini, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "54.68.207.33"})
	if err != nil {
		panic(err)
	}

	// Get locations from file
	locations := getLocations()
	for _, loc := range locations {
		// Create the bounding box string
		box := []string{fmt.Sprint(loc.East), fmt.Sprint(loc.South), fmt.Sprint(loc.West), fmt.Sprint(loc.North)}
		stringBox := strings.Join(box[:], ",")

		regionData, err := json.Marshal(&loc)

		if err != nil {
			panic(err)
		}

		// Open a stream for that location
		go openStream(stringBox, loc.Name, string(regionData), client, kini)
	}

	// Run until we are sent SIGINT (CTRL-C)
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println(<-ch)

}

func openStream(loc string, region string, regionData string, client *twitter.Client, kafkaProd *kafka.Producer) {
	demux := twitter.NewSwitchDemux()
	demux.Tweet = func(tweet *twitter.Tweet) {
		handleTweet(tweet, region, kafkaProd)
	}

	// Twitter client
	params := &twitter.StreamFilterParams{
		Locations:     []string{loc},
		StallWarnings: twitter.Bool(true),
	}

	// Setup streams per location
	stream, err := client.Streams.Filter(params)
	if err != nil {
		panic(err)
	}

	demux.HandleChan(stream.Messages)
	demux.All = func(message interface{}) {
		fmt.Println(message)
	}
}

// Tweet -> Kinesis
func handleTweet(tweet *twitter.Tweet, regionName string, kafkaProd *kafka.Producer) {
	tweetJSON, _ := json.Marshal(tweet)
	fmt.Println(regionName, tweet.Text)

	topic := "raw-tweets"

	kafkaProd.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(tweetJSON)}, nil)
	// Update info that a new tweet has gone through

}

/*
 * This reads from the JSON file to get all of the city information.
 * The creation of locations will occur without the knowledge of this file.
 */
func getLocations() []SectorGenerator.LocationData {
	gen := SectorGenerator.NewGenerator()

	data, err := ioutil.ReadFile("./locations.json")

	if err != nil {
		panic(err)
	}

	err = gen.ParseLocationDataJSON(data)
	if err != nil {
		panic(err)
	}

	return gen.GetLocationData()
}
