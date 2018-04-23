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
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
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
	kini, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}

	// Get locations from file
	locations := getLocations()

	var msg [25]uint64
	for i := range locations {
		msg[i] = 0
	}

	for i, loc := range locations {
		// Create the bounding box string
		box := []string{fmt.Sprint(loc.East), fmt.Sprint(loc.South), fmt.Sprint(loc.West), fmt.Sprint(loc.North)}
		stringBox := strings.Join(box[:], ",")

		regionData, err := json.Marshal(&loc)

		if err != nil {
			panic(err)
		}

		// Open a stream for that location
		go openStream(stringBox, loc.ID, string(regionData), client, kini, &msg[i])

		fmt.Println("Opening Stream for " + loc.ID)

		time.Sleep(30 * time.Second)
	}

	// Loop over counts from each channel, see if any have fallen by the wayside
	for {
		time.Sleep(30 * time.Second)

		fmt.Print("\033[H\033[2J")
		fmt.Println("In the last 30 seconds:")

		for i, loc := range locations {
			current := atomic.LoadUint64(&msg[i])
			fmt.Println(loc.ID + ":\t" + strconv.FormatUint(current, 10))
			atomic.StoreUint64(&msg[i], 0)

			if current == 0 {
				// Stop current channel, restart stream
			}
		}
	}

	// Run until we are sent SIGINT (CTRL-C)
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println(<-ch)

}

func openStream(loc string, region string, regionData string, client *twitter.Client, kafkaProd *kafka.Producer, channel *uint64) {
	demux := twitter.NewSwitchDemux()
	demux.Tweet = func(tweet *twitter.Tweet) {
		handleTweet(tweet, region, kafkaProd, channel)
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
func handleTweet(tweet *twitter.Tweet, regionName string, kafkaProd *kafka.Producer, channel *uint64) {
	tweetJSON, _ := json.Marshal(tweet)

	atomic.AddUint64(channel, 1)

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
