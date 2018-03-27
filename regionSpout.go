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
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
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
type locData struct {
	Name  string `json:"name"`
	South string `json:"south"`
	West  string `json:"west"`
	North string `json:"north"`
	East  string `json:"east"`
}

type rawTweet struct {
	ID     string `json:"ID"`
	Text   string `json:"text"`
	Region string `json:"region"`
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

	// AWS Kinesis session
	sess := session.Must(session.NewSession())
	kini := kinesis.New(sess)

	// Get locations from file
	locations := getLocations()
	for _, loc := range locations {
		// Create the bounding box string
		box := []string{loc.East, loc.South, loc.West, loc.North}
		stringBox := strings.Join(box[:], ",")

		// Open a stream for that location
		go openStream(stringBox, loc.Name, client, kini)
	}

	// Run until we are sent SIGINT (CTRL-C)
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println(<-ch)

}

func openStream(loc string, region string, client *twitter.Client, kini *kinesis.Kinesis) {
	demux := twitter.NewSwitchDemux()
	demux.Tweet = func(tweet *twitter.Tweet) {
		handleTweet(tweet, region, kini)
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
}

// Tweet -> Kinesis
func handleTweet(tweet *twitter.Tweet, regionName string, kini *kinesis.Kinesis) {
	// Make a new rawTweet
	kiniData, err := json.Marshal(rawTweet{
		ID:     tweet.IDStr,
		Text:   tweet.Text,
		Region: regionName})
	if err != nil {
		panic(err)
	}

	// Kinesis params
	partitionKey := "1"
	streamName := "raw-tweets"
	streamInput := kinesis.PutRecordInput{
		Data:         kiniData,
		PartitionKey: &partitionKey,
		StreamName:   &streamName}

	_, kerr := kini.PutRecord(&streamInput)

	if kerr != nil {
		panic(kerr)
	}

}

/*
 * This reads from the JSON file to get all of the city information.
 * The creation of locations will occur without the knowledge of this file.
 */
func getLocations() []locData {
	raw, err := ioutil.ReadFile("./locations.json")
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	var locations []locData
	json.Unmarshal(raw, &locations)
	return locations
}
