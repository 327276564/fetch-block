package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	mspmgmt "github.com/hyperledger/fabric/msp/mgmt"
	pb "github.com/hyperledger/fabric/protos/peer"
	"github.com/spf13/viper"
	"github.com/cendhu/fetch-block/src/events/parse"
)

var expName string // folder of this name is created where all data goes
var interestedEvents []*pb.Interest
var log_event, log_block, log_blockperf bool = false, false, false


type eventAdapter struct {
	block_channel chan *pb.Event_Block
}

//eventAdapter must implement GetInterestedEvents(), Recv() and Disconnect()
//which will be called by EventClient.
func (adapter *eventAdapter) GetInterestedEvents() ([]*pb.Interest, error) {
	return interestedEvents, nil
}

//We are intersted only in Event_Block events.
//Hence, we ignore the following events: Event_Register,
//Event_Unregister, Event_ChaincodeEvent, Event_Rejection
func (adapter *eventAdapter) Recv(msg *pb.Event) (bool, error) {
	if blockEvent, ok := msg.Event.(*pb.Event_Block); ok {
		adapter.block_channel <- blockEvent
		return true, nil
	}
	return false, fmt.Errorf("Not received block event. Event type is unknown: %v", msg)
}

func (adapter *eventAdapter) Disconnected(err error) {
	fmt.Print("Disconnected\n")
	os.Exit(1)
}

func startEventClient(peerEventAddress, domain, tlsCert string) *eventAdapter {
	var eventClient *EventsClient
	var err error
	adapter := &eventAdapter{block_channel: make(chan *pb.Event_Block, 100)}
	eventClient, _ = NewEventsClient(peerEventAddress, domain, tlsCert, 10, adapter)
	if err = eventClient.Start(); err != nil {
		fmt.Printf("could not start chat %s\n", err)
		eventClient.Stop()
		return nil
	}
	return adapter
}

func prettyprint(b []byte) ([]byte, error) {
	var out bytes.Buffer
	err := json.Indent(&out, b, "", "  ")
	return out.Bytes(), err
}

func processBlock(blockEvent *pb.Event_Block) parse.Block {
	return parse.ParseBlock(blockEvent.Block, uint64(len(blockEvent.Block.String())))
}

func main() {
	fmt.Println("Enter experiment name (creates folder of this name in working dir):")
	fmt.Scanln(&expName)
	os.MkdirAll(expName, os.ModePerm)

	viper.SetConfigType("yaml")
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	viper.SetEnvPrefix("FB")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
	if err := viper.ReadInConfig(); err != nil {
		fmt.Printf("Fatol error when read config file: err %s\n", err)
	}
	peerEventAddress := viper.GetString("peer.event-address")
	if peerEventAddress == "" {
		fmt.Printf("Event address of the peer should not be empty\n")
	}
	mspDir := viper.GetString("msp.path")
	if mspDir == "" {
		fmt.Printf("MSP config path should not be empty\n")
	}
	mspID := viper.GetString("msp.localMspId")
	if mspID == "" {
		fmt.Printf("MSP ID should not be empty\n")
	}

	fmt.Printf("Peer Event Address: %s\n", peerEventAddress)
	fmt.Printf("Local MSP Directory: %s\n", mspDir)
	fmt.Printf("Local MSP ID: %s\n", mspID)

	err := mspmgmt.LoadLocalMsp(mspDir, nil, mspID)
	if err != nil {
		fmt.Printf("Fatal error when setting up MSP from directory: err %s\n", err)
	}
	log_event = viper.GetBool("log.event")
	log_block = viper.GetBool("log.block")
	log_blockperf = viper.GetBool("log.blockperf")
	fmt.Printf("Log:\n")
	fmt.Printf("  Event: %v\n", log_event)
	fmt.Printf("  Block: %v\n", log_block)
	fmt.Printf("  Blockperf: %v\n", log_blockperf)

	event := &pb.Interest{EventType: pb.EventType_BLOCK}
	//for receiving blocks from specific channel, we can
	//pass channel id to pb.Interest as shown below:
	//event := &pb.Interest{EventType: EventType_BLOCK, ChainID: givenChannelID}
	//However, we are interested in receiving all blocks.
	interestedEvents = append(interestedEvents, event)
	fmt.Printf("Starting Client\n")
	adapter := startEventClient(peerEventAddress, "", "")
	if adapter == nil {
		fmt.Println("Error starting EventClient")
		return
	}

	fmt.Println("Listening for the event...\n")
	for {
		select {
		case blockEvent := <-adapter.block_channel:
			if log_event {
				fmt.Println("Got a block. Processing.")
			}
			go processBlock(blockEvent)
		}
	}
}
