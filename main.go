// November 18, 2016
// Craig Hesling <craig@hesling.com>

/*
 * We need the following additions:
 * - Ability to subscribe or listen to updates to list of nodes requiring serialization
 */

// This is a serialization service for the OpenChirp framework
package main

import (
	"crypto/rand"
	"flag"
	"log"
	"math/big"
	"os"
	"os/signal"

	"encoding/json"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/openchirp/framework"
)

type ServiceConfig struct {
	Mapping []string `json:"mapping"`
}

const (
	deviceRxData = "rawrx"
	deviceTxData = "rawtx"
)

const (
	defaultframeworkserver = "http://localhost"
	mqttdefaultbroker      = "tcp://localhost:1883"
	mqttclientidprefix     = "happybitz"
)

/* Options to be filled in by arguments */
var frameworkServer string
var mqttBroker string
var mqttUser string
var mqttPass string
var mqttQos uint
var serviceID string

/* Generate a random client id for mqtt */
func genclientid() string {
	r, err := rand.Int(rand.Reader, new(big.Int).SetInt64(100000))
	if err != nil {
		log.Fatal("Couldn't generate a random number for MQTT client ID")
	}
	return mqttclientidprefix + r.String()
}

/* Setup argument flags and help prompt */
func init() {
	flag.StringVar(&frameworkServer, "framework_server", defaultframeworkserver, "Sets the HTTP REST framework server")
	flag.StringVar(&mqttBroker, "mqtt_broker", mqttdefaultbroker, "Sets the MQTT broker")
	flag.StringVar(&mqttUser, "mqtt_user", "", "Sets the MQTT username")
	flag.StringVar(&mqttPass, "mqtt_pass", "", "Sets the MQTT password")
	flag.UintVar(&mqttQos, "mqtt_qos", 0, "Sets the MQTT QOS to use when publishing and subscribing [0, 1, or 2]")
	flag.StringVar(&serviceID, "service_id", "", "Sets the service ID associated with this service instance")
}

func main() {
	/* Parse Arguments */
	flag.Parse()

	/* Verify Arguments Given */
	if serviceID == "" {
		log.Fatal("Must set service_id")
	}

	log.Println("Starting")

	/* Get ServiceNode Information */
	serviceinfo, err := framework.NewHost(frameworkServer).RequestServiceInfo(serviceID)
	if err != nil {
		log.Fatalln("Failed to fecth service info from framework server:", err.Error())
	}
	log.Println("Sucessfully retrieved ServiceNode information")

	/* Setup basic MQTT connection */
	opts := MQTT.NewClientOptions().AddBroker(mqttBroker)
	opts.SetClientID(genclientid())
	if mqttUser != "" {
		opts.SetUsername(mqttUser)
		opts.SetPassword(mqttPass)
	}

	/* Create and start a client using the above ClientOptions */
	c := MQTT.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal("Failed to connect:", token.Error())
	}
	defer c.Disconnect(250)
	log.Println("MQTT connection sucessful")

	/* Subscribe to Device Feeds */
	for _, dev := range serviceinfo.DeviceNodes {
		var config ServiceConfig

		/* Decode Service Config from DeviceNode */
		err := json.Unmarshal(dev.ServiceConfig, &config)
		if err != nil {
			log.Printf("Error - Device %s (%s) config could not be parsed", dev.ID, dev.Name)
			continue // ignore device
		}

		/* Build Two-Way FieldID-Name Map */
		nodedescriptor := framework.NodeDescriptor{
			ID:       dev.ID,
			Name:     dev.Name,
			MQTTRoot: dev.MQTTRoot,
		}
		d := NewDevice(nodedescriptor)
		err = d.SetMapping(config.Mapping)
		if err != nil {
			log.Printf("Error - Device %s (%s): %s\n", dev.ID, dev.Name, err.Error())
			continue
		}

		err = d.Register(c)
		if err != nil {
			log.Printf("Error - Device %s (%s): %s\n", dev.ID, dev.Name, err.Error())
			continue
		}

	}

	log.Println("Subscribed to all device data streams")

	/* Wait for SIGINT */
	signals := make(chan os.Signal)
	signal.Notify(signals, os.Interrupt)
	<-signals

	log.Println("Shutting down")
}
