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

	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/openchirp/framework"
)

type ServiceConfig struct {
	RxData []string `json:"rxmap"`
	TxData []string `json:"txmap"`
	// should add a TXBuffering time
}

const (
	mqttclientidprefix = "easybits"
	deviceRxData       = "rawrx"
	deviceTxData       = "rawtx"
)

const (
	defaultframeworkserver = "http://localhost"
	defaultmqttbroker      = "tcp://localhost:1883"
	defaultmqttuser        = ""
	defaultmqttpass        = ""
	defaultserviceid       = ""
	defaultmqttqos         = 0
	defaultRefreshTime     = 10 // seconds
)

/* Options to be filled in by arguments */
var serviceID string
var frameworkServer string
var refreshTime uint
var mqttBroker string
var mqttUser string
var mqttPass string
var mqttQos uint

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
	flag.StringVar(&serviceID, "service_id", defaultserviceid, "Sets the service ID associated with this service instance")
	flag.StringVar(&frameworkServer, "framework_server", defaultframeworkserver, "Sets the HTTP REST framework server")
	flag.UintVar(&refreshTime, "refresh", defaultRefreshTime, "Sets the service config refresh time in seconds")
	flag.StringVar(&mqttBroker, "mqtt_broker", defaultmqttbroker, "Sets the MQTT broker")
	flag.StringVar(&mqttUser, "mqtt_user", defaultmqttuser, "Sets the MQTT username")
	flag.StringVar(&mqttPass, "mqtt_pass", defaultmqttpass, "Sets the MQTT password")
	flag.UintVar(&mqttQos, "mqtt_qos", defaultmqttqos, "Sets the MQTT QOS to use when publishing and subscribing [0, 1, or 2]")
}

func main() {
	/* Parse Arguments */
	flag.Parse()

	/* Verify Arguments Given */
	if serviceID == "" {
		log.Fatal("Must set service_id")
	}

	log.Println("Starting")

	var serviceinfo framework.ServiceNode
	var devices map[string]*Device = make(map[string]*Device)

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
			log.Printf("Error parsing conf for Device %s (%s)", dev.ID, dev.Name)
			continue // ignore device
		}

		/* Build Two-Way FieldID-Name Map */
		d := NewDevice(dev.NodeDescriptor)
		err = d.SetMapping(config)
		if err != nil {
			log.Printf("Error setting map for Device %s (%s): %v\n", dev.ID, dev.Name, err)
			continue
		}

		err = d.Register(c)
		if err != nil {
			log.Printf("Error registering Device %s (%s): %v\n", dev.ID, dev.Name, err)
			continue
		}

		devices[dev.NodeDescriptor.ID] = d
	}

	log.Println("Subscribed to all device data streams")

	/* Wait for SIGINT */
	signals := make(chan os.Signal)
	signal.Notify(signals, os.Interrupt)

	for {
		select {
		case <-time.After(time.Duration(refreshTime) * time.Second):

			newdevices := make(map[string]*Device)

			log.Println("Time To Update Configs")
			/* Get ServiceNode Information */
			serviceinfo, err = framework.NewHost(frameworkServer).RequestServiceInfo(serviceID)
			if err != nil {
				log.Fatalln("Failed to fecth service info from framework server:", err.Error())
			}
			log.Println("Sucessfully retrieved ServiceNode information")

			/* Update Device Configs */
			for _, dev := range serviceinfo.DeviceNodes {
				var config ServiceConfig

				/* Decode Service Config from DeviceNode */
				err := json.Unmarshal(dev.ServiceConfig, &config)
				if err != nil {
					log.Printf("Error parsing conf for Device %s (%s)", dev.ID, dev.Name)
					continue // ignore device
				}

				/* Check if it is an existing device that can be updated */
				if d, ok := devices[dev.NodeDescriptor.ID]; ok {
					// simply update and add to newdevices
					log.Printf("Updating mapping for device %s (%s)", dev.ID, dev.Name)
					err = d.UpdateMapping(c, config)
					if err != nil {
						log.Printf("Error updating Device %s (%s): %v\n", dev.ID, dev.Name, err)
						continue
					}

					newdevices[dev.NodeDescriptor.ID] = d
					delete(devices, dev.NodeDescriptor.ID)
					continue
				}

				/* Build a new Device */
				log.Printf("Adding mapping for device %s (%s)\n", dev.ID, dev.Name)
				d := NewDevice(dev.NodeDescriptor)
				err = d.SetMapping(config)
				if err != nil {
					log.Printf("Error setting map for Device %s (%s): %v\n", dev.ID, dev.Name, err)
					continue
				}

				err = d.Register(c)
				if err != nil {
					log.Printf("Error registering Device %s (%s): %v\n", dev.ID, dev.Name, err)
					continue
				}

				newdevices[dev.NodeDescriptor.ID] = d
			}

			/* Deregister and delete old devices */
			for _, d := range devices {
				log.Printf("Removing mapping for device %s (%s)\n", d.ID, d.Name)
				err = d.Deregister(c)
				if err != nil {
					log.Printf("Error deregistering Device %s: %v\n", d.ID, err)
				}
				delete(devices, d.ID)
			}

			/* Replace devices with newdevices map */
			devices = newdevices
		case <-signals:
			goto shutdown
		}
	}
shutdown:

	log.Println("Shutting down")
}
