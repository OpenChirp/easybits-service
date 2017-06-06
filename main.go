// November 18, 2016
// Craig Hesling <craig@hesling.com>

/*
 * We need the following additions:
 * - Ability to subscribe or listen to updates to list of nodes requiring serialization
 */

// This is a serialization service for the OpenChirp framework
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"

	"encoding/json"

	"strconv"

	"fmt"

	"github.com/openchirp/framework"
	"github.com/openchirp/framework/rest"
)

/*
"serviceconfig": {
	"rxmap": ["temp,sint32,1", "humidity,uint32,2", "light,uint32,3", "pir,uint32,4", "mic,uint32,5", "accX,uint32,6", "accY,uint32,7", "accZ,uint32,8"],
	"txmap": ["duty,uint32,9"]
}
*/

type ServiceConfig struct {
	RxData []string `json:"rxmap"`
	TxData []string `json:"txmap"`
	// should add a TXBuffering time
}

const (
	deviceRxData  = "transducer/rawrx"
	deviceTxData  = "transducer/rawtx"
	rxConfigLabel = "rxconfig"
	txConfigLabel = "txconfig"
)

const (
	defaultFrameworkServer = "http://localhost"
	defaultServiceID       = ""
	defaultRefreshTime     = 10 // seconds
)

/* Options to be filled in by arguments */
var frameworkURI string
var user string
var pass string
var serviceID string
var refreshTime uint

/* HACK to allow us to use raw MQTT during conversion period */
var mqttQos int64

/* Setup argument flags and help prompt */
func init() {
	flag.StringVar(&serviceID, "serviceid", defaultServiceID, "Sets the service ID associated with this service instance")
	flag.StringVar(&frameworkURI, "framework", defaultFrameworkServer, "Sets the HTTP REST framework server")
	flag.StringVar(&user, "user", "", "Sets the framework username")
	flag.StringVar(&pass, "pass", "", "Sets the framework password")
	flag.UintVar(&refreshTime, "refresh", defaultRefreshTime, "Sets the service config refresh time in seconds")
}

func main() {
	var err error
	logstd := log.New(os.Stderr, "", log.Flags())
	logerr := log.New(os.Stderr, "", log.Flags())

	/* Parse Arguments */
	flag.Parse()

	/* Verify Arguments Given */
	if serviceID == defaultServiceID {
		logerr.Fatal("Must set serviceid")
	}

	logstd.Println("Starting")

	/* Setup master table of registered devices */
	var devices map[string]*Device = make(map[string]*Device)

	/* Get ServiceNode Information */
	host := rest.NewHost(frameworkURI)
	err = host.Login(user, pass)
	if err != nil {
		logerr.Fatalln("Failed to login to framework server:", err)
	}

	s, err := framework.StartService(host, serviceID)
	if err != nil {
		logerr.Fatalf("Failed to start framework service as id %s: %v\n", serviceID, err)
	}
	defer s.StopService()
	logstd.Println("Started framework service as id", serviceID)

	/* HACK to allow us to use raw MQTT during conversion period */
	mqttQos, err = strconv.ParseInt(s.GetProperty("MQTTQos"), 10, 8)
	if err != nil {
		logerr.Fatalln("Failed to get MQTTQos:", err)
	}

	events, err := s.StartDeviceUpdates()
	if err != nil {
		logerr.Fatalln("Failed to start device events channel:", err)
	}

	/* Get MQTT client to stay compatible with old framework */
	c := s.GetMQTTClient()

	logstd.Println("Fetching device configurations from framework")
	devConfig, err := s.FetchDeviceConfigs()
	if err != nil {
		logerr.Fatalln("Failed to fetch initial device configuration:", err)
	}

	/* Subscribe to Device Feeds */
	for _, dev := range devConfig {
		var config ServiceConfig

		m := dev.GetConfigMap()
		fmt.Println(m)

		rx, ok := m[rxConfigLabel]
		if !ok {
			logerr.Printf("Device %s did not specify an %s\n", dev.GetID(), rxConfigLabel)
			continue // ignore device
		}
		tx, ok := m[txConfigLabel]
		if !ok {
			logerr.Printf("Device %s did not specify an %s\n", dev.GetID(), txConfigLabel)
			continue // ignore device
		}
		err := json.Unmarshal([]byte(rx), &config.RxData)
		if err != nil {
			logerr.Printf("Error parsing %s for device with ID %s\n", rxConfigLabel, dev.GetID())
			continue // ignore device
		}
		err = json.Unmarshal([]byte(tx), &config.TxData)
		if err != nil {
			logerr.Printf("Error parsing %s for device with ID %s\n", txConfigLabel, dev.GetID())
			continue // ignore device
		}

		fulldev, err := host.RequestDeviceInfo(dev.GetID())
		if err != nil {
			logerr.Printf("Error fetching device info for device with ID %s\n", dev.GetID())
			continue // ignore device
		}

		/* Build Two-Way FieldID-Name Map */
		d := NewDevice(fulldev.NodeDescriptor)
		err = d.SetMapping(config)
		if err != nil {
			logerr.Printf("Error setting map for Device %s: %v\n", dev.GetID(), err)
			continue
		}

		logstd.Println("Registering device", dev.GetID())
		err = d.Register(c)
		if err != nil {
			logerr.Printf("Error registering Device %s: %v\n", dev.GetID(), err)
			continue
		}

		devices[fulldev.NodeDescriptor.ID] = d
	}

	logstd.Println("Subscribed to all device data streams")

	/* Wait for SIGINT */
	signals := make(chan os.Signal)
	signal.Notify(signals, os.Interrupt)

	for {
		select {
		case event := <-events:
			switch event.Type {
			case framework.DeviceUpdateTypeRem:
				d, ok := devices[event.Id]
				if !ok {
					logerr.Printf("Asked to remove device %s that was not registered", event.Id)
					continue
				}
				err = d.Deregister(c)
				if err != nil {
					logerr.Printf("Failed to deregister Device %s: %v\n", d.ID, err)
				}
				delete(devices, d.ID)

			case framework.DeviceUpdateTypeUpd:
				d, ok := devices[event.Id]
				if !ok {
					logerr.Printf("Asked to remove device %s that was not registered", event.Id)
					continue
				}
				err = d.Deregister(c)
				if err != nil {
					logerr.Printf("Failed to deregister Device %s: %v\n", d.ID, err)
				}
				delete(devices, d.ID)
				fallthrough
			case framework.DeviceUpdateTypeAdd:
				var dev rest.ServiceDeviceListItem
				dev.Id = event.Id
				dev.Config = event.Config

				var config ServiceConfig

				m := dev.GetConfigMap()
				fmt.Println(m)

				rx, ok := m[rxConfigLabel]
				if !ok {
					logerr.Printf("Device %s did not specify an %s\n", dev.GetID(), rxConfigLabel)
					continue // ignore device
				}
				tx, ok := m[txConfigLabel]
				if !ok {
					logerr.Printf("Device %s did not specify an %s\n", dev.GetID(), txConfigLabel)
					continue // ignore device
				}
				err := json.Unmarshal([]byte(rx), &config.RxData)
				if err != nil {
					logerr.Printf("Error parsing %s for device with ID %s\n", rxConfigLabel, dev.GetID())
					continue // ignore device
				}
				err = json.Unmarshal([]byte(tx), &config.TxData)
				if err != nil {
					logerr.Printf("Error parsing %s for device with ID %s\n", txConfigLabel, dev.GetID())
					continue // ignore device
				}

				fulldev, err := host.RequestDeviceInfo(dev.GetID())
				if err != nil {
					logerr.Printf("Error fetching device info for device with ID %s\n", dev.GetID())
					continue // ignore device
				}

				/* Build Two-Way FieldID-Name Map */
				d := NewDevice(fulldev.NodeDescriptor)
				err = d.SetMapping(config)
				if err != nil {
					logerr.Printf("Error setting map for Device %s: %v\n", dev.GetID(), err)
					continue
				}

				logstd.Println("Registering device", dev.GetID())
				err = d.Register(c)
				if err != nil {
					logerr.Printf("Error registering Device %s: %v\n", dev.GetID(), err)
					continue
				}

				devices[fulldev.NodeDescriptor.ID] = d
			}

		// case <-time.After(time.Duration(refreshTime) * time.Second):

		// 	newdevices := make(map[string]*Device)

		// 	log.Println("Time To Update Configs")
		// 	/* Get ServiceNode Information */
		// 	serviceinfo, err = framework.NewHost(frameworkServer).RequestServiceInfo(serviceID)
		// 	if err != nil {
		// 		log.Fatalln("Failed to fecth service info from framework server:", err.Error())
		// 	}
		// 	log.Println("Sucessfully retrieved ServiceNode information")

		// 	/* Update Device Configs */
		// 	for _, dev := range serviceinfo.DeviceNodes {
		// 		var config ServiceConfig

		// 		/* Decode Service Config from DeviceNode */
		// 		err := json.Unmarshal(dev.ServiceConfig, &config)
		// 		if err != nil {
		// 			log.Printf("Error parsing conf for Device %s (%s)", dev.ID, dev.Name)
		// 			continue // ignore device
		// 		}

		// 		/* Check if it is an existing device that can be updated */
		// 		if d, ok := devices[dev.NodeDescriptor.ID]; ok {
		// 			// simply update and add to newdevices
		// 			log.Printf("Updating mapping for device %s (%s)", dev.ID, dev.Name)
		// 			err = d.UpdateMapping(c, config)
		// 			if err != nil {
		// 				log.Printf("Error updating Device %s (%s): %v\n", dev.ID, dev.Name, err)
		// 				continue
		// 			}

		// 			newdevices[dev.NodeDescriptor.ID] = d
		// 			delete(devices, dev.NodeDescriptor.ID)
		// 			continue
		// 		}

		// 		/* Build a new Device */
		// 		log.Printf("Adding mapping for device %s (%s)\n", dev.ID, dev.Name)
		// 		d := NewDevice(dev.NodeDescriptor)
		// 		err = d.SetMapping(config)
		// 		if err != nil {
		// 			log.Printf("Error setting map for Device %s (%s): %v\n", dev.ID, dev.Name, err)
		// 			continue
		// 		}

		// 		err = d.Register(c)
		// 		if err != nil {
		// 			log.Printf("Error registering Device %s (%s): %v\n", dev.ID, dev.Name, err)
		// 			continue
		// 		}

		// 		newdevices[dev.NodeDescriptor.ID] = d
		// 	}

		// 	/* Deregister and delete old devices */
		// 	for _, d := range devices {
		// 		log.Printf("Removing mapping for device %s (%s)\n", d.ID, d.Name)
		// 		err = d.Deregister(c)
		// 		if err != nil {
		// 			log.Printf("Error deregistering Device %s: %v\n", d.ID, err)
		// 		}
		// 		delete(devices, d.ID)
		// 	}

		// 	/* Replace devices with newdevices map */
		// 	devices = newdevices

		case <-signals:
			goto shutdown
		}
	}
shutdown:

	log.Println("Shutting down")
}
