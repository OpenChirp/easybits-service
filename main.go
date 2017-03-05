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
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"

	"encoding/json"

	"strings"

	"strconv"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/linux4life798/dproto"
	"github.com/openchirp/framework"
)

const mappingSeparator = ","

type ServiceConfig struct {
	Mapping []string `json:"mapping"`
}

var typeName2ProtoType = map[string]descriptor.FieldDescriptorProto_Type{
	"int32":  descriptor.FieldDescriptorProto_TYPE_INT32,
	"uint32": descriptor.FieldDescriptorProto_TYPE_UINT32,
	"sint32": descriptor.FieldDescriptorProto_TYPE_SINT32,
	"bool":   descriptor.FieldDescriptorProto_TYPE_BOOL,
}

type Device struct {
	num2name map[uint32]string
	name2num map[string]uint32
	fieldMap *dproto.ProtoFieldMap
}

func NewDevice(mapping []string) *Device {
	d := new(Device)
	d.num2name = make(map[uint32]string, len(mapping))
	d.name2num = make(map[string]uint32, len(mapping))
	d.fieldMap = dproto.NewProtoFieldMap()

	for _, m := range mapping {
		parts := strings.Split(m, mappingSeparator)
		if len(parts) != 3 {
			return nil
		}
		fname := parts[0]
		ftype, ok := typeName2ProtoType[parts[1]]
		if !ok {
			return nil
		}
		fnum, err := strconv.ParseUint(parts[2], 10, 32)
		if err != nil {
			return nil
		}
		d.num2name[uint32(fnum)] = fname
		d.name2num[fname] = uint32(fnum)
		d.fieldMap.Add(dproto.FieldNum(fnum), ftype)
	}

	return d
}

func (d *Device) GetFieldName(num uint32) (string, bool) {
	name, ok := d.num2name[num]
	return name, ok
}

func (d *Device) GetFieldNum(name string) (uint32, bool) {
	num, ok := d.name2num[name]
	return num, ok
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
		var d *Device

		/* Decode Service Config from DeviceNode */
		err := json.Unmarshal(dev.ServiceConfig, &config)
		if err != nil {
			log.Printf("Error - Device %s (%s) config could not be parsed", dev.ID, dev.Name)
			continue // ignore device
		}

		/* Build Two-Way FieldID-Name Map */
		// fmap = NewFieldMap(config.Mapping)
		d = NewDevice(config.Mapping)
		if d == nil {
			log.Printf("Error - Device %s (%s) config.Mapping could not be parsed", dev.ID, dev.Name)
			continue
		}

		/* Subscribe to Device's rawrx Data Stream */
		token := c.Subscribe(dev.MQTTRoot+"/"+deviceRxData, byte(mqttQos), func(c MQTT.Client, m MQTT.Message) {
			// miovalue := &mio.MIOValue{}

			/* Decode base64 */
			data, err := base64.StdEncoding.DecodeString(string(m.Payload()))
			if err != nil {
				// log error and proceed to next packet
				log.Println("Error - Decoding base64:", err)
				return
			}

			/* Decode Protobuf */
			fields, err := d.fieldMap.DecodeBuffer(data)
			if err != nil {
				log.Println("Error while decoding rx buffer")
			}

			for _, field := range fields {
				/* Resolve Field Mapping */
				fieldname, ok := d.GetFieldName(uint32(field.Field))
				if !ok {
					// if no name specified, just ignore it
					continue
				}
				// /* Publish Data Named Field */
				topic := dev.MQTTRoot + "/" + fieldname
				message := fmt.Sprint(field.Value)
				c.Publish(topic, byte(mqttQos), false, message)
				log.Println("Published", string(message), "to", topic)
			}
		})
		if token.Wait(); token.Error() != nil {
			log.Println("Failed to subscribe to", dev.MQTTRoot+"/"+deviceRxData)
			continue
		}
		log.Println("Subscribed to", dev.MQTTRoot+"/"+deviceRxData)
	}
	log.Println("Subscribed to all device data streams")

	/* Wait for SIGINT */
	signals := make(chan os.Signal)
	signal.Notify(signals, os.Interrupt)
	<-signals

	log.Println("Shutting down")
}
