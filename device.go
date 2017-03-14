package main

import (
	"encoding/base64"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	"errors"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/linux4life798/dproto"
	"github.com/openchirp/framework"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

// ErrDeviceRegistered is emitted when a change is attmpted while the device is
// still registered
var ErrDeviceRegistered = errors.New("Error the device is currently registered")

// ErrDeviceNotRegistered is emitted when a change is attmpted while the device
// is not currently registered
var ErrDeviceNotRegistered = errors.New("Error the device is not currently registered")

// ErrParseMapping is emitted when there was an error parsing the mapper config
var ErrParseMapping = errors.New("Error parsing mapper config")

var ErrRegistrationFailed = errors.New("Error registration or deregistration failed")

const mappingSeparator = ","

// parsedMapping holds the intermediate parsed parts of a config mapping
type parsedMapping struct {
	// fname if the field's name, which is the mqtt sub topic
	fname string
	// ftype is the field's protobuf type
	ftype descriptor.FieldDescriptorProto_Type
	// fnum is the field's protobuf enumeration
	fnum uint32
}

func parseMapping(mapping string) (parsedMapping, error) {
	pm := parsedMapping{}

	/* Split the string into the three parameters */
	parts := strings.Split(mapping, mappingSeparator)

	/* Verify there are 3 parts */
	if len(parts) != 3 {
		return pm, ErrParseMapping
	}

	/* Get the three parameters of the mapping */

	// Param 1 is the Field Name
	fname := parts[0]
	// Param 2 is the Field Protobuf Type
	ftype, ok := dproto.ParseProtobufType(parts[1])
	if !ok {
		return pm, ErrParseMapping
	}
	// Param 3 is the Field Number
	fnum, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		return pm, ErrParseMapping
	}

	pm.fname = fname
	pm.fnum = uint32(fnum)
	pm.ftype = ftype
	return pm, nil
}

// Device holds all configuration and lokoujp information about a device
// that requests our service
type Device struct {
	lock         sync.RWMutex
	isregistered bool
	framework.NodeDescriptor
	// node     framework.NodeDescriptor
	mapping  ServiceConfig // saved to compare for changes later
	num2name map[uint32]string
	name2num map[string]uint32
	fieldMap *dproto.ProtoFieldMap
}

// NewDevice creates a new initialized Device
func NewDevice(node framework.NodeDescriptor) *Device {
	return &Device{NodeDescriptor: node}
}

// isMappingEqual return true is the given ServiceConfig matches
// the one saved in the device. This is used to check if the device
// needs to be updated.
func (d *Device) isMappingEqual(mapping ServiceConfig) bool {
	// Recall len( []string(nil) ) == 0

	if len(d.mapping.RxData) != len(mapping.RxData) {
		return false
	}
	if len(d.mapping.TxData) != len(mapping.TxData) {
		return false
	}

	for i, v := range d.mapping.RxData {
		if v != mapping.RxData[i] {
			return false
		}
	}

	for i, v := range d.mapping.TxData {
		if v != mapping.TxData[i] {
			return false
		}
	}
	return true
}

// setMapping sets a device's mappings from a service config
func (d *Device) setMapping(mapping ServiceConfig) error {
	/* Copy Service Config mapping for later comparison */
	d.mapping = mapping
	d.mapping.RxData = make([]string, len(mapping.RxData))
	d.mapping.TxData = make([]string, len(mapping.TxData))
	copy(d.mapping.RxData, mapping.RxData)
	copy(d.mapping.TxData, mapping.TxData)

	d.num2name = make(map[uint32]string, len(mapping.RxData)+len(mapping.TxData))
	d.name2num = make(map[string]uint32, len(mapping.RxData)+len(mapping.TxData))
	d.fieldMap = dproto.NewProtoFieldMap()

	// Add all associations
	for _, m := range d.mapping.RxData {
		pm, err := parseMapping(m)
		if err != nil {
			return err
		}

		/* Add the association  */
		d.num2name[pm.fnum] = pm.fname
		d.name2num[pm.fname] = pm.fnum
		d.fieldMap.Add(dproto.FieldNum(pm.fnum), pm.ftype)
	}
	for _, m := range d.mapping.TxData {
		pm, err := parseMapping(m)
		if err != nil {
			return err
		}

		/* Add the association  */
		d.num2name[pm.fnum] = pm.fname
		d.name2num[pm.fname] = pm.fnum
		d.fieldMap.Add(dproto.FieldNum(pm.fnum), pm.ftype)
	}
	return nil
}

// deregister unsubscribes all device topics with the MQTT broker
func (d *Device) deregister(c MQTT.Client) error {
	if !d.isregistered {
		return ErrDeviceNotRegistered
	}

	/* Unsubscribe from Device's rawrx Data Stream */
	token := c.Unsubscribe(d.MQTTRoot + "/" + deviceRxData)
	if token.Wait(); token.Error() != nil {
		return ErrRegistrationFailed
	}
	log.Println("Unsubscribed from", d.MQTTRoot+"/"+deviceRxData)

	d.isregistered = false

	return nil
}

// register sets up all subscriptions with MQTT broker for the device
func (d *Device) register(c MQTT.Client) error {

	if d.isregistered {
		return ErrDeviceRegistered
	}

	/* Subscribe to Device's rawrx Data Stream */
	token := c.Subscribe(d.MQTTRoot+"/"+deviceRxData, byte(mqttQos), func(c MQTT.Client, m MQTT.Message) {

		d.lock.RLock()
		defer d.lock.RUnlock()

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
			/* Publish Data Named Field */
			topic := d.MQTTRoot + "/" + fieldname
			message := fmt.Sprint(field.Value)
			c.Publish(topic, byte(mqttQos), false, message)
			log.Println("Published", string(message), "to", topic)
		}

	})
	if token.Wait(); token.Error() != nil {
		return ErrRegistrationFailed
	}
	log.Println("Subscribed to", d.MQTTRoot+"/"+deviceRxData)

	/* Subscribe to all device's TX topics */
	for _, m := range d.mapping.TxData {
		pm, err := parseMapping(m) // last reference to m should be here
		if err != nil {
			return err
		}

		topic := d.MQTTRoot + "/" + pm.fname

		/* Subscribe to Device's txdata streams */
		token := c.Subscribe(topic, byte(mqttQos), func(c MQTT.Client, m MQTT.Message) {

			d.lock.RLock()
			defer d.lock.RUnlock()

			fnum, ok := d.GetFieldNum(pm.fname)
			if !ok {
				// log error and ignore publication
				log.Println("Error - Looking up field number for " + pm.fname + " for device " + d.ID)
				return
			}

			typ, ok := d.fieldMap.Get(dproto.FieldNum(fnum))
			if !ok {
				// log error and ignore publication
				log.Println("Error - Looking up field type for " + pm.fname + " for device " + d.ID)
				return
			}
			value, err := dproto.ParseAs(string(m.Payload()), typ, 0)
			if err != nil {
				// log error and ignore publication
				log.Println("Error - Parsing published value \""+string(m.Payload())+"\" for "+pm.fname+" for device "+d.ID+" as a "+typ.String()+":", err)
				return
			}
			values := []dproto.FieldValue{dproto.FieldValue{Field: dproto.FieldNum(fnum), Value: value}}
			buf, err := d.fieldMap.EncodeBuffer(values)
			if err != nil {
				// log error and ignore publication
				log.Println("Error - Encoding field", pm.fname, "with", string(m.Payload()), "for device", d.ID)
				return
			}

			// convert to base64 for rawtx
			data := base64.StdEncoding.EncodeToString(buf)
			c.Publish(d.MQTTRoot+"/"+deviceTxData, byte(mqttQos), false, data)
			log.Println("Published", data, "to", topic)

		})
		if token.Wait(); token.Error() != nil {
			return ErrRegistrationFailed
		}
		log.Println("Subscribed to", d.MQTTRoot+"/"+pm.fname)
	}

	d.isregistered = true

	return nil
}

// IsMappingEqual return true is the given ServiceConfig matches
// the one saved in the device. This is used to check if the device
// needs to be updated.
func (d *Device) IsMappingEqual(mapping ServiceConfig) bool {
	d.lock.RLock()
	defer d.lock.RUnlock()

	return d.isMappingEqual(mapping)
}

// SetMapping sets a device's mappings from a service config
func (d *Device) SetMapping(mapping ServiceConfig) error {

	log.Printf("SetMapping: %v\n", mapping)

	d.lock.Lock()
	defer d.lock.Unlock()

	if d.isregistered {
		return ErrDeviceRegistered
	}
	return d.setMapping(mapping)
}

func (d *Device) UpdateMapping(c MQTT.Client, mapping ServiceConfig) error {

	d.lock.Lock()
	defer d.lock.Unlock()

	if !d.isMappingEqual(mapping) {
		// if not registered, just update mapping
		if !d.isregistered {
			err := d.setMapping(mapping)
			return err
		}

		// deregister
		err := d.deregister(c)
		if err != nil {
			return err
		}

		// change config
		err = d.setMapping(mapping)
		if err != nil {
			return err
		}

		// re-register
		err = d.register(c)
		if err != nil {
			return err
		}
		log.Println("Update needed")
	} else {
		log.Println("Update not needed")
	}
	return nil
}

// Note: Not thread safe - call inside safe region
func (d *Device) GetFieldName(num uint32) (string, bool) {
	name, ok := d.num2name[num]
	return name, ok
}

// Note: Not thread safe - call inside safe region
func (d *Device) GetFieldNum(name string) (uint32, bool) {
	num, ok := d.name2num[name]
	return num, ok
}

// Deregister unsubscribes all device topics with the MQTT broker
func (d *Device) Deregister(c MQTT.Client) error {

	d.lock.Lock()
	defer d.lock.Unlock()

	return d.deregister(c)
}

// Register sets up all subscriptions with MQTT broker for the device
func (d *Device) Register(c MQTT.Client) error {

	d.lock.Lock()
	defer d.lock.Unlock()

	return d.register(c)
}
