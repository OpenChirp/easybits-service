package main

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/linux4life798/dproto"
	"github.com/openchirp/framework/pubsub"
	"github.com/openchirp/framework/rest"
	log "github.com/sirupsen/logrus"
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
	rest.NodeDescriptor
	// node     framework.NodeDescriptor
	mapping    ServiceConfig // saved to compare for changes later
	rxNum2Name map[uint32]string
	txName2Num map[string]uint32
	rxFieldMap *dproto.ProtoFieldMap
	txFieldMap *dproto.ProtoFieldMap
}

// NewDevice creates a new initialized Device
func NewDevice(node rest.NodeDescriptor) *Device {
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

	d.rxNum2Name = make(map[uint32]string, len(mapping.RxData))
	d.txName2Num = make(map[string]uint32, len(mapping.TxData))
	d.rxFieldMap = dproto.NewProtoFieldMap()
	d.txFieldMap = dproto.NewProtoFieldMap()

	// Add all associations
	for _, m := range d.mapping.RxData {
		pm, err := parseMapping(m)
		if err != nil {
			return err
		}

		/* Add the association */
		d.rxNum2Name[pm.fnum] = pm.fname
		d.rxFieldMap.Add(dproto.FieldNum(pm.fnum), pm.ftype)
	}
	for _, m := range d.mapping.TxData {
		pm, err := parseMapping(m)
		if err != nil {
			return err
		}

		/* Add the association */
		d.txName2Num[pm.fname] = pm.fnum
		d.txFieldMap.Add(dproto.FieldNum(pm.fnum), pm.ftype)
	}
	return nil
}

// deregister unsubscribes all device topics with the MQTT broker
func (d *Device) deregister(c pubsub.PubSub) error {
	logitem := log.WithField("deviceid", d.ID)

	if !d.isregistered {
		return ErrDeviceNotRegistered
	}

	/* Unsubscribe from Device's rawrx Data Stream */
	err := c.Unsubscribe(d.Pubsub.Topic + "/" + deviceRxData)
	if err != nil {
		return ErrRegistrationFailed
	}
	logitem.Debug("Unsubscribed from ", d.Pubsub.Topic+"/"+deviceRxData)

	/* Unsubscribe from all device's TX topics */
	for _, m := range d.mapping.TxData {
		pm, err := parseMapping(m) // last reference to m should be here
		if err != nil {
			return err
		}

		topic := d.Pubsub.Topic + "/" + pm.fname
		err = c.Unsubscribe(topic)
		if err != nil {
			return ErrRegistrationFailed
		}
		logitem.Debug("Unsubscribed from ", topic)
	}

	d.isregistered = false

	return nil
}

// register sets up all subscriptions with MQTT broker for the device
func (d *Device) register(c pubsub.PubSub) error {
	logitem := log.WithField("deviceid", d.ID)

	if d.isregistered {
		return ErrDeviceRegistered
	}

	/* Subscribe to Device's rawrx Data Stream */
	err := c.Subscribe(d.Pubsub.Topic+"/"+deviceRxData, func(topic string, payload []byte) {
		d.lock.RLock()
		defer d.lock.RUnlock()

		logi := log.WithField("deviceid", d.ID)

		/* Decode base64 */
		data, err := base64.StdEncoding.DecodeString(string(payload))
		if err != nil {
			// log error and proceed to next packet
			logi.Warn("Error - Decoding base64:", err)
			c.Publish(d.Pubsub.Topic+"/easybits", "Failed to decode rawrx base64")
			return
		}

		/* Decode Protobuf */
		fields, err := d.rxFieldMap.DecodeBuffer(data)
		if err != nil {
			logi.Warn("Error while decoding rx buffer")
			c.Publish(d.Pubsub.Topic+"/easybits", "Error while decoding rawrx protobuf data")
		}

		for _, field := range fields {
			/* Resolve Field Mapping */
			fieldname, ok := d.GetRXFieldName(uint32(field.Field))
			if !ok {
				// if no name specified, just ignore it
				continue
			}
			/* Publish Data Named Field */
			topic := d.Pubsub.Topic + "/" + fieldname
			message := ""
			if bytes, ok := field.Value.([]byte); ok {
				// Convert to base64 for publishing
				message = base64.StdEncoding.EncodeToString(bytes)
			} else {
				message = fmt.Sprint(field.Value)
			}

			c.Publish(topic, message)
			logi.Debug("Published ", string(message), " to ", topic)
		}

	})
	if err != nil {
		return ErrRegistrationFailed
	}
	logitem.Info("Subscribed to ", d.Pubsub.Topic+"/"+deviceRxData)

	/* Subscribe to all device's TX topics */
	for _, m := range d.mapping.TxData {
		pm, err := parseMapping(m) // last reference to m should be here
		if err != nil {
			return err
		}

		topic := d.Pubsub.Topic + "/" + pm.fname

		/* Subscribe to Device's txdata streams */
		err = c.Subscribe(topic, func(topic string, payload []byte) {

			d.lock.RLock()
			defer d.lock.RUnlock()

			logi := log.WithField("deviceid", d.ID)

			fnum, ok := d.GetTXFieldNum(pm.fname)
			if !ok {
				// log error and ignore publication
				logi.Warnf("Error - Looking up field number for " + pm.fname + " for device " + d.ID)
				return
			}

			typ, ok := d.txFieldMap.Get(dproto.FieldNum(fnum))
			if !ok {
				// log error and ignore publication
				logi.Warnf("Error - Looking up field type for " + pm.fname + " for device " + d.ID)
				return
			}
			value, err := dproto.ParseAs(string(payload), typ, 0)
			if err != nil {
				// log error and ignore publication
				logi.Warnf("Error - Parsing published value \""+string(payload)+"\" for "+pm.fname+" for device "+d.ID+" as a "+typ.String()+":", err)
				return
			}
			values := []dproto.FieldValue{dproto.FieldValue{Field: dproto.FieldNum(fnum), Value: value}}
			buf, err := d.txFieldMap.EncodeBuffer(values)
			if err != nil {
				// log error and ignore publication
				logi.Warnf("Error - Encoding field", pm.fname, "with", string(payload), "for device", d.ID)
				return
			}

			// convert to base64 for rawtx
			data := base64.StdEncoding.EncodeToString(buf)
			c.Publish(d.Pubsub.Topic+"/"+deviceTxData, data)
			logitem.Debug("Published ", data, " to ", d.Pubsub.Topic+"/"+deviceTxData, " as a result of ", topic)

		})
		if err != nil {
			return ErrRegistrationFailed
		}
		logitem.Debug("Subscribed to ", topic)
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
	logitem := log.WithField("deviceid", d.ID)
	logitem.Debugf("SetMapping: %v", mapping)

	d.lock.Lock()
	defer d.lock.Unlock()

	if d.isregistered {
		return ErrDeviceRegistered
	}
	return d.setMapping(mapping)
}

func (d *Device) UpdateMapping(c pubsub.PubSub, mapping ServiceConfig) error {
	logitem := log.WithField("deviceid", d.ID)

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
		logitem.Debug("Update needed")
	} else {
		logitem.Debug("Update not needed")
	}
	return nil
}

// Note: Not thread safe - call inside safe region
func (d *Device) GetRXFieldName(num uint32) (string, bool) {
	name, ok := d.rxNum2Name[num]
	return name, ok
}

// Note: Not thread safe - call inside safe region
func (d *Device) GetTXFieldNum(name string) (uint32, bool) {
	num, ok := d.txName2Num[name]
	return num, ok
}

// Deregister unsubscribes all device topics with the MQTT broker
func (d *Device) Deregister(c pubsub.PubSub) error {

	d.lock.Lock()
	defer d.lock.Unlock()

	return d.deregister(c)
}

// Register sets up all subscriptions with MQTT broker for the device
func (d *Device) Register(c pubsub.PubSub) error {

	d.lock.Lock()
	defer d.lock.Unlock()

	return d.register(c)
}
