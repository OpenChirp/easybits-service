package main

import (
	"encoding/base64"
	"fmt"
	"log"
	"strconv"
	"strings"

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

var typeName2ProtoType = map[string]descriptor.FieldDescriptorProto_Type{
	"int32":  descriptor.FieldDescriptorProto_TYPE_INT32,
	"uint32": descriptor.FieldDescriptorProto_TYPE_UINT32,
	"sint32": descriptor.FieldDescriptorProto_TYPE_SINT32,
	"int64":  descriptor.FieldDescriptorProto_TYPE_INT64,
	"uint64": descriptor.FieldDescriptorProto_TYPE_UINT64,
	"sint64": descriptor.FieldDescriptorProto_TYPE_SINT64,
	"bool":   descriptor.FieldDescriptorProto_TYPE_BOOL,
}

type Device struct {
	isregistered bool
	node         framework.NodeDescriptor
	mapping      []string // saved to compare for changes later
	num2name     map[uint32]string
	name2num     map[string]uint32
	fieldMap     *dproto.ProtoFieldMap
}

func NewDevice(node framework.NodeDescriptor) *Device {
	return &Device{node: node}
}

func (d *Device) IsMappingEqual(mapping []string) bool {
	if d.mapping == nil && mapping == nil {
		return true
	}
	if d.mapping == nil || mapping == nil {
		return false
	}
	//ASSERT: both must not be nil

	if len(d.mapping) != len(mapping) {
		return false
	}

	for i, v := range d.mapping {
		if v != mapping[i] {
			return false
		}
	}
	return true
}

func (d *Device) SetMapping(mapping []string) error {

	if d.isregistered {
		return ErrDeviceRegistered
	}

	// copy for later comparison
	d.mapping = make([]string, len(mapping))
	copy(d.mapping, mapping)

	d.num2name = make(map[uint32]string, len(mapping))
	d.name2num = make(map[string]uint32, len(mapping))
	d.fieldMap = dproto.NewProtoFieldMap()

	// Add all associations
	for _, m := range mapping {
		parts := strings.Split(m, mappingSeparator)
		if len(parts) != 3 {
			return ErrParseMapping
		}
		fname := parts[0]
		ftype, ok := typeName2ProtoType[parts[1]]
		if !ok {
			return ErrParseMapping
		}
		fnum, err := strconv.ParseUint(parts[2], 10, 32)
		if err != nil {
			return ErrParseMapping
		}
		d.num2name[uint32(fnum)] = fname
		d.name2num[fname] = uint32(fnum)
		d.fieldMap.Add(dproto.FieldNum(fnum), ftype)
	}
	return nil
}

func (d *Device) GetFieldName(num uint32) (string, bool) {
	name, ok := d.num2name[num]
	return name, ok
}

func (d *Device) GetFieldNum(name string) (uint32, bool) {
	num, ok := d.name2num[name]
	return num, ok
}

func (d *Device) Deregister(c MQTT.Client) error {
	if d.isregistered {
		return ErrDeviceNotRegistered
	}

	/* Unsubscribe from Device's rawrx Data Stream */
	token := c.Unsubscribe(d.node.MQTTRoot + "/" + deviceRxData)
	if token.Wait(); token.Error() != nil {
		return ErrRegistrationFailed
	}
	log.Println("Unsubscribed from", d.node.MQTTRoot+"/"+deviceRxData)

	d.isregistered = false

	return nil
}

func (d *Device) Register(c MQTT.Client) error {
	if d.isregistered {
		return ErrDeviceRegistered
	}

	/* Subscribe to Device's rawrx Data Stream */
	token := c.Subscribe(d.node.MQTTRoot+"/"+deviceRxData, byte(mqttQos), func(c MQTT.Client, m MQTT.Message) {

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
			topic := d.node.MQTTRoot + "/" + fieldname
			message := fmt.Sprint(field.Value)
			c.Publish(topic, byte(mqttQos), false, message)
			log.Println("Published", string(message), "to", topic)
		}
	})
	if token.Wait(); token.Error() != nil {
		return ErrRegistrationFailed
	}
	log.Println("Subscribed to", d.node.MQTTRoot+"/"+deviceRxData)

	d.isregistered = true

	return nil
}
