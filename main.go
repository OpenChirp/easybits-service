// Craig Hesling
// October 17, 2017
//
// This is an example OpenChirp service. It sets up arguments and the main
// runtime event loop to process new device service links
package main

import (
	"encoding/json"
	"os"
	"os/signal"
	"syscall"

	"github.com/openchirp/framework"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

const (
	version string = "1.2"
)

const (
	// Set this value to true to have the service publish a service status of
	// "Running" each time it receives a device update event
	//
	// This could be used as a service alive pulse if enabled
	// Otherwise, the service status will indicate "Started" at the time the
	// service "Started" the client
	runningStatus = true
)

/*
"serviceconfig": {
	"rxconfig": ["temp,sint32,1", "humidity,uint32,2", "light,uint32,3", "pir,uint32,4", "mic,uint32,5", "accX,uint32,6", "accY,uint32,7", "accZ,uint32,8"],
	"txconfig": ["duty,uint32,9"]
}
*/

type ServiceConfig struct {
	RxData []string `json:"rxmap"`
	TxData []string `json:"txmap"`
	// should add a TXBuffering time
}

const (
	deviceRxData  = "rawrx"
	deviceTxData  = "rawtx"
	rxConfigLabel = "rxconfig"
	txConfigLabel = "txconfig"
)

const (
	defaultFrameworkServer = "http://localhost"
	defaultServiceID       = ""
	defaultRefreshTime     = 10 // seconds
)

func run(ctx *cli.Context) error {
	/* Set logging level */
	log.SetLevel(log.Level(uint32(ctx.Int("log-level"))))

	log.Info("Starting Example Service ")

	/* Start framework service client */
	c, err := framework.StartServiceClientStatus(
		ctx.String("framework-server"),
		ctx.String("mqtt-server"),
		ctx.String("service-id"),
		ctx.String("service-token"),
		"Unexpected disconnect!")
	if err != nil {
		log.Error("Failed to StartServiceClient: ", err)
		return cli.NewExitError(nil, 1)
	}
	defer c.StopClient()
	log.Info("Started service")

	/* Post service status indicating I am starting */
	err = c.SetStatus("Starting")
	if err != nil {
		log.Error("Failed to publish service status: ", err)
		return cli.NewExitError(nil, 1)
	}
	log.Info("Published Service Status")

	/* Setup master table of registered devices */
	var devices map[string]*Device = make(map[string]*Device)

	/* Start service main device updates stream */
	log.Info("Starting Device Updates Stream")
	updates, err := c.StartDeviceUpdatesSimple()
	if err != nil {
		log.Error("Failed to start device updates stream: ", err)
		return cli.NewExitError(nil, 1)
	}
	defer c.StopDeviceUpdates()

	/* Setup signal channel */
	log.Info("Processing device updates")
	signals := make(chan os.Signal)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	/* Post service status indicating I started */
	err = c.SetStatus("Started")
	if err != nil {
		log.Error("Failed to publish service status: ", err)
		return cli.NewExitError(nil, 1)
	}
	log.Info("Published Service Status")

	for {
		select {
		case update := <-updates:
			/* If runningStatus is set, post a service status as an alive msg */
			if runningStatus {
				err = c.SetStatus("Running")
				if err != nil {
					log.Error("Failed to publish service status: ", err)
					return cli.NewExitError(nil, 1)
				}
				log.Info("Published Service Status")
			}

			logitem := log.WithFields(
				log.Fields{"type": update.Type, "deviceid": update.Id},
			)

			switch update.Type {
			case framework.DeviceUpdateTypeRem:
				logitem.Info("Removing device with id ", update.Id, " and config ", update.Config)
				d, ok := devices[update.Id]
				if !ok {
					logitem.Errorf("Asked to remove device %s that was not registered", update.Id)
					continue
				}
				err = d.Deregister(c)
				if err != nil {
					logitem.Errorf("Failed to deregister Device %s: %v", d.ID, err)
				}
				delete(devices, d.ID)
			case framework.DeviceUpdateTypeUpd:
				logitem.Info("Removing device for update with id", update.Id, " and config ", update.Config)
				d, ok := devices[update.Id]
				if !ok {
					logitem.Errorf("Asked to remove device %s that was not registered", update.Id)
				} else {
					err = d.Deregister(c)
					if err != nil {
						logitem.Errorf("Failed to deregister Device %s: %v", d.ID, err)
					}
					delete(devices, d.ID)
				}
				fallthrough
			case framework.DeviceUpdateTypeAdd:
				logitem.Info("Adding device")

				// var dev rest.ServiceDeviceListItem
				// dev.Id = event.Id
				// dev.Config = event.Config

				var config ServiceConfig

				/* Extract rx and tx configs */
				rx := update.Config[rxConfigLabel]
				if rx == "" {
					logitem.Warnf("Device %s did not specify an %s", update.Id, rxConfigLabel)
					rx = "[]"
				}
				tx := update.Config[txConfigLabel]
				if tx == "" {
					logitem.Warnf("Device %s did not specify an %s", update.Id, txConfigLabel)
					tx = "[]"
				}

				/* Parse rx and tx configs as JSON string arrays */
				err := json.Unmarshal([]byte(rx), &config.RxData)
				if err != nil {
					logitem.Warnf("Error parsing %s for device with ID %s", rxConfigLabel, update.Id)
					c.SetDeviceStatus(update.Id, "Failed to parse "+rxConfigLabel+" as JSON string array")
					continue // ignore device
				}
				err = json.Unmarshal([]byte(tx), &config.TxData)
				if err != nil {
					logitem.Warnf("Error parsing %s for device with ID %s", txConfigLabel, update.Id)
					c.SetDeviceStatus(update.Id, "Failed to parse "+txConfigLabel+" as JSON string array")
					continue // ignore device
				}

				/* Lookup full device info */
				fulldev, err := c.FetchDeviceInfo(update.Id)
				if err != nil {
					logitem.Warnf("Error fetching device info for device with ID %s", update.Id)
					continue // ignore device
				}

				/* Build Two-Way FieldID-Name Map */
				d := NewDevice(fulldev.NodeDescriptor)
				err = d.SetMapping(config)
				if err != nil {
					logitem.Warnf("Error setting map for Device %s: %v", update.Id, err)
					c.SetDeviceStatus(update.Id, "Failed to set mapping: "+err.Error())
					continue
				}

				logitem.Info("Registering device ", update.Id)
				err = d.Register(c)
				if err != nil {
					logitem.Warnf("Error registering Device %s: %v", update.Id, err)
					c.SetDeviceStatus(update.Id, "Failed to set register: "+err.Error())
					continue
				}

				devices[fulldev.NodeDescriptor.ID] = d
				c.SetDeviceStatus(update.Id, "Success")
			}
		case sig := <-signals:
			log.WithField("signal", sig).Info("Received signal")
			goto cleanup
		}
	}

cleanup:

	log.Warning("Shutting down")
	err = c.SetStatus("Shutting down")
	if err != nil {
		log.Error("Failed to publish service status: ", err)
	}
	log.Info("Published service status")

	return nil
}

func main() {
	app := cli.NewApp()
	app.Name = "example-service"
	app.Usage = ""
	app.Copyright = "See https://github.com/openchirp/example-service for copyright information"
	app.Version = version
	app.Action = run
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "framework-server",
			Usage:  "OpenChirp framework server's URI",
			Value:  "http://localhost:7000",
			EnvVar: "FRAMEWORK_SERVER",
		},
		cli.StringFlag{
			Name:   "mqtt-server",
			Usage:  "MQTT server's URI (e.g. scheme://host:port where scheme is tcp or tls)",
			Value:  "tls://localhost:1883",
			EnvVar: "MQTT_SERVER",
		},
		cli.StringFlag{
			Name:   "service-id",
			Usage:  "OpenChirp service id",
			EnvVar: "SERVICE_ID",
		},
		cli.StringFlag{
			Name:   "service-token",
			Usage:  "OpenChirp service token",
			EnvVar: "SERVICE_TOKEN",
		},
		cli.IntFlag{
			Name:   "log-level",
			Value:  4,
			Usage:  "debug=5, info=4, warning=3, error=2, fatal=1, panic=0",
			EnvVar: "LOG_LEVEL",
		},
	}
	app.Run(os.Args)
}
