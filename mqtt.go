package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"

	"github.com/fabrikiot/wsmqttrt/wsmqttrtpuller"
)

type MqtthelperSvc struct {
	logger *log.Logger

	isStopReq *atomic.Bool
	wsPuller  *wsmqttrtpuller.WsMqttRtPuller

	deviceidtasks map[string]string

	MqttData chan interface{}
}

func NewMqtthelperSvc(logger *log.Logger) *MqtthelperSvc {
	return &MqtthelperSvc{
		logger: logger,

		isStopReq: &atomic.Bool{},
		wsPuller:  nil,

		deviceidtasks: make(map[string]string),

		MqttData: make(chan interface{}),
	}
}

func (o *MqtthelperSvc) read_file(filename string) {
	finp, err := os.Open(filename)
	if err != nil {
		o.logger.Print("the err is :", err)
	}
	defer finp.Close()

	fileScanner := bufio.NewScanner(finp)
	fileScanner.Split(bufio.ScanLines)

	for fileScanner.Scan() {
		nextline := fileScanner.Text()
		nextline = strings.TrimSpace(nextline)
		nextline = strings.ToUpper(nextline)
		if len(nextline) != 16 {
			fmt.Println("Deviceid looks wrong:", nextline)
			continue
		}

		o.deviceidtasks[nextline] = "1"

	}
}

func (o *MqtthelperSvc) subscribe_to_topics(deviceid string) error {

	topicsToSubscribe := []string{"/intellicar/layer5/deviceinfo/", "/intellicar/layer5/gpsinfo/", "/intellicar/layer5/lafcanwithtime/", "/intellicar/layer5/fotaresponse/", "/intellicar/layer5/coprocstatus/"}
	for _, eachTopic := range topicsToSubscribe {
		o.wsPuller.Subscribe([]byte(eachTopic+deviceid), func(topic []byte, issubscribe, isok bool) {
			// o.logger.Printf("Topic subscribe callback, topic:%v, issub:%v,isok:%v", string(topic), issubscribe, isok)
		})
	}

	return nil

}

func (o *MqtthelperSvc) pass_to_channel(channel chan string) {

	for eachDevId := range o.deviceidtasks {
		channel <- eachDevId
	}
}

func (o *MqtthelperSvc) wsmsg_HandleNextMsg(nextmsg *wsmsgMsg) {
	topicSplit := strings.Split(string(nextmsg.topic), "/")

	deviceid := topicSplit[len(topicSplit)-1]
	topic := topicSplit[3]
	payload := make(map[string]interface{}, 0)

	jserr := json.Unmarshal(nextmsg.payload, &payload)
	if jserr != nil {
		return
	}

	response := map[string]interface{}{
		"deviceid": deviceid,
		"topic":    topic,
		"payload":  payload,
	}

	o.MqttData <- response

}
