package main

import (
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fabrikiot/wsmqttrt/wsmqttrtpuller"
)

const (
	WSPullerHost = "dmt1.intellicar.in"
	WSPullerPort = 11884
)

type wsmsgMsg struct {
	topic   []byte
	payload []byte
}

func (o *MqtthelperSvc) run() {

	msgch := make(chan string, 1024)
	filename := os.Args[1]

	o.read_file(filename)

	activeThreads := &sync.WaitGroup{}

	wsmsgch := make(chan *wsmsgMsg)
	wsthreadstopflag := &atomic.Bool{}

	activeThreads.Add(1)
	go func() {
		o.pass_to_channel(msgch)
		activeThreads.Done()
	}()

	wsPullerOpts := wsmqttrtpuller.NewWsMqttRtPullerOpts(WSPullerHost, WSPullerPort)
	wsPullerOpts.ConnectTimeout = int64(30000)
	wsPullerOpts.ConnectRetryTimeout = int64(30000)
	wsPullerOpts.KeepAliveTimeout = int64(30000)

	wsStateCallback := &wsmqttrtpuller.WsMqttRtPullerStateCallback{
		Started: func() {
			o.logger.Printf("WS Puller started")
		},
		Stopped: func() {
			o.logger.Printf("WS Puller stopped")
			activeThreads.Done()
			o.isStopReq.Store(true)
			wsthreadstopflag.Store(true)
		},
	}

	wsMsgCallback := func(topic []byte, payload []byte) {
		wsmsgch <- &wsmsgMsg{
			topic:   topic,
			payload: payload,
		}
	}

	wsPuller := wsmqttrtpuller.NewWsMqttRtPuller(wsPullerOpts, wsStateCallback, wsMsgCallback)
	activeThreads.Add(1)
	wsPuller.Start()
	o.wsPuller = wsPuller

	ticker := time.NewTicker(time.Millisecond * 1000)
	for !o.isStopReq.Load() {
		select {
		case nextQMsg := <-msgch:
			err := o.subscribe_to_topics(nextQMsg)
			if err != nil {
				o.logger.Print("the error occured while subscribing to topics :", err)
			}
		case nextWSMsg := <-wsmsgch:
			o.wsmsg_HandleNextMsg(nextWSMsg)
		case <-ticker.C:
		}
	}
	wsPuller.Stop()

	for !wsthreadstopflag.Load() {
		for len(wsmsgch) > 0 {
			<-wsmsgch
		}
		time.Sleep(time.Millisecond * 100)
	}

	activeThreads.Wait()
	o.logger.Printf("fotasvc run thread exiting")
}

func (o *MqtthelperSvc) Start() {
	go o.run()
}

func (o *MqtthelperSvc) Stop() {
	o.isStopReq.Store(true)
}
