package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/akashrampure/wslib/server"
	"github.com/akashrampure/wslib/utils"
)

func main() {
	logger := log.New(os.Stdout, "[ws-server] ", log.LstdFlags|log.Llongfile)

	topics := []string{"gpsinfo"}

	mqttSvc := NewMqtthelperSvc(log.New(os.Stdout, "[mqtt] ", log.LstdFlags|log.Llongfile), topics)
	mqttSvc.Start()
	defer mqttSvc.Stop()

	config := server.NewWsConfig("localhost:8080", "/ws", []string{"*"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mqttWg sync.WaitGroup
	var sendCtxCancel context.CancelFunc

	var wsServer *server.Server

	callbacks := &server.WsCallback{
		Started: func() {
			fmt.Println("Server started")
		},
		Stopped: func() {
			fmt.Println("Server stopped")
		},
		OnConnect: func() {
			fmt.Println("Client connected")

			if sendCtxCancel != nil {
				sendCtxCancel()
			}

			var sendCtx context.Context
			sendCtx, sendCtxCancel = context.WithCancel(ctx)

			mqttWg.Add(1)
			go func() {
				defer mqttWg.Done()
				for {
					select {
					case <-sendCtx.Done():
						return
					case data, ok := <-mqttSvc.MqttData:
						if !ok {
							return
						}
						if err := wsServer.Send(data); err != nil {
							logger.Printf("Send error: %v", err)
							return
						}
					}
				}
			}()
		},
		OnDisconnect: func(err error) {
			fmt.Println("Client disconnected", err)
			if sendCtxCancel != nil {
				sendCtxCancel()
			}
		},
		OnMessage: func(msg []byte) {
			fmt.Println("Received message", string(msg))
		},
		OnError: func(err error) {
			fmt.Println("Error:", err)
		},
	}

	wsServer = server.NewServer(config, callbacks, logger)

	go func() {
		if err := wsServer.Start(); err != nil {
			logger.Fatalf("Failed to start server: %v", err)
		}
	}()

	utils.CloseSignal()

	logger.Println("Shutting down server...")

	cancel()
	wsServer.Shutdown()
	mqttWg.Wait()

	logger.Println("Server shutdown complete")
}
