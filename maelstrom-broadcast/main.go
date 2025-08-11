package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	messageMap := map[float64]any{}
	topology := map[string][]string{}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		if num, ok := body["message"].(float64); ok {
			if _, ok := messageMap[num]; !ok {
				messageMap[num] = body["msg_id"].(float64)
				targets := topology[msg.Dest]
				for _, target := range targets {
					n.Send(target, body)
				}
			}
		}

		return n.Reply(msg, map[string]string{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		body := map[string]any{}
		body["type"] = "read_ok"
		messages := make([]float64, len(messageMap))
		i := 0
		for k := range messageMap {
			messages[i] = k
			i++
		}
		body["messages"] = messages

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		if received, ok := body["topology"].(map[string]any); ok {
			for k, v := range received {
				interfaceSlice := v.([]interface{})
				stringSlice := make([]string, len(interfaceSlice))
				for i, item := range interfaceSlice {
					stringSlice[i] = item.(string)
				}
				topology[k] = stringSlice
			}
		}

		return n.Reply(msg, map[string]string{
			"type": "topology_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
