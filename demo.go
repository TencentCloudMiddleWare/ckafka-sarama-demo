package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"github.com/Shopify/sarama"
)

func main() {
	server := []string{"yourckafkavip"}
	groupID := "yourgroupid"
	topic := []string{"yourtopicname"}
	config := sarama.NewConfig()
	//指定kafka版本，选择和购买的ckafka相对应的版本，如果不指定，sarama会使用最低支持的版本
	config.Version = sarama.V1_1_1_0
	config.Net.SASL.Enable = true
	config.Net.SASL.User = "yourinstance#yourusername"
	config.Net.SASL.Password = "yourpassword"

	//producer
	proClient, err := sarama.NewClient(server, config)
	if err != nil {
		log.Fatalf("unable to create kafka client: %q", err)
	}
	defer proClient.Close()
	producer, err := sarama.NewAsyncProducerFromClient(proClient)
	if err != nil {
		log.Fatalln("failed to start Sarama producer:", err)
	}
	defer producer.Close()

	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case t := <-ticker.C:
				//向一个topic生产消息
				msg := &sarama.ProducerMessage{
					Topic: topic[0],
					Key:   sarama.StringEncoder(t.Second()),
					Value: sarama.StringEncoder("Hello World!"),
				}
				producer.Input() <- msg
			}
		}
	}()
	//consumer group
	consumer := Consumer{
		ready: make(chan bool),
	}
	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(server, groupID, config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			//Consume 需要在一个无限循环中调用，当重平衡发生的时候，需要重新创建consumer session来获得新ConsumeClaim
			if err := client.Consume(ctx, topic, &consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			//如果context设置为取消，则直接退出
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()
	log.Println("Sarama consumer up and running!...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigterm:
		log.Println("terminating: via signal")
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}

//Consumer 消费者结构体
type Consumer struct {
	ready chan bool
}

//Setup 函数会在创建新的consumer session的时候调用，调用时期发生在ConsumeClaim调用前
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

//Cleanup 函数会在所有的ConsumeClaim协程退出后被调用
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim 是实际处理消息的函数
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// 注意:
	// 不要使用协程启动以下代码.
	// ConsumeClaim 会自己拉起协程，具体行为见源码:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
		session.MarkMessage(message, "")
	}

	return nil
}
