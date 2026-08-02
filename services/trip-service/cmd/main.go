package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"ride-sharing/services/trip-service/internal/infrastructure/events"
	"ride-sharing/services/trip-service/internal/infrastructure/grpc"
	"ride-sharing/services/trip-service/internal/infrastructure/repository"
	"ride-sharing/services/trip-service/internal/service"
	"ride-sharing/shared/env"
	"ride-sharing/shared/messaging"
	"syscall"

	grpcserver "google.golang.org/grpc"
)

var GrpcAddr = ":9093"

func main() {

	rabbitMqURI := env.GetString("RABBITMQ_URI", "amqp://guest:guest@rabbitmq:5672/")

	inmemRepo := repository.NewInmemRepository()
	svc := service.NewService(inmemRepo)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down trip service...")
		cancel()
	}()

	// RMQ connection
	rabbitmq, err := messaging.NewRabbitMQ(rabbitMqURI)
	if err != nil {
		log.Fatalf("failed to connect to RabbitMQ: %v", err)
	}
	defer rabbitmq.Close()

	log.Println("Connected to RabbitMQ")

	lis, err := net.Listen("tcp", GrpcAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	publisher := events.NewTripEventPublisher(rabbitmq)

	grpcServer := grpcserver.NewServer()
	grpc.NewGRPCHandler(grpcServer, svc, publisher)

	log.Printf("Starting gRPC server trip Service on port %s", lis.Addr().String())

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("failed to serve: %v", err)
			cancel()
		}
	}()

	<-ctx.Done()
	log.Println("Stopping gRPC server...")
	grpcServer.GracefulStop()
	log.Println("Trip service stopped.")
}
