package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
	"github.com/evo-cloud/logs/go/logs"
	"github.com/evo-cloud/logs/go/server"
	"github.com/evo-cloud/logs/go/server/hub"
	"github.com/evo-cloud/logs/go/source"
)

var (
	hubServeIngressAddr = ":8000"
	hubServeStreamAddr  = ":8001"
	hubServeListenAddr  = ":8080"
	hubServeReplicate   = false

	hubSendClientName = ""
)

func hubServe(cmd *cobra.Command, args []string) error {
	logsConfig.MustSetupDefaultLogger()

	grpcLn, err := net.Listen("tcp", hubServeIngressAddr)
	if err != nil {
		return fmt.Errorf("listen ingress RPC server %s: %w", hubServeIngressAddr, err)
	}
	tcpLn, err := net.Listen("tcp", hubServeStreamAddr)
	if err != nil {
		return fmt.Errorf("listen ingress stream server %s: %w", hubServeStreamAddr, err)
	}
	ln, err := net.Listen("tcp", hubServeListenAddr)
	if err != nil {
		return fmt.Errorf("listen egress server %s: %w", hubServeListenAddr, err)
	}
	defer ln.Close()
	defer grpcLn.Close()

	logs.Infof("Ingress RPC server on %s", grpcLn.Addr())
	logs.Infof("Ingress stream server on %s", tcpLn.Addr())
	logs.Infof("Egress server on %s", ln.Addr())

	dispatcher := &hub.Dispatcher{}
	if hubServeReplicate {
		dispatcher.Emitter = logs.Default()
	}
	ingress := &server.IngressServer{Store: dispatcher}
	srv := grpc.NewServer()
	logspb.RegisterIngressServiceServer(srv, ingress)
	errCh := make(chan error, 3)
	go func() { errCh <- dispatcher.Serve(ln) }()
	go func() { errCh <- srv.Serve(grpcLn) }()
	go func() { errCh <- server.StreamLogsServe(tcpLn, dispatcher) }()
	return <-errCh
}

func hubSend(cmd *cobra.Command, args []string) error {
	addr := "localhost:8001"
	if len(args) > 0 {
		addr = args[0]
	}
	emitter, err := logs.NewStreamBatchEmitter(hubSendClientName, "", addr)
	if err != nil {
		return err
	}
	reader := &source.StreamReader{In: os.Stdin, SkipErrors: true}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	for {
		entry, err := reader.Read(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := emitter.EmitLogEntries(ctx, []*logspb.LogEntry{entry}); err != nil {
			logs.Warningf("Emit error: %w\n", err)
		}
	}
}

func hubConnect(cmd *cobra.Command, args []string) error {
	emitter, err := logsConfig.Emitter()
	if err != nil {
		return fmt.Errorf("setup logger: %w", err)
	}
	addr := "localhost:8080"
	if len(args) > 0 {
		addr = args[0]
	}
	connector := &hub.Connector{Emitter: emitter}
	if err := connector.DialAndStream("tcp", addr); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func cmdHub() *cobra.Command {
	hubServeCmd := &cobra.Command{
		Use:     "serve",
		Aliases: []string{"s"},
		Short:   "Run a hub server",
		RunE:    hubServe,
	}
	hubServeCmd.Flags().StringVarP(&hubServeIngressAddr, "ingress-addr", "i", hubServeIngressAddr, "Logs ingress service (gRPC) address")
	hubServeCmd.Flags().StringVarP(&hubServeStreamAddr, "ingress-stream-addr", "s", hubServeStreamAddr, "Logs ingress stream (TCP) address")
	hubServeCmd.Flags().StringVarP(&hubServeListenAddr, "egress-addr", "e", hubServeListenAddr, "Logs egress (TCP) listening address")
	hubServeCmd.Flags().BoolVar(&hubServeReplicate, "replicate", hubServeReplicate, "Replicate ingress logs to the current logger")

	hubSendCmd := &cobra.Command{
		Use:   "send ADDR",
		Short: "Stream logs to hub",
		RunE:  hubSend,
	}
	hubSendCmd.Flags().StringVarP(&hubSendClientName, "client-name", "c", hubSendClientName, "The client name")

	hubConnectCmd := &cobra.Command{
		Use:     "connect ADDR",
		Aliases: []string{"c"},
		Short:   "Connect to hub and stream logs",
		RunE:    hubConnect,
	}

	cmd := &cobra.Command{
		Use:   "hub",
		Short: "Log hub related functions",
	}

	cmd.AddCommand(hubServeCmd, hubSendCmd, hubConnectCmd)
	return cmd
}
