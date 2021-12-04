package main

import (
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
	"github.com/evo-cloud/logs/go/logs"
	"github.com/evo-cloud/logs/go/server"
	"github.com/evo-cloud/logs/go/server/hub"
)

var (
	hubServeIngressAddr = ":8000"
	hubServeListenAddr  = ":8080"
	hubServeReplicate   = false
)

func hubServe(cmd *cobra.Command, args []string) error {
	logsConfig.MustSetupDefaultLogger()

	grpcLn, err := net.Listen("tcp", hubServeIngressAddr)
	if err != nil {
		return fmt.Errorf("listen ingress server %s: %w", hubServeIngressAddr, err)
	}
	ln, err := net.Listen("tcp", hubServeListenAddr)
	if err != nil {
		fmt.Errorf("listen egress server %s: %w", hubServeListenAddr, err)
	}
	defer ln.Close()
	defer grpcLn.Close()

	logs.Infof("Ingress server on %s", grpcLn.Addr())
	logs.Infof("Egress server on %s", ln.Addr())

	dispatcher := &hub.Dispatcher{}
	if hubServeReplicate {
		dispatcher.Emitter = logs.Default()
	}
	ingress := &server.IngressServer{Store: dispatcher}
	srv := grpc.NewServer()
	logspb.RegisterIngressServiceServer(srv, ingress)
	errCh := make(chan error, 2)
	go func() { errCh <- dispatcher.Serve(ln) }()
	go func() { errCh <- srv.Serve(grpcLn) }()
	return <-errCh
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
	hubServeCmd.Flags().StringVarP(&hubServeListenAddr, "egress-addr", "e", hubServeListenAddr, "Logs egress (TCP) listening address")
	hubServeCmd.Flags().BoolVar(&hubServeReplicate, "replicate", hubServeReplicate, "Replicate ingress logs to the current logger")

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

	cmd.AddCommand(hubServeCmd, hubConnectCmd)
	return cmd
}
