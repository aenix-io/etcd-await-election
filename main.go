package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type ConfigError struct {
	missingEnv string
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("config: missing required environment variable: '%s'", e.missingEnv)
}

type AwaitElection struct {
	LockName       string
	LeaderIdentity string
	StatusEndpoint string
	LeaseDuration  time.Duration
	RetryPeriod    time.Duration
	LeaderExec     func(ctx context.Context) error
	EtcdClient     *clientv3.Client
	leaseID        clientv3.LeaseID
	ForceAcquire   bool
	currentLeader  string
}

func NewAwaitElectionConfig(exec func(ctx context.Context) error) (*AwaitElection, error) {
	lockName := os.Getenv("ETCD_AWAIT_ELECTION_LOCK_NAME")
	if lockName == "" {
		return nil, &ConfigError{missingEnv: "ETCD_AWAIT_ELECTION_LOCK_NAME"}
	}

	leaderIdentity := os.Getenv("ETCD_AWAIT_ELECTION_IDENTITY")
	if leaderIdentity == "" {
		return nil, &ConfigError{missingEnv: "ETCD_AWAIT_ELECTION_IDENTITY"}
	}

	statusEndpoint := os.Getenv("ETCD_AWAIT_ELECTION_STATUS_ENDPOINT")

	leaseDuration := 15 * time.Second
	if val, ok := os.LookupEnv("ETCD_AWAIT_ELECTION_LEASE_DURATION"); ok {
		d, err := strconv.Atoi(val)
		if err == nil {
			leaseDuration = time.Duration(d) * time.Second
		}
	}

	retryPeriod := 2 * time.Second
	if val, ok := os.LookupEnv("ETCD_AWAIT_ELECTION_RETRY_PERIOD"); ok {
		d, err := strconv.Atoi(val)
		if err == nil {
			retryPeriod = time.Duration(d) * time.Second
		}
	}

	endpoints := os.Getenv("ETCD_AWAIT_ELECTION_ENDPOINTS")
	if endpoints == "" {
		return nil, &ConfigError{missingEnv: "ETCD_AWAIT_ELECTION_ENDPOINTS"}
	}
	endpointsList := strings.Split(endpoints, ",")

	tlsConfig, err := getTLSConfig()
	if err != nil {
		return nil, err
	}

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   endpointsList,
		TLS:         tlsConfig,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}

	forceAcquire := false
	if val, ok := os.LookupEnv("ETCD_AWAIT_ELECTION_FORCE_ACQUIRE"); ok {
		forceAcquire, _ = strconv.ParseBool(val) // Ignore error, default will be false
	}

	return &AwaitElection{
		LockName:       lockName,
		LeaderIdentity: leaderIdentity,
		StatusEndpoint: statusEndpoint,
		LeaseDuration:  leaseDuration,
		RetryPeriod:    retryPeriod,
		LeaderExec:     exec,
		EtcdClient:     etcdClient,
		ForceAcquire:   forceAcquire,
	}, nil
}

func getTLSConfig() (*tls.Config, error) {
	certFile := os.Getenv("ETCD_AWAIT_ELECTION_CERT")
	keyFile := os.Getenv("ETCD_AWAIT_ELECTION_KEY")
	caFile := os.Getenv("ETCD_AWAIT_ELECTION_CACERT")

	if certFile == "" || keyFile == "" || caFile == "" {
		return nil, nil
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load client certificate: %w", err)
	}

	caCert, err := ioutil.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to add CA certificate to pool")
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}, nil
}

func (el *AwaitElection) createAndSaveNewLease(ctx context.Context) error {
	lease, err := el.EtcdClient.Grant(ctx, int64(el.LeaseDuration.Seconds()))
	if err != nil {
		return fmt.Errorf("failed to create lease: %v", err)
	}
	el.leaseID = lease.ID
	return nil
}

func (el *AwaitElection) keepLeaseAlive(ctx context.Context, cancel context.CancelFunc) {
	ch, kaerr := el.EtcdClient.KeepAlive(ctx, el.leaseID)
	if kaerr != nil {
		log.Printf("Failed to set up lease keep-alive: %v", kaerr)
		cancel()            // Immediately cancel context if setting up keep-alive fails
		el.revokeLease(ctx) // Immediately try to revoke lease if setting up keep-alive fails
		return
	}
	for {
		select {
		case kaResp, ok := <-ch:
			if !ok || kaResp == nil {
				log.Printf("Lease keep-alive channel closed or response is nil, lease might be lost")
				cancel()            // Cancel context if keep-alive fails
				el.revokeLease(ctx) // Revoke lease immediately if keep-alive fails
				return
			}
		case <-time.After(el.RetryPeriod):
			healthCtx, healthCancel := context.WithTimeout(ctx, 2*time.Second)
			defer healthCancel()
			_, err := el.EtcdClient.Get(healthCtx, "health")
			if err != nil {
				log.Printf("Failed to get etcd health: %v", err)
				cancel()
				el.revokeLease(ctx)
				return
			}
		case <-ctx.Done():
			log.Printf("Context canceled, stopping lease keep-alive")
			return
		}
	}
}

func (el *AwaitElection) revokeLease(ctx context.Context) {
	if _, err := el.EtcdClient.Revoke(ctx, el.leaseID); err != nil {
		log.Printf("Failed to revoke lease: %v", err)
	} else {
		log.Printf("Lease revoked successfully")
	}
}

func (el *AwaitElection) watchLeadership(ctx context.Context, cancel context.CancelFunc) {
	watchChan := el.EtcdClient.Watch(ctx, el.LockName)
	for watchResp := range watchChan {
		for _, ev := range watchResp.Events {
			if ev.Type == clientv3.EventTypeDelete {
				log.Printf("Leadership key deleted, cancelling leadership")
				cancel()
				return
			}
		}
	}
}

func (el *AwaitElection) acquireLeadership(ctx context.Context) error {
	for {
		if el.ForceAcquire {
			// Delete the existing lock if force acquire is enabled
			_, err := el.EtcdClient.Delete(ctx, el.LockName)
			if err != nil {
				log.Printf("Failed to delete existing lock: %v", err)
			} else {
				log.Printf("Existing lock deleted under force acquire policy")
			}
		} else {
			resp, err := el.EtcdClient.Get(ctx, el.LockName)
			if err != nil {
				return fmt.Errorf("failed to get current leader: %v", err)
			}

			if len(resp.Kvs) > 0 && string(resp.Kvs[0].Value) == el.LeaderIdentity {
				// Current node is already the leader, delete the existing lock to reacquire it
				_, err := el.EtcdClient.Delete(ctx, el.LockName)
				if err != nil {
					log.Printf("Failed to delete existing lock: %v", err)
				} else {
					log.Printf("Existing lock owned by current node deleted to reacquire it")
				}
			}
		}

		lease, err := el.EtcdClient.Grant(ctx, int64(el.LeaseDuration.Seconds()))
		if err != nil {
			return fmt.Errorf("failed to create lease: %v", err)
		}
		el.leaseID = lease.ID

		txn := el.EtcdClient.Txn(ctx).If(
			clientv3.Compare(clientv3.CreateRevision(el.LockName), "=", 0),
		).Then(
			clientv3.OpPut(el.LockName, el.LeaderIdentity, clientv3.WithLease(lease.ID)),
		).Else(
			clientv3.OpGet(el.LockName),
		)

		txnResp, err := txn.Commit()
		if err != nil {
			return fmt.Errorf("failed to commit transaction: %v", err)
		}

		if txnResp.Succeeded {
			// Leadership acquired
			el.currentLeader = el.LeaderIdentity
			log.Printf("Leadership acquired by %s", el.LeaderIdentity)
			return nil
		}

		// There's a current leader, check who it is
		resp, err := el.EtcdClient.Get(ctx, el.LockName)
		if err != nil {
			return fmt.Errorf("failed to get current leader: %v", err)
		}

		currentLeader := string(resp.Kvs[0].Value)
		if currentLeader != el.currentLeader {
			el.currentLeader = currentLeader
			log.Printf("Current leader is %s, will continue trying", currentLeader)
		}

		// Wait and retry
		time.Sleep(el.RetryPeriod)
	}
}

func (el *AwaitElection) Run() error {
	for {
		mainCtx, cancel := context.WithCancel(context.Background())

		// Start the status endpoint to serve HTTP requests
		el.startStatusEndpoint(mainCtx)

		// Attempt to acquire leadership
		err := el.acquireLeadership(mainCtx)
		if err != nil {
			log.Fatalf("Failed to acquire leadership: %v", err)
			cancel()
			return err
		}

		// Keep lease alive in a separate goroutine
		go el.keepLeaseAlive(mainCtx, cancel)

		// Monitor leadership status in a separate goroutine
		go el.watchLeadership(mainCtx, cancel)

		// Execute the leader function
		err = el.LeaderExec(mainCtx)
		if err != nil {
			log.Printf("Leader execution error: %v", err)
		}

		// Regardless of the command result, prepare to cancel context
		cancel()

		// Use a separate context for lease revocation to ensure it can complete
		revokeCtx, revokeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer revokeCancel()
		el.revokeLease(revokeCtx)

		// Retrieve exit code if the command failed
		exitCode := 0
		if exitErr, ok := err.(*exec.ExitError); ok {
			if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
				exitCode = status.ExitStatus()
			}
		}

		// Log and exit with the command's exit status
		log.Printf("Exiting process with code %d", exitCode)
		syscall.Exit(exitCode)
	}
}

func (el *AwaitElection) startStatusEndpoint(ctx context.Context) {
	if el.StatusEndpoint == "" {
		log.Printf("Status endpoint not configured")
		return
	}

	serveMux := http.NewServeMux()
	serveMux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		resp, err := el.EtcdClient.Get(ctx, el.LockName)
		if err != nil || len(resp.Kvs) == 0 {
			writer.Header().Set("Content-Type", "text/plain")
			writer.WriteHeader(http.StatusOK)
			writer.Write([]byte(fmt.Sprintf("{\"status\": \"ok\", \"phase\": \"awaiting\", \"leader\": \"unknown\"}\n")))
			return
		}

		currentLeader := string(resp.Kvs[0].Value)
		if currentLeader == el.LeaderIdentity {
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusOK)
			writer.Write([]byte(fmt.Sprintf("{\"status\": \"ok\", \"phase\": \"running\", \"leader\": \"%s\"}\n", currentLeader)))
		} else {
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusOK)
			writer.Write([]byte(fmt.Sprintf("{\"status\": \"ok\", \"phase\": \"awaiting\", \"leader\": \"%s\"}\n", currentLeader)))
		}
	})

	statusServer := http.Server{
		Addr:    el.StatusEndpoint,
		Handler: serveMux,
		BaseContext: func(listener net.Listener) context.Context {
			return ctx
		},
	}

	go func() {
		log.Printf("Starting status endpoint on %s", el.StatusEndpoint)
		if err := statusServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Failed to start status server: %v", err)
		}
	}()
}

func Execute(ctx context.Context) error {
	log.Printf("Starting command '%s' with arguments: '%v'", os.Args[1], os.Args[2:])
	cmd := exec.CommandContext(ctx, os.Args[1], os.Args[2:]...)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	return cmd.Run()
}

func main() {
	log.Printf("Running etcd-await-election")

	if len(os.Args) <= 1 {
		log.Fatalf("Need at least one argument to run")
	}

	awaitElectionConfig, err := NewAwaitElectionConfig(Execute)
	if err != nil {
		log.Fatalf("Failed to create runner: %v", err)
	}

	err = awaitElectionConfig.Run()
	if err != nil {
		log.Fatalf("Failed to run: %v", err)
	}
}
