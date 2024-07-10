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
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
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
	RenewDeadline  time.Duration
	RetryPeriod    time.Duration
	LeaderExec     func(ctx context.Context) error
	EtcdClient     *clientv3.Client
	Election       *concurrency.Election
	Session        *concurrency.Session
	leaseID        clientv3.LeaseID
	ForceAcquire   bool
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

	renewDeadline := 10 * time.Second
	if val, ok := os.LookupEnv("ETCD_AWAIT_ELECTION_RENEW_DEADLINE"); ok {
		d, err := strconv.Atoi(val)
		if err == nil {
			renewDeadline = time.Duration(d) * time.Second
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
		RenewDeadline:  renewDeadline,
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

func (el *AwaitElection) writeLeaseID(ctx context.Context, leaseID clientv3.LeaseID) error {
	_, err := el.EtcdClient.Put(ctx, el.LockName+"/leaseID", fmt.Sprintf("%d", leaseID))
	return err
}

func (el *AwaitElection) readLeaseID(ctx context.Context) (clientv3.LeaseID, error) {
	resp, err := el.EtcdClient.Get(ctx, el.LockName+"/leaseID")
	if err != nil {
		return 0, err
	}
	if len(resp.Kvs) == 0 {
		return 0, fmt.Errorf("no leaseID found")
	}
	leaseID, err := strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
	if err != nil {
		return 0, err
	}
	return clientv3.LeaseID(leaseID), nil
}

func (el *AwaitElection) createOrRestoreSession(ctx context.Context) error {
	storedLeaseID, err := el.readLeaseID(ctx)
	if err == nil && storedLeaseID != 0 {
		// Attempt to verify existing lease
		_, err := el.EtcdClient.TimeToLive(ctx, storedLeaseID)
		if err == nil {
			el.leaseID = storedLeaseID
			session, err := concurrency.NewSession(el.EtcdClient, concurrency.WithLease(storedLeaseID))
			if err == nil {
				el.Session = session
				return nil
			}
		}
	}
	// Recreate new lease on any errors
	return el.createAndSaveNewLease(ctx)
}

func (el *AwaitElection) createAndSaveNewLease(ctx context.Context) error {
	lease, err := el.EtcdClient.Grant(ctx, int64(el.LeaseDuration.Seconds()))
	if err != nil {
		return fmt.Errorf("failed to create lease: %v", err)
	}
	el.leaseID = lease.ID
	session, err := concurrency.NewSession(el.EtcdClient, concurrency.WithLease(lease.ID))
	if err != nil {
		return fmt.Errorf("failed to create session: %v", err)
	}
	el.Session = session
	err = el.writeLeaseID(ctx, lease.ID)
	if err != nil {
		return fmt.Errorf("failed to write leaseID: %v", err)
	}
	return nil
}

func (el *AwaitElection) keepLeaseAlive(ctx context.Context) {
	ch, kaerr := el.EtcdClient.KeepAlive(ctx, el.leaseID)
	if kaerr != nil {
		log.Printf("Failed to set up lease keep-alive: %v", kaerr)
		el.revokeLease(ctx) // Immediately try to revoke lease if setting up keep-alive fails
		return
	}
	for kaResp := range ch {
		if kaResp == nil {
			log.Printf("Lease keep-alive response is nil, lease might be lost")
			el.revokeLease(ctx) // Revoke lease immediately if keep-alive fails
			return
		}
		log.Printf("Lease keep-alive received: ID=%v, TTL=%v", kaResp.ID, kaResp.TTL)
	}
}

func (el *AwaitElection) revokeLease(ctx context.Context) {
	if _, err := el.EtcdClient.Revoke(ctx, el.leaseID); err != nil {
		log.Printf("Failed to revoke lease: %v", err)
	} else {
		log.Printf("Lease revoked successfully")
	}
}

func (el *AwaitElection) createSessionAndElection(ctx context.Context) error {
	if el.ForceAcquire {
		_, err := el.EtcdClient.Delete(ctx, el.LockName, clientv3.WithPrefix())
		if err != nil {
			log.Printf("Failed to delete existing locks with prefix %s: %v", el.LockName, err)
			return err
		}
		log.Printf("Existing locks with prefix %s deleted successfully under force acquire policy", el.LockName)
	}

	session, err := concurrency.NewSession(el.EtcdClient)
	if err != nil {
		return fmt.Errorf("failed to create session: %v", err)
	}
	el.Session = session
	el.Election = concurrency.NewElection(session, el.LockName)
	return nil
}

func (el *AwaitElection) Run() error {
	for {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Initialize session and election
		err := el.createSessionAndElection(ctx)
		if err != nil {
			log.Fatalf("Failed to initialize session or election: %v", err)
			return err
		}

		go el.keepLeaseAlive(ctx)

		// Check current leader immediately
		resp, err := el.Election.Leader(ctx)
		if err == nil {
			currentLeader := string(resp.Kvs[0].Value)
			log.Printf("Current leader is %s", currentLeader)

			if currentLeader == el.LeaderIdentity {
				log.Printf("Leader identity matches, continuing leadership.")
				log.Printf("Long live our new leader: %s", el.LeaderIdentity)
				go el.monitorLeadership(ctx, cancel)
				err = el.LeaderExec(ctx)
				if err != nil {
					log.Printf("Leader execution error: %v", err)
				}
				return err
			} else {
				log.Printf("Current leader is %s, not %s, will continue trying.", currentLeader, el.LeaderIdentity)
			}
		} else {
			log.Printf("Failed to get current leader: %v, will campaign.", err)
			// Continue with campaigning
		}

		err = el.Election.Campaign(ctx, el.LeaderIdentity)
		if err != nil {
			log.Printf("Failed to campaign for leadership: %v, retrying...", err)
			time.Sleep(el.RetryPeriod)
			continue
		}

		log.Printf("Long live our new leader: %s", el.LeaderIdentity)
		go el.monitorLeadership(ctx, cancel)

		err = el.LeaderExec(ctx)
		if err != nil {
			log.Printf("Leader execution error: %v", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-el.Session.Done():
			log.Printf("Session expired or lost, losing leadership")
			cancel()
			time.Sleep(el.RetryPeriod)
			// Starting new election cycle
		}
	}
}

func (el *AwaitElection) monitorLeaseKeepAlive(ctx context.Context, ch <-chan *clientv3.LeaseKeepAliveResponse) {
	for {
		select {
		case <-ctx.Done():
			return
		case kaResp, ok := <-ch:
			if !ok {
				log.Printf("Lease keep-alive channel closed, losing leadership")
				return
			}
			if kaResp == nil {
				log.Printf("Lease keep-alive response is nil, losing leadership")
				return
			}
		}
	}
}

func (el *AwaitElection) monitorLeadership(ctx context.Context, cancel context.CancelFunc) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(el.RetryPeriod):
			resp, err := el.Election.Leader(ctx)
			if err != nil {
				log.Printf("Lost leadership or error checking leader: %v", err)
				cancel()
				return
			}
			if string(resp.Kvs[0].Value) != el.LeaderIdentity {
				log.Printf("Lost leadership to %s, exiting", string(resp.Kvs[0].Value))
				cancel()
				return
			}
		}
	}
}

func (el *AwaitElection) startStatusEndpoint(ctx context.Context) {
	if el.StatusEndpoint == "" {
		return
	}

	serveMux := http.NewServeMux()
	serveMux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		leaderResp, err := el.Election.Leader(ctx)
		if err != nil || string(leaderResp.Kvs[0].Value) != el.LeaderIdentity {
			http.Error(writer, "Not leader", http.StatusServiceUnavailable)
			return
		}
		writer.Write([]byte("{\"status\": \"ok\"}"))
	})

	statusServer := http.Server{
		Addr:    el.StatusEndpoint,
		Handler: serveMux,
		BaseContext: func(listener net.Listener) context.Context {
			return ctx
		},
	}
	go func() {
		_ = statusServer.ListenAndServe()
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
