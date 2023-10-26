package main

import (
	"context"
	"errors"
	"fmt"
	dagpoolcli "github.com/filedag-project/filedag-storage/dag/pool/client"
	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/kubo/client/rpc"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
	dagpool "github.com/yann-y/fds/dag/pool/ipfs"
	"github.com/yann-y/fds/internal/iam"
	"github.com/yann-y/fds/internal/iam/auth"
	"github.com/yann-y/fds/internal/iamapi"
	"github.com/yann-y/fds/internal/s3api"
	"github.com/yann-y/fds/internal/store"
	"github.com/yann-y/fds/internal/uleveldb"
	"github.com/yann-y/fds/internal/utils"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

const (
	EnvRootUser     = "FILEDAG_ROOT_USER"
	EnvRootPassword = "FILEDAG_ROOT_PASSWORD"
)

var log = logging.Logger("sever")

func missingCredentialError(user, pwd string) error {
	return errors.New(fmt.Sprintf("Missing credential environment variable, user is \"%s\" and password is\"%s\"."+
		" Root user and password are expected to be specified via environment variables "+
		"FILEDAG_ROOT_USER and FILEDAG_ROOT_PASSWORD respectively", user, pwd))
}

// startServer Start a IamServer
func startServer(cctx *cli.Context) {
	listen := cctx.String("listen")
	datadir := cctx.String("data-dir")
	poolAddr := cctx.String("pool-addr")

	user := cctx.String("root-user")
	password := cctx.String("root-password")
	if user == "" || password == "" {
		log.Fatal(missingCredentialError(user, password))
	}
	cred, err := auth.CreateCredentials(user, password)
	if err != nil {
		log.Fatal("Invalid credentials. Please provide correct credentials. " +
			"Root user length should be at least 3, and password length at least 8 characters")
	}

	db, err := uleveldb.OpenDb(datadir)
	if err != nil {
		return
	}
	defer db.Close()
	router := mux.NewRouter()
	kuboApi, err := rpc.NewApi(ma.StringCast(poolAddr))
	if err != nil {
		log.Fatal(err)
	}
	poolClient, err := dagpool.NewPoolClient(kuboApi, true)
	if err != nil {
		log.Fatalf("connect dagpool server err: %v", err)
	}
	defer poolClient.Close(context.TODO())
	dagServ := merkledag.NewDAGService(dagpoolcli.NewBlockService(poolClient))
	storageSys := store.NewStorageSys(cctx.Context, dagServ, kuboApi, db)
	authSys := iam.NewAuthSys(db, cred)
	bmSys := store.NewBucketMetadataSys(db)
	storageSys.SetNewBucketNSLock(bmSys.NewNSLock)
	storageSys.SetHasBucket(bmSys.HasBucket)
	bmSys.SetEmptyBucket(storageSys.EmptyBucket)

	cleanData := func(accessKey string) {
		ctx := context.Background()
		bkts, err := bmSys.GetAllBucketsOfUser(ctx, accessKey)
		if err != nil {
			log.Errorf("GetAllBucketsOfUser error: %v", err)
		}
		for _, bkt := range bkts {
			if err = storageSys.CleanObjectsInBucket(ctx, bkt.Name); err != nil {
				log.Errorf("CleanObjectsInBucket error: %v", err)
				continue
			}
			if err = bmSys.DeleteBucket(ctx, bkt.Name); err != nil {
				log.Errorf("DeleteBucket error: %v", err)
			}
		}
	}
	handler := s3api.CorsHandler(router)
	s3api.NewS3Server(router, authSys, bmSys, storageSys)
	iamapi.NewIamApiServer(router, authSys, cleanData)

	if strings.HasPrefix(listen, ":") {
		for _, ip := range utils.MustGetLocalIP4().ToSlice() {
			log.Infof("start sever at http://%v%v", ip, listen)
		}
	} else {
		log.Infof("start sever at http://%v", listen)
	}
	go func() {
		if err = http.ListenAndServe(listen, handler); err != nil {
			log.Errorf("Listen And Serve err%v", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server.
	quit := make(chan os.Signal, 1)
	// kill (no param) default send syscall.SIGTERM
	// kill -2 is syscall.SIGINT
	// kill -9 is syscall.SIGKILL but can't be catch, so don't need add it
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info("Shutdown Server ...")
	log.Info("Server exit")
}
