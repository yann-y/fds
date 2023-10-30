package ipfs

import (
	"context"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/go-blockservice"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/kubo/client/rpc"
)

var log = logging.Logger("ipfs-client")

type PoolClient struct {
	api       *rpc.HttpApi
	addr      string
	enablePin bool
}

func NewBlockService(blkstore blockstore.Blockstore) blockservice.BlockService {
	return blockservice.NewWriteThrough(blkstore, offline.Exchange(blkstore))
}

// NewPoolClient new a dagPoolClient
func NewPoolClient(api *rpc.HttpApi, enablePin bool) (*PoolClient, error) {
	pool := &PoolClient{
		api:       api,
		addr:      "",
		enablePin: enablePin,
	}
	_, err := pool.api.Swarm().ListenAddrs(context.Background())
	return pool, err
}
func (i *PoolClient) Close() {}
func (i *PoolClient) Block() *BlockAPI {
	return (*BlockAPI)(i)
}
func (i *PoolClient) Store() *Store {
	return (*Store)(i)
}
func (i *PoolClient) Health(ctx context.Context) bool {
	_, err := i.api.Swarm().ListenAddrs(ctx)
	if err != nil {
		log.Error("IPFS is not healthy %v", err)
	}
	return err == nil
}
